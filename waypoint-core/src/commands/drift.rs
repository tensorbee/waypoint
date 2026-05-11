//! Detect manual schema changes that bypassed migrations.
//!
//! Creates a temporary schema (PostgreSQL) or database (MySQL), applies all
//! migrations to it, then compares it against the live schema to detect drift.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db;
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::migration::scan_migrations;
use crate::placeholder::build_placeholders;
use crate::schema::{self, SchemaDiff};

/// Type of drift detected.
#[derive(Debug, Clone, Serialize)]
pub enum DriftType {
    /// An object exists in the live database but not in the expected migration state.
    ExtraObject,
    /// An object is expected from migrations but missing from the live database.
    MissingObject,
    /// An object exists in both but its definition has been changed outside migrations.
    ModifiedObject,
}

impl std::fmt::Display for DriftType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriftType::ExtraObject => write!(f, "Extra (not in migrations)"),
            DriftType::MissingObject => write!(f, "Missing (in migrations but not in DB)"),
            DriftType::ModifiedObject => write!(f, "Modified (differs from migrations)"),
        }
    }
}

/// A single drift finding.
#[derive(Debug, Clone, Serialize)]
pub struct DriftEntry {
    /// Category of drift (extra, missing, or modified).
    pub drift_type: DriftType,
    /// Identifier of the affected database object (e.g. "TABLE users").
    pub object: String,
    /// Human-readable description of the drift.
    pub detail: String,
}

/// Drift detection report.
#[derive(Debug, Serialize)]
pub struct DriftReport {
    /// All drift findings detected.
    pub drifts: Vec<DriftEntry>,
    /// Whether any drift was detected.
    pub has_drift: bool,
    /// Name of the schema that was checked for drift.
    pub schema: String,
}

/// Execute the drift command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<DriftReport> {
    let schema_name = &config.migrations.schema;
    let table = &config.migrations.table;

    // Generate a random temp schema name
    let temp_schema = format!(
        "waypoint_drift_check_{}",
        chrono::Utc::now().format("%Y%m%d%H%M%S")
    );

    // Create temp schema
    client
        .batch_execute(&format!("CREATE SCHEMA {}", db::quote_ident(&temp_schema)))
        .await?;

    let result = run_drift_check(client, config, schema_name, table, &temp_schema).await;

    // Always clean up temp schema
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA {} CASCADE",
            db::quote_ident(&temp_schema)
        ))
        .await;

    result
}

#[cfg(feature = "postgres")]
async fn run_drift_check(
    client: &Client,
    config: &WaypointConfig,
    schema_name: &str,
    table: &str,
    temp_schema: &str,
) -> Result<DriftReport> {
    // Create history table in temp schema
    history::create_history_table(client, temp_schema, table).await?;

    // Get applied migrations (successful ones only)
    let applied = history::get_applied_migrations(client, schema_name, table).await?;
    let effective = history::effective_applied_versions(&applied);

    // Scan migration files
    let resolved = scan_migrations(&config.migrations.locations)?;

    // Get DB info for placeholders
    let db_user = db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    // Set search_path to temp schema and apply migrations
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            db::quote_ident(temp_schema)
        ))
        .await?;

    // Apply versioned migrations that were successfully applied
    for migration in resolved.iter().filter(|m| m.is_versioned()) {
        let version = migration.version().unwrap();
        if !effective.contains(&version.raw) {
            continue;
        }

        let placeholders = build_placeholders(
            &config.placeholders,
            temp_schema,
            &db_user,
            &db_name,
            &migration.script,
        );
        let sql = crate::placeholder::replace_placeholders(&migration.sql, &placeholders)?;
        client.batch_execute(&sql).await.map_err(|e| {
            crate::error::WaypointError::MigrationFailed {
                script: migration.script.clone(),
                reason: format!("Drift check: {}", e),
            }
        })?;
    }

    // Reset search_path
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            db::quote_ident(schema_name)
        ))
        .await?;

    // Introspect both schemas
    let live_snapshot = schema::introspect(client, schema_name).await?;
    let expected_snapshot = schema::introspect(client, temp_schema).await?;

    // Diff: expected (from migrations) vs live (actual DB state)
    let diffs = schema::diff(&expected_snapshot, &live_snapshot);

    let mut drifts = Vec::new();
    for d in &diffs {
        let (drift_type, object, detail) = match d {
            SchemaDiff::TableAdded(t) => (
                DriftType::ExtraObject,
                format!("TABLE {}", t.name),
                "Table exists in DB but not in migrations".to_string(),
            ),
            SchemaDiff::TableDropped(n) => (
                DriftType::MissingObject,
                format!("TABLE {}", n),
                "Table exists in migrations but not in DB".to_string(),
            ),
            SchemaDiff::ColumnAdded { table, column } => (
                DriftType::ExtraObject,
                format!("COLUMN {}.{}", table, column.name),
                format!("Column added outside migrations ({})", column.data_type),
            ),
            SchemaDiff::ColumnDropped { table, column } => (
                DriftType::MissingObject,
                format!("COLUMN {}.{}", table, column),
                "Column removed outside migrations".to_string(),
            ),
            SchemaDiff::ColumnAltered { table, column, .. } => (
                DriftType::ModifiedObject,
                format!("COLUMN {}.{}", table, column),
                "Column definition changed outside migrations".to_string(),
            ),
            SchemaDiff::IndexAdded(idx) => (
                DriftType::ExtraObject,
                format!("INDEX {}", idx.name),
                "Index exists in DB but not in migrations".to_string(),
            ),
            SchemaDiff::IndexDropped(n) => (
                DriftType::MissingObject,
                format!("INDEX {}", n),
                "Index missing from DB".to_string(),
            ),
            _ => {
                // Generic handling for other diff types
                let detail = format!("{}", d);
                let drift_type = if detail.starts_with('+') {
                    DriftType::ExtraObject
                } else if detail.starts_with('-') {
                    DriftType::MissingObject
                } else {
                    DriftType::ModifiedObject
                };
                (drift_type, detail.clone(), detail)
            }
        };

        // Filter out the history table itself from drift results
        if object.contains(table) || object.contains("waypoint_drift_check") {
            continue;
        }

        drifts.push(DriftEntry {
            drift_type,
            object,
            detail,
        });
    }

    let has_drift = !drifts.is_empty();

    Ok(DriftReport {
        drifts,
        has_drift,
        schema: schema_name.to_string(),
    })
}

/// Execute the drift command (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<DriftReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => execute(client.as_postgres()?, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => execute_mysql(client, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}

/// MySQL drift detection.
///
/// Creates a throwaway database, replays migrations that have been recorded
/// as applied in history, then diffs the live database against it. The diff
/// surfaces objects present in live but absent in the migration-replay state
/// (extras) and vice versa (missing / modified).
#[cfg(feature = "mysql")]
async fn execute_mysql(client: &DbClient, config: &WaypointConfig) -> Result<DriftReport> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let schema_name = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    let temp_db = format!(
        "waypoint_drift_check_{}",
        chrono::Utc::now().format("%Y%m%d%H%M%S")
    );

    // Create the throwaway database.
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("CREATE DATABASE `{}`", temp_db))
        .await?;

    let result = run_drift_check_mysql(client, config, &schema_name, table, &temp_db).await;

    // Always drop the temp DB.
    if let Err(e) = conn
        .query_drop(format!("DROP DATABASE IF EXISTS `{}`", temp_db))
        .await
    {
        log::warn!(
            "Failed to drop drift-check temp database {}: {}",
            temp_db,
            e
        );
    }

    result
}

#[cfg(feature = "mysql")]
async fn run_drift_check_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    schema_name: &str,
    table: &str,
    temp_db: &str,
) -> Result<DriftReport> {
    let pool = client.as_mysql()?;

    // Create history table inside the temp DB so the replay can write to it.
    // (We use the same `client` — the introspect/insert queries are scoped via
    // the explicit schema argument, so they target temp_db when we pass it.)
    history::create_history_table_db(client, temp_db, table).await?;

    // Get applied migrations from the LIVE database.
    let applied = history::get_applied_migrations_db(client, schema_name, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let resolved = scan_migrations(&config.migrations.locations)?;
    let db_user = client
        .current_user()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let db_name = client
        .current_database()
        .await
        .unwrap_or_else(|_| "unknown".into());

    // Replay applied versioned migrations against the temp DB. We checkout a
    // single conn for the whole replay so a single `USE temp_db` persists.
    use mysql_async::prelude::*;
    let mut replay_conn = pool.get_conn().await?;
    replay_conn.query_drop(format!("USE `{}`", temp_db)).await?;
    for migration in resolved.iter().filter(|m| m.is_versioned()) {
        let version = migration.version().expect("filtered to versioned");
        if !effective.contains(&version.raw) {
            continue;
        }
        let placeholders = build_placeholders(
            &config.placeholders,
            temp_db,
            &db_user,
            &db_name,
            &migration.script,
        );
        let sql = crate::placeholder::replace_placeholders(&migration.sql, &placeholders)?;
        for stmt in crate::sql_parser::split_mysql_statements(&sql) {
            replay_conn
                .query_drop(&stmt)
                .await
                .map_err(|e| WaypointError::MigrationFailed {
                    script: migration.script.clone(),
                    reason: format!("Drift check: {}", e),
                })?;
        }
    }
    drop(replay_conn);

    // Introspect both: live (actual state) vs expected (after replaying migrations).
    let live = schema::introspect_db(client, schema_name).await?;
    let expected = schema::introspect_db(client, temp_db).await?;

    let diffs = schema::diff(&expected, &live);
    let drifts = diffs_to_drift_entries(&diffs, table);

    Ok(DriftReport {
        has_drift: !drifts.is_empty(),
        drifts,
        schema: schema_name.to_string(),
    })
}

/// Convert a list of structural [`SchemaDiff`]s to user-facing [`DriftEntry`]s.
/// Pulled out so the MySQL path can reuse the PG drift-categorization rules.
fn diffs_to_drift_entries(diffs: &[SchemaDiff], history_table: &str) -> Vec<DriftEntry> {
    let mut drifts = Vec::new();
    for d in diffs {
        let (drift_type, object, detail) = match d {
            SchemaDiff::TableAdded(t) => (
                DriftType::ExtraObject,
                format!("TABLE {}", t.name),
                "Table exists in DB but not in migrations".to_string(),
            ),
            SchemaDiff::TableDropped(n) => (
                DriftType::MissingObject,
                format!("TABLE {}", n),
                "Table exists in migrations but not in DB".to_string(),
            ),
            SchemaDiff::ColumnAdded { table, column } => (
                DriftType::ExtraObject,
                format!("COLUMN {}.{}", table, column.name),
                format!("Column added outside migrations ({})", column.data_type),
            ),
            SchemaDiff::ColumnDropped { table, column } => (
                DriftType::MissingObject,
                format!("COLUMN {}.{}", table, column),
                "Column removed outside migrations".to_string(),
            ),
            SchemaDiff::ColumnAltered { table, column, .. } => (
                DriftType::ModifiedObject,
                format!("COLUMN {}.{}", table, column),
                "Column definition changed outside migrations".to_string(),
            ),
            SchemaDiff::IndexAdded(idx) => (
                DriftType::ExtraObject,
                format!("INDEX {}", idx.name),
                "Index exists in DB but not in migrations".to_string(),
            ),
            SchemaDiff::IndexDropped(n) => (
                DriftType::MissingObject,
                format!("INDEX {}", n),
                "Index missing from DB".to_string(),
            ),
            other => {
                let detail = format!("{}", other);
                (DriftType::ModifiedObject, detail.clone(), detail)
            }
        };
        // Filter out the history table itself + the drift-check temp DB.
        if object.contains(history_table) || object.contains("waypoint_drift_check") {
            continue;
        }
        drifts.push(DriftEntry {
            drift_type,
            object,
            detail,
        });
    }
    drifts
}
