//! Migration simulation: run pending migrations in a throwaway schema
//! to prove they will succeed before applying to the real schema.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db::quote_ident;
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::migration::scan_migrations;
use crate::placeholder::{build_placeholders, replace_placeholders};
#[cfg(feature = "postgres")]
use crate::schema;

/// Report from a migration simulation.
#[derive(Debug, Clone, Serialize)]
pub struct SimulationReport {
    /// Whether all pending migrations passed simulation.
    pub passed: bool,
    /// Number of migrations simulated.
    pub migrations_simulated: usize,
    /// Name of the temporary schema used.
    pub temp_schema: String,
    /// Errors encountered during simulation.
    pub errors: Vec<SimulationError>,
}

/// An error encountered during simulation.
#[derive(Debug, Clone, Serialize)]
pub struct SimulationError {
    /// The migration script that failed.
    pub script: String,
    /// Error message.
    pub error: String,
}

/// Execute migration simulation in a throwaway schema (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<SimulationReport> {
    let schema_name = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create history table if needed (for querying applied state)
    history::create_history_table(client, schema_name, table).await?;

    // Generate a unique temp schema name
    let temp_schema = format!(
        "waypoint_sim_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );

    let result = run_simulation(client, config, &temp_schema).await;

    // Always clean up the temp schema (retry once on failure)
    let drop_sql = format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_ident(&temp_schema)
    );
    if let Err(e) = client.batch_execute(&drop_sql).await {
        log::warn!(
            "First attempt to drop simulation schema {} failed, retrying: {}",
            temp_schema,
            e
        );
        if let Err(e2) = client.batch_execute(&drop_sql).await {
            log::error!(
                "Failed to drop simulation schema {} after retry: {}",
                temp_schema,
                e2
            );
        }
    }

    result
}

#[cfg(feature = "postgres")]
async fn run_simulation(
    client: &Client,
    config: &WaypointConfig,
    temp_schema: &str,
) -> Result<SimulationReport> {
    let schema_name = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create the temp schema
    let create_sql = format!("CREATE SCHEMA {}", quote_ident(temp_schema));
    client
        .batch_execute(&create_sql)
        .await
        .map_err(|e| WaypointError::SimulationFailed {
            reason: format!("Failed to create simulation schema: {}", e),
        })?;

    // Replicate current schema structure into temp schema
    let snapshot = schema::introspect(client, schema_name).await?;
    let ddl = schema::to_ddl(&snapshot);

    if !ddl.is_empty() {
        // Set search_path to temp schema for DDL execution
        let set_path = format!("SET search_path TO {}", quote_ident(temp_schema));
        client
            .batch_execute(&set_path)
            .await
            .map_err(|e| WaypointError::SimulationFailed {
                reason: format!("Failed to set search_path: {}", e),
            })?;

        // Execute DDL to replicate structure (ignore errors for complex objects)
        if let Err(e) = client.batch_execute(&ddl).await {
            log::debug!("Partial schema replication in simulation: {}", e);
        }
    }

    // Set search_path to temp schema
    let set_path = format!("SET search_path TO {}", quote_ident(temp_schema));
    client
        .batch_execute(&set_path)
        .await
        .map_err(|e| WaypointError::SimulationFailed {
            reason: format!("Failed to set search_path: {}", e),
        })?;

    // Get pending migrations
    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations(client, schema_name, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let db_user = crate::db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = crate::db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    let mut errors = Vec::new();
    let mut simulated = 0;

    for migration in &resolved {
        if migration.is_undo() {
            continue;
        }
        if let Some(version) = migration.version() {
            if effective.contains(&version.raw) {
                continue; // Already applied
            }
        }

        let placeholders = build_placeholders(
            &config.placeholders,
            temp_schema,
            &db_user,
            &db_name,
            &migration.script,
        );
        let sql = match replace_placeholders(&migration.sql, &placeholders) {
            Ok(s) => s,
            Err(e) => {
                errors.push(SimulationError {
                    script: migration.script.clone(),
                    error: e.to_string(),
                });
                continue;
            }
        };

        match client.batch_execute(&sql).await {
            Ok(_) => {
                simulated += 1;
            }
            Err(e) => {
                errors.push(SimulationError {
                    script: migration.script.clone(),
                    error: crate::error::format_db_error(&e),
                });
            }
        }
    }

    // Restore search_path
    let restore_path = format!("SET search_path TO {}", quote_ident(schema_name));
    if let Err(e) = client.batch_execute(&restore_path).await {
        log::warn!("Failed to restore search_path: {}", e);
    }

    Ok(SimulationReport {
        passed: errors.is_empty(),
        migrations_simulated: simulated,
        temp_schema: temp_schema.to_string(),
        errors,
    })
}

/// Execute migration simulation in a throwaway schema (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<SimulationReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => execute(client.as_postgres()?, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => execute_mysql(client, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

#[cfg(feature = "mysql")]
async fn execute_mysql(client: &DbClient, config: &WaypointConfig) -> Result<SimulationReport> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let source_db = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    history::create_history_table_db(client, &source_db, table).await?;

    let temp_db = format!(
        "waypoint_sim_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );

    let result = run_simulation_mysql(client, config, &source_db, &temp_db).await;

    // Always drop the temp database (retry once on failure).
    let mut conn = pool.get_conn().await?;
    let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", temp_db);
    if let Err(e) = conn.query_drop(&drop_sql).await {
        log::warn!(
            "First attempt to drop simulation database {} failed, retrying: {}",
            temp_db,
            e
        );
        if let Err(e2) = conn.query_drop(&drop_sql).await {
            log::error!(
                "Failed to drop simulation database {} after retry: {}",
                temp_db,
                e2
            );
        }
    }

    result
}

#[cfg(feature = "mysql")]
async fn run_simulation_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    source_db: &str,
    temp_db: &str,
) -> Result<SimulationReport> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;

    // Create the throwaway database.
    let create_sql = format!("CREATE DATABASE `{}`", temp_db);
    conn.query_drop(&create_sql)
        .await
        .map_err(|e| WaypointError::SimulationFailed {
            reason: format!("Failed to create simulation database: {}", e),
        })?;

    // Replicate source structure into the temp DB. We use SHOW CREATE TABLE
    // / SHOW CREATE VIEW (same approach as MySQL snapshot) and rewrite the
    // qualified name to point at the temp DB. Simulation tolerates partial
    // replication — anything we can't replicate just becomes a SQL error when
    // the migration references it.
    let tables: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
            (source_db,),
        )
        .await?;

    conn.query_drop(format!("USE `{}`", temp_db)).await?;

    for table_name in &tables {
        let show_stmt = format!("SHOW CREATE TABLE `{}`.`{}`", source_db, table_name);
        if let Ok(Some((_, create_sql))) = conn.query_first::<(String, String), _>(&show_stmt).await
        {
            // The DDL is "CREATE TABLE `name` (...)"; since USE has set our
            // default database to temp_db it lands there.
            if let Err(e) = conn.query_drop(&create_sql).await {
                log::debug!(
                    "Partial replication for {}: {} (simulation continuing)",
                    table_name,
                    e
                );
            }
        }
    }

    // Views: SHOW CREATE VIEW returns DDL with the schema baked in. Rewriting
    // it reliably is tricky, so we omit views in simulation. Migrations that
    // depend on views via SELECT will hit a clear error.

    // Get pending migrations.
    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied =
        history::get_applied_migrations_db(client, source_db, &config.migrations.table).await?;
    let effective = history::effective_applied_versions(&applied);

    let db_user = client
        .current_user()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let db_name = client
        .current_database()
        .await
        .unwrap_or_else(|_| "unknown".into());

    let mut errors = Vec::new();
    let mut simulated = 0;

    for migration in &resolved {
        if migration.is_undo() {
            continue;
        }
        if let Some(version) = migration.version() {
            if effective.contains(&version.raw) {
                continue;
            }
        }

        let placeholders = build_placeholders(
            &config.placeholders,
            temp_db,
            &db_user,
            &db_name,
            &migration.script,
        );
        let sql = match replace_placeholders(&migration.sql, &placeholders) {
            Ok(s) => s,
            Err(e) => {
                errors.push(SimulationError {
                    script: migration.script.clone(),
                    error: e.to_string(),
                });
                continue;
            }
        };

        // Execute via execute_raw which handles MySQL per-statement protocol.
        // We've USE'd into temp_db so unqualified table refs land there.
        match client.execute_raw(&sql).await {
            Ok(_) => simulated += 1,
            Err(e) => errors.push(SimulationError {
                script: migration.script.clone(),
                error: e.to_string(),
            }),
        }
    }

    Ok(SimulationReport {
        passed: errors.is_empty(),
        migrations_simulated: simulated,
        temp_schema: temp_db.to_string(),
        errors,
    })
}
