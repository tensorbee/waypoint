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
    /// Non-fatal warnings — most commonly partial-replication failures on
    /// MySQL (e.g. views that reference a database we couldn't recreate in
    /// the simulation environment). Empty on PG today.
    #[serde(default)]
    pub warnings: Vec<String>,
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
        warnings: Vec::new(),
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

    let mut warnings: Vec<String> = Vec::new();

    for table_name in &tables {
        let show_stmt = format!("SHOW CREATE TABLE `{}`.`{}`", source_db, table_name);
        if let Ok(Some((_, create_sql))) = conn.query_first::<(String, String), _>(&show_stmt).await
        {
            // The DDL is "CREATE TABLE `name` (...)"; since USE has set our
            // default database to temp_db it lands there.
            if let Err(e) = conn.query_drop(&create_sql).await {
                warnings.push(format!(
                    "Could not replicate table `{}` into the simulation database: {}. \
                     Migrations that depend on this table may report misleading errors.",
                    table_name, e
                ));
            }
        }
    }

    // Replicate views. SHOW CREATE VIEW returns the DDL with `source_db`.
    // baked into qualified column refs. We rewrite `source_db`. → empty so
    // the view binds to the current default database (temp_db, since we
    // USE'd into it above). This handles the common case where a view
    // references tables in the same database; cross-database views would
    // need a proper SQL rewriter — those will fail to replicate and the
    // dependent migration will surface a clear error.
    let views: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
            (source_db,),
        )
        .await?;
    for view_name in &views {
        let show_stmt = format!("SHOW CREATE VIEW `{}`.`{}`", source_db, view_name);
        if let Ok(Some(row)) = conn.query_first::<mysql_async::Row, _>(&show_stmt).await {
            let mut row = row;
            if let Some(create_sql) = row.take::<String, _>(1) {
                let other_db = first_other_db_qualifier(&create_sql, source_db);
                let rewritten = rewrite_view_db_qualifier(&create_sql, source_db);
                if let Err(e) = conn.query_drop(&rewritten).await {
                    if let Some(other) = other_db {
                        warnings.push(format!(
                            "View `{}` references database `{}` which is not replicated \
                             into the simulation environment; skipped (error: {}). \
                             Migrations that read from this view may surface misleading errors.",
                            view_name, other, e
                        ));
                    } else {
                        warnings.push(format!(
                            "Could not replicate view `{}` into the simulation database: {}.",
                            view_name, e
                        ));
                    }
                }
            }
        }
    }

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
        warnings,
    })
}

/// Rewrite `\`source_db\`.` prefixes in a view DDL so the view binds to the
/// current default database when re-executed.
///
/// MySQL's `SHOW CREATE VIEW` returns column references qualified with the
/// source database. When we replay the DDL into a different database, those
/// qualifiers would still point at the original — so we strip them and let
/// the current `USE` provide the binding. Simple string-replace; for views
/// that legitimately reference *other* databases this won't work and the
/// replay will fail (surfaced as a SimulationReport warning).
#[cfg(feature = "mysql")]
fn rewrite_view_db_qualifier(create_sql: &str, source_db: &str) -> String {
    let qualifier = format!("`{}`.", source_db);
    create_sql.replace(&qualifier, "")
}

/// If a view DDL references a database *other* than `source_db`, return its
/// name. Used to produce a clearer warning when replication into the
/// simulation env fails. Looks for backtick-quoted identifiers that are the
/// *first* segment of a qualified name (preceded by something other than `.`)
/// and followed by a dot — that's the shape MySQL's `SHOW CREATE VIEW` emits
/// for database qualifiers. Identifiers preceded by `.` are table/column
/// names within a qualified reference, not databases.
#[cfg(feature = "mysql")]
fn first_other_db_qualifier(create_sql: &str, source_db: &str) -> Option<String> {
    let bytes = create_sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'`' {
            // Find the matching closing backtick.
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j] != b'`' {
                j += 1;
            }
            if j >= bytes.len() {
                return None;
            }
            // Only treat this as a DB qualifier if (a) the char before the
            // opening backtick is not `.` (otherwise this ident is the table
            // or column part of `db.table.col`), and (b) the char after the
            // closing backtick is `.` (this ident has at least one trailing
            // segment, so it's a leading qualifier).
            let preceded_by_dot = i > 0 && bytes[i - 1] == b'.';
            let followed_by_dot = j + 1 < bytes.len() && bytes[j + 1] == b'.';
            if !preceded_by_dot && followed_by_dot {
                let ident = &create_sql[start..j];
                if ident != source_db && !ident.is_empty() {
                    return Some(ident.to_string());
                }
            }
            i = j + 1;
        } else {
            i += 1;
        }
    }
    None
}

#[cfg(all(test, feature = "mysql"))]
mod tests {
    use super::*;

    #[test]
    fn rewrite_strips_source_db_prefix() {
        let sql = "CREATE VIEW `v` AS SELECT `db1`.`t`.`c` FROM `db1`.`t`";
        let out = rewrite_view_db_qualifier(sql, "db1");
        assert_eq!(out, "CREATE VIEW `v` AS SELECT `t`.`c` FROM `t`");
    }

    #[test]
    fn rewrite_preserves_unrelated_db_prefix() {
        let sql = "CREATE VIEW `v` AS SELECT `other`.`t`.`c` FROM `other`.`t`";
        let out = rewrite_view_db_qualifier(sql, "db1");
        assert_eq!(out, sql);
    }

    #[test]
    fn rewrite_handles_no_qualifier() {
        let sql = "CREATE VIEW `v` AS SELECT 1 AS x";
        let out = rewrite_view_db_qualifier(sql, "db1");
        assert_eq!(out, sql);
    }

    #[test]
    fn first_other_db_detects_cross_db_ref() {
        let sql = "CREATE VIEW `v` AS SELECT `shared`.`t`.`c` FROM `shared`.`t`";
        assert_eq!(
            first_other_db_qualifier(sql, "app"),
            Some("shared".to_string())
        );
    }

    #[test]
    fn first_other_db_ignores_source_db() {
        // The source-db prefix is *not* a cross-database reference. Only an
        // unrelated database name should be flagged.
        let sql = "CREATE VIEW `v` AS SELECT `app`.`t`.`c` FROM `app`.`t`";
        assert_eq!(first_other_db_qualifier(sql, "app"), None);
    }

    #[test]
    fn first_other_db_returns_none_for_no_qualifier() {
        let sql = "CREATE VIEW `v` AS SELECT 1 AS x";
        assert_eq!(first_other_db_qualifier(sql, "app"), None);
    }

    #[test]
    fn first_other_db_reports_first_match() {
        // When multiple foreign DBs are referenced, surface the first one.
        let sql = "CREATE VIEW `v` AS \
                   SELECT `shared`.`t`.`c`, `audit`.`log`.`m` \
                   FROM `shared`.`t` JOIN `audit`.`log`";
        assert_eq!(
            first_other_db_qualifier(sql, "app"),
            Some("shared".to_string())
        );
    }
}
