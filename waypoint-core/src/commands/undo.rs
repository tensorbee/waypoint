//! Undo applied migrations by executing U{version}__*.sql files,
//! or auto-generated reversal SQL stored in the history table.

use std::collections::HashMap;

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
use crate::migration::{scan_migrations, MigrationVersion, ResolvedMigration};
use crate::placeholder::{build_placeholders, replace_placeholders};

/// How many / which versions to undo.
#[derive(Debug, Clone)]
pub enum UndoTarget {
    /// Undo the single most recently applied migration.
    Last,
    /// Undo all migrations above this version (the target version itself stays applied).
    Version(MigrationVersion),
    /// Undo the last N applied migrations in reverse order.
    Count(usize),
}

/// Report returned after an undo operation.
#[derive(Debug, Serialize)]
pub struct UndoReport {
    /// Number of migrations that were undone.
    pub migrations_undone: usize,
    /// Total execution time of all undo operations in milliseconds.
    pub total_time_ms: i32,
    /// Per-migration details for each undone migration.
    pub details: Vec<UndoDetail>,
}

/// Details of a single undone migration.
#[derive(Debug, Serialize)]
pub struct UndoDetail {
    /// Version string of the migration that was undone.
    pub version: String,
    /// Human-readable description from the undo migration filename.
    pub description: String,
    /// Filename of the undo migration script that was executed.
    pub script: String,
    /// Execution time of the undo operation in milliseconds.
    pub execution_time_ms: i32,
    /// Whether the undo used auto-generated reversal SQL.
    pub auto_reversal: bool,
}

/// Execute undo SQL within an atomic transaction (BEGIN/execute/history-insert/COMMIT).
///
/// On SQL execution failure, the transaction is rolled back and a best-effort
/// failure record is inserted into the history table. Returns the execution
/// time in milliseconds on success.
#[cfg(feature = "postgres")]
#[allow(clippy::too_many_arguments)]
async fn execute_undo_sql(
    client: &Client,
    schema: &str,
    table: &str,
    version: &str,
    description: &str,
    script: &str,
    checksum: Option<i32>,
    installed_by: &str,
    sql: &str,
) -> Result<i32> {
    let start = std::time::Instant::now();
    client.batch_execute("BEGIN").await?;

    match client.batch_execute(sql).await {
        Ok(()) => {
            let exec_time = start.elapsed().as_millis() as i32;
            match history::insert_applied_migration(
                client,
                schema,
                table,
                Some(version),
                description,
                "UNDO_SQL",
                script,
                checksum,
                installed_by,
                exec_time,
                true,
            )
            .await
            {
                Ok(()) => {
                    client.batch_execute("COMMIT").await?;
                    Ok(exec_time)
                }
                Err(e) => {
                    if let Err(rb) = client.batch_execute("ROLLBACK").await {
                        log::error!("Failed to rollback undo transaction: {}", rb);
                    }
                    Err(e)
                }
            }
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                log::error!("Failed to rollback undo transaction: {}", rollback_err);
            }

            // Record failure — best-effort outside the rolled-back transaction
            if let Err(record_err) = history::insert_applied_migration(
                client,
                schema,
                table,
                Some(version),
                description,
                "UNDO_SQL",
                script,
                checksum,
                installed_by,
                0,
                false,
            )
            .await
            {
                log::warn!(
                    "Failed to record undo failure; script={}, error={}",
                    script,
                    record_err
                );
            }

            let reason = crate::error::format_db_error(&e);
            Err(WaypointError::UndoFailed {
                script: script.to_string(),
                reason,
            })
        }
    }
}

/// Execute the undo command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    let table = &config.migrations.table;

    // Acquire advisory lock
    db::acquire_advisory_lock(client, table).await?;

    let result = run_undo(client, config, target).await;

    // Always release the advisory lock
    if let Err(e) = db::release_advisory_lock(client, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => {
            log::info!(
                "Undo completed; migrations_undone={}, total_time_ms={}",
                report.migrations_undone,
                report.total_time_ms
            );
        }
        Err(e) => {
            log::error!("Undo failed: {}", e);
        }
    }

    result
}

#[cfg(feature = "postgres")]
async fn run_undo(
    client: &Client,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create history table if not exists
    history::create_history_table(client, schema, table).await?;

    // Scan migration files — build map of undo files by version
    let resolved = scan_migrations(&config.migrations.locations)?;
    let undo_by_version: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_undo())
        .filter_map(|m| m.version().map(|v| (v.raw.clone(), m)))
        .collect();

    // Get applied history and compute effective set
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let effective = history::effective_applied_versions(&applied);

    // Build list of currently-applied versioned migrations, sorted descending by version
    let mut applied_versions: Vec<MigrationVersion> = effective
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .collect();
    applied_versions.sort();
    applied_versions.reverse(); // newest first

    // Determine which versions to undo
    let versions_to_undo: Vec<MigrationVersion> = match target {
        UndoTarget::Last => applied_versions.into_iter().take(1).collect(),
        UndoTarget::Count(n) => applied_versions.into_iter().take(n).collect(),
        UndoTarget::Version(ref target_ver) => applied_versions
            .into_iter()
            .filter(|v| v > target_ver)
            .collect(),
    };

    // Get database user info for placeholders
    let db_user = db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or(&db_user);

    let mut report = UndoReport {
        migrations_undone: 0,
        total_time_ms: 0,
        details: Vec::new(),
    };

    // Execute undo for each version (newest first)
    for version in &versions_to_undo {
        // Try manual U file first, then fall back to auto-generated reversal
        if let Some(undo_migration) = undo_by_version.get(&version.raw) {
            // Manual undo file takes precedence
            log::info!(
                "Undoing migration (manual); migration={}, schema={}",
                undo_migration.script,
                schema
            );

            let placeholders = build_placeholders(
                &config.placeholders,
                schema,
                &db_user,
                &db_name,
                &undo_migration.script,
            );
            let sql = replace_placeholders(&undo_migration.sql, &placeholders)?;

            let exec_time = execute_undo_sql(
                client,
                schema,
                table,
                &version.raw,
                &undo_migration.description,
                &undo_migration.script,
                Some(undo_migration.checksum),
                installed_by,
                &sql,
            )
            .await?;

            report.migrations_undone += 1;
            report.total_time_ms += exec_time;
            report.details.push(UndoDetail {
                version: version.raw.clone(),
                description: undo_migration.description.clone(),
                script: undo_migration.script.clone(),
                execution_time_ms: exec_time,
                auto_reversal: false,
            });
        } else if config.reversals.enabled {
            // Fall back to auto-generated reversal SQL from history table
            match crate::reversal::get_reversal(client, schema, table, &version.raw).await? {
                Some(reversal_sql) => {
                    let script = format!("auto-reversal:V{}", version.raw);
                    log::info!(
                        "Undoing migration (auto-reversal); version={}, schema={}",
                        version.raw,
                        schema
                    );

                    let exec_time = execute_undo_sql(
                        client,
                        schema,
                        table,
                        &version.raw,
                        "Auto-generated reversal",
                        &script,
                        None,
                        installed_by,
                        &reversal_sql,
                    )
                    .await?;

                    report.migrations_undone += 1;
                    report.total_time_ms += exec_time;
                    report.details.push(UndoDetail {
                        version: version.raw.clone(),
                        description: "Auto-generated reversal".to_string(),
                        script,
                        execution_time_ms: exec_time,
                        auto_reversal: true,
                    });
                }
                None => {
                    return Err(WaypointError::UndoMissing {
                        version: version.raw.clone(),
                    });
                }
            }
        } else {
            return Err(WaypointError::UndoMissing {
                version: version.raw.clone(),
            });
        }
    }

    Ok(report)
}

// ── Dialect-aware entry + MySQL path (Phase 1+: manual U-files only) ──────────
//
// MySQL undo deliberately supports manual U{version}__*.sql files only. Auto-
// reversal generation requires schema introspection which is deferred (the
// `reversal::get_reversal` path is PG-specific).

/// Execute the undo command (dialect-aware entry).
pub async fn execute_db(
    client: &DbClient,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => execute(client.as_postgres()?, config, target).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => execute_mysql(client, config, target).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

#[cfg(feature = "mysql")]
async fn execute_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    let table = &config.migrations.table;

    client.acquire_lock(table).await?;

    let result = run_undo_mysql(client, config, target).await;

    if let Err(e) = client.release_lock(table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => {
            log::info!(
                "Undo completed (mysql); migrations_undone={}, total_time_ms={}",
                report.migrations_undone,
                report.total_time_ms
            );
        }
        Err(e) => {
            log::error!("Undo failed (mysql): {}", e);
        }
    }

    result
}

#[cfg(feature = "mysql")]
async fn run_undo_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    target: UndoTarget,
) -> Result<UndoReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let schema = schema.as_str();
    let table = &config.migrations.table;

    history::create_history_table_db(client, schema, table).await?;

    let resolved = scan_migrations(&config.migrations.locations)?;
    let undo_by_version: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_undo())
        .filter_map(|m| m.version().map(|v| (v.raw.clone(), m)))
        .collect();

    let applied = history::get_applied_migrations_db(client, schema, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let mut applied_versions: Vec<MigrationVersion> = effective
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .collect();
    applied_versions.sort();
    applied_versions.reverse();

    let versions_to_undo: Vec<MigrationVersion> = match target {
        UndoTarget::Last => applied_versions.into_iter().take(1).collect(),
        UndoTarget::Count(n) => applied_versions.into_iter().take(n).collect(),
        UndoTarget::Version(ref target_ver) => applied_versions
            .into_iter()
            .filter(|v| v > target_ver)
            .collect(),
    };

    let db_user = client
        .current_user()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let db_name = client
        .current_database()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or(&db_user)
        .to_string();

    let mut report = UndoReport {
        migrations_undone: 0,
        total_time_ms: 0,
        details: Vec::new(),
    };

    for version in &versions_to_undo {
        let undo_migration = match undo_by_version.get(&version.raw) {
            Some(m) => *m,
            None => {
                // Phase 1 MySQL undo does not support auto-reversal — fail fast
                // and ask the user for an explicit U file. When reversal-gen
                // lands on MySQL (Phase 3) this branch becomes the fall-back.
                return Err(WaypointError::UndoMissing {
                    version: version.raw.clone(),
                });
            }
        };

        log::info!(
            "Undoing migration (manual); migration={}, schema={}",
            undo_migration.script,
            schema
        );

        let placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &db_user,
            &db_name,
            &undo_migration.script,
        );
        let sql = replace_placeholders(&undo_migration.sql, &placeholders)?;

        let start = std::time::Instant::now();
        let exec_result = client.execute_raw(&sql).await;
        let exec_time = start.elapsed().as_millis() as i32;

        match exec_result {
            Ok(_) => {
                // Record success
                history::insert_applied_migration_db(
                    client,
                    schema,
                    table,
                    Some(&version.raw),
                    &undo_migration.description,
                    "UNDO_SQL",
                    &undo_migration.script,
                    Some(undo_migration.checksum),
                    &installed_by,
                    exec_time,
                    true,
                )
                .await?;

                report.migrations_undone += 1;
                report.total_time_ms += exec_time;
                report.details.push(UndoDetail {
                    version: version.raw.clone(),
                    description: undo_migration.description.clone(),
                    script: undo_migration.script.clone(),
                    execution_time_ms: exec_time,
                    auto_reversal: false,
                });
            }
            Err(e) => {
                // Best-effort failure record. MySQL DDL auto-commits so the
                // schema may be in a partially-undone state; we report the
                // failure with a clear message and let the operator decide.
                if let Err(record_err) = history::insert_applied_migration_db(
                    client,
                    schema,
                    table,
                    Some(&version.raw),
                    &undo_migration.description,
                    "UNDO_SQL",
                    &undo_migration.script,
                    Some(undo_migration.checksum),
                    &installed_by,
                    exec_time,
                    false,
                )
                .await
                {
                    log::warn!(
                        "Failed to record undo failure; script={}, error={}",
                        undo_migration.script,
                        record_err
                    );
                }
                return Err(WaypointError::UndoFailed {
                    script: undo_migration.script.clone(),
                    reason: e.to_string(),
                });
            }
        }
    }

    Ok(report)
}
