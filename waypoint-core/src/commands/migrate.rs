use std::collections::HashMap;

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::hooks::{self, HookType, ResolvedHook};
use crate::migration::{MigrationVersion, ResolvedMigration, scan_migrations};
use crate::placeholder::{build_placeholders, replace_placeholders};

/// Report returned after a migrate operation.
#[derive(Debug, Serialize)]
pub struct MigrateReport {
    pub migrations_applied: usize,
    pub total_time_ms: i32,
    pub details: Vec<MigrateDetail>,
    pub hooks_executed: usize,
    pub hooks_time_ms: i32,
}

#[derive(Debug, Serialize)]
pub struct MigrateDetail {
    pub version: Option<String>,
    pub description: String,
    pub script: String,
    pub execution_time_ms: i32,
}

/// Execute the migrate command.
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    let table = &config.migrations.table;

    // Acquire advisory lock
    db::acquire_advisory_lock(client, table).await?;

    let result = run_migrate(client, config, target_version).await;

    // Always release the advisory lock
    if let Err(e) = db::release_advisory_lock(client, table).await {
        tracing::warn!(error = %e, "Failed to release advisory lock");
    }

    match &result {
        Ok(report) => {
            tracing::info!(
                migrations_applied = report.migrations_applied,
                total_time_ms = report.total_time_ms,
                hooks_executed = report.hooks_executed,
                "Migrate completed"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "Migrate failed");
        }
    }

    result
}

async fn run_migrate(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    // Create history table if not exists
    history::create_history_table(client, schema, table).await?;

    // Validate on migrate if enabled
    if config.migrations.validate_on_migrate {
        if let Err(e) = super::validate::execute(client, config).await {
            // Only fail on actual validation errors, not if there's nothing to validate
            match &e {
                WaypointError::ValidationFailed(_) => return Err(e),
                _ => {
                    tracing::debug!("Validation skipped: {}", e);
                }
            }
        }
    }

    // Scan migration files
    let resolved = scan_migrations(&config.migrations.locations)?;

    // Scan and load hooks
    let mut all_hooks: Vec<ResolvedHook> = hooks::scan_hooks(&config.migrations.locations)?;
    let config_hooks = hooks::load_config_hooks(&config.hooks)?;
    all_hooks.extend(config_hooks);

    // Get applied migrations
    let applied = history::get_applied_migrations(client, schema, table).await?;

    // Get database user info for placeholders
    let db_user = db::get_current_user(client).await.unwrap_or_else(|_| "unknown".to_string());
    let db_name = db::get_current_database(client).await.unwrap_or_else(|_| "unknown".to_string());
    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or(&db_user);

    // Parse target version if provided
    let target = target_version
        .map(MigrationVersion::parse)
        .transpose()?;

    // Find the baseline version if any
    let baseline_version = applied
        .iter()
        .find(|a| a.migration_type == "BASELINE")
        .and_then(|a| a.version.as_ref())
        .map(|v| MigrationVersion::parse(v))
        .transpose()?;

    // Find highest applied versioned migration (version presence, not type string,
    // for Flyway compatibility)
    let highest_applied = applied
        .iter()
        .filter(|a| a.success && a.version.is_some())
        .filter_map(|a| a.version.as_ref())
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .max();

    // Build set of applied versions and scripts for quick lookup
    let applied_versions: HashMap<String, &crate::history::AppliedMigration> = applied
        .iter()
        .filter(|a| a.success)
        .filter_map(|a| a.version.as_ref().map(|v| (v.clone(), a)))
        .collect();

    let applied_scripts: HashMap<String, &crate::history::AppliedMigration> = applied
        .iter()
        .filter(|a| a.success && a.version.is_none())
        .map(|a| (a.script.clone(), a))
        .collect();

    let mut report = MigrateReport {
        migrations_applied: 0,
        total_time_ms: 0,
        details: Vec::new(),
        hooks_executed: 0,
        hooks_time_ms: 0,
    };

    // ── beforeMigrate hooks ──
    let before_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &db_user,
        &db_name,
        "beforeMigrate",
    );
    let (count, ms) = hooks::run_hooks(client, config, &all_hooks, &HookType::BeforeMigrate, &before_placeholders).await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    // ── Apply versioned migrations ──
    let versioned: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_versioned())
        .collect();

    for migration in &versioned {
        let version = migration.version().unwrap();

        // Skip if already applied successfully
        if applied_versions.contains_key(&version.raw) {
            continue;
        }

        // Skip if below baseline
        if let Some(ref bv) = baseline_version {
            if version <= bv {
                tracing::debug!("Skipping {} (below baseline)", migration.script);
                continue;
            }
        }

        // Check target version
        if let Some(ref tv) = target {
            if version > tv {
                tracing::debug!("Skipping {} (above target {})", migration.script, tv);
                break;
            }
        }

        // Check out-of-order
        if !config.migrations.out_of_order {
            if let Some(ref highest) = highest_applied {
                if version < highest {
                    return Err(WaypointError::OutOfOrder {
                        version: version.raw.clone(),
                        highest: highest.raw.clone(),
                    });
                }
            }
        }

        // beforeEachMigrate hooks
        let each_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &db_user,
            &db_name,
            &migration.script,
        );
        let (count, ms) = hooks::run_hooks(client, config, &all_hooks, &HookType::BeforeEachMigrate, &each_placeholders).await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        // Apply migration
        let exec_time = apply_migration(
            client,
            config,
            migration,
            schema,
            table,
            installed_by,
            &db_user,
            &db_name,
        )
        .await?;

        // afterEachMigrate hooks
        let (count, ms) = hooks::run_hooks(client, config, &all_hooks, &HookType::AfterEachMigrate, &each_placeholders).await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        report.migrations_applied += 1;
        report.total_time_ms += exec_time;
        report.details.push(MigrateDetail {
            version: Some(version.raw.clone()),
            description: migration.description.clone(),
            script: migration.script.clone(),
            execution_time_ms: exec_time,
        });
    }

    // ── Apply repeatable migrations ──
    let repeatables: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| !m.is_versioned())
        .collect();

    for migration in &repeatables {
        // Check if already applied with same checksum
        if let Some(applied_entry) = applied_scripts.get(&migration.script) {
            if applied_entry.checksum == Some(migration.checksum) {
                continue; // Unchanged, skip
            }
            // Checksum differs — re-apply (outdated)
            tracing::info!(migration = %migration.script, "Re-applying changed repeatable migration");
        }

        // beforeEachMigrate hooks
        let each_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &db_user,
            &db_name,
            &migration.script,
        );
        let (count, ms) = hooks::run_hooks(client, config, &all_hooks, &HookType::BeforeEachMigrate, &each_placeholders).await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        let exec_time = apply_migration(
            client,
            config,
            migration,
            schema,
            table,
            installed_by,
            &db_user,
            &db_name,
        )
        .await?;

        // afterEachMigrate hooks
        let (count, ms) = hooks::run_hooks(client, config, &all_hooks, &HookType::AfterEachMigrate, &each_placeholders).await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        report.migrations_applied += 1;
        report.total_time_ms += exec_time;
        report.details.push(MigrateDetail {
            version: None,
            description: migration.description.clone(),
            script: migration.script.clone(),
            execution_time_ms: exec_time,
        });
    }

    // ── afterMigrate hooks ──
    let after_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &db_user,
        &db_name,
        "afterMigrate",
    );
    let (count, ms) = hooks::run_hooks(client, config, &all_hooks, &HookType::AfterMigrate, &after_placeholders).await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    Ok(report)
}

async fn apply_migration(
    client: &Client,
    config: &WaypointConfig,
    migration: &ResolvedMigration,
    schema: &str,
    table: &str,
    installed_by: &str,
    db_user: &str,
    db_name: &str,
) -> Result<i32> {
    tracing::info!(migration = %migration.script, schema = %schema, "Applying migration");

    // Build placeholders
    let placeholders = build_placeholders(
        &config.placeholders,
        schema,
        db_user,
        db_name,
        &migration.script,
    );

    // Replace placeholders in SQL
    let sql = replace_placeholders(&migration.sql, &placeholders)?;

    let version_str = migration.version().map(|v| v.raw.as_str());
    let type_str = migration.migration_type().to_string();

    // Execute in transaction
    match db::execute_in_transaction(client, &sql).await {
        Ok(exec_time) => {
            // Record success (rank is assigned atomically in the INSERT)
            history::insert_applied_migration(
                client,
                schema,
                table,
                version_str,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                exec_time,
                true,
            )
            .await?;

            Ok(exec_time)
        }
        Err(e) => {
            // Record failure — we try to insert the failure record, but don't fail if that also fails
            if let Err(record_err) = history::insert_applied_migration(
                client,
                schema,
                table,
                version_str,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                0,
                false,
            )
            .await
            {
                tracing::warn!(script = %migration.script, error = %record_err, "Failed to record migration failure in history table");
            }

            // Extract detailed error message
            let reason = match &e {
                WaypointError::DatabaseError(db_err) => crate::error::format_db_error(db_err),
                other => other.to_string(),
            };
            tracing::error!(script = %migration.script, reason = %reason, "Migration failed");
            Err(WaypointError::MigrationFailed {
                script: migration.script.clone(),
                reason,
            })
        }
    }
}
