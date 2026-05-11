//! Apply pending migrations to the database.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db;
use crate::db::DbClient;
use crate::directive::MigrationDirectives;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::hooks::{self, HookType, ResolvedHook};
use crate::migration::{scan_migrations, MigrationVersion, ResolvedMigration};
use crate::placeholder::{build_placeholders, replace_placeholders};

/// Check if a migration should run in the current environment.
///
/// Returns true if:
/// - The migration has no env directives (runs everywhere)
/// - No environment is configured (runs everything)
/// - The migration's env list includes the current environment
fn should_run_in_environment(directives: &MigrationDirectives, current_env: Option<&str>) -> bool {
    // No env directives = runs everywhere
    if directives.env.is_empty() {
        return true;
    }
    // No environment configured = runs everything
    let env = match current_env {
        Some(e) => e,
        None => return true,
    };
    // Check if current env matches any directive
    directives.env.iter().any(|e| e.eq_ignore_ascii_case(env))
}

/// Report returned after a migrate operation.
#[derive(Debug, Serialize)]
pub struct MigrateReport {
    /// Number of migrations that were applied in this run.
    pub migrations_applied: usize,
    /// Total execution time of all migrations in milliseconds.
    pub total_time_ms: i32,
    /// Per-migration details for each applied migration.
    pub details: Vec<MigrateDetail>,
    /// Number of lifecycle hooks that were executed.
    pub hooks_executed: usize,
    /// Total execution time of all hooks in milliseconds.
    pub hooks_time_ms: i32,
}

/// Details of a single applied migration within a migrate run.
#[derive(Debug, Serialize)]
pub struct MigrateDetail {
    /// Version string, or None for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description from the migration filename.
    pub description: String,
    /// Filename of the migration script.
    pub script: String,
    /// Execution time of this migration in milliseconds.
    pub execution_time_ms: i32,
}

/// Result of evaluating require guard preconditions for a single migration.
enum GuardAction {
    /// All preconditions passed; proceed with the migration.
    Continue,
    /// A precondition failed with on_require_fail=Skip; skip this migration.
    Skip,
    /// A precondition failed fatally; abort with the given error.
    Error(WaypointError),
}

/// Common state prepared by `prepare_migrate()` for both run modes.
struct MigrateSetup<'a> {
    /// All resolved migration files on disk.
    resolved: Vec<ResolvedMigration>,
    /// All hooks (from disk + config).
    all_hooks: Vec<ResolvedHook>,
    /// Current database user.
    db_user: String,
    /// Current database name.
    db_name: String,
    /// Who to record as the installer.
    installed_by: String,
    /// Parsed target version, if specified.
    target: Option<MigrationVersion>,
    /// Baseline version from history, if any.
    baseline_version: Option<MigrationVersion>,
    /// Set of effectively-applied version strings (respects undo).
    effective_versions: HashSet<String>,
    /// Highest effectively-applied version.
    highest_applied: Option<MigrationVersion>,
    /// Map of repeatable script name -> applied checksum (for checksum comparison).
    applied_scripts: HashMap<String, Option<i32>>,
    /// Current environment from config.
    current_env: Option<&'a str>,
}

/// Perform all shared setup: history table creation, validation, preflight,
/// file scanning, hooks loading, version computation.
async fn prepare_migrate<'a>(
    client: &Client,
    config: &'a WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateSetup<'a>> {
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
                    log::debug!("Validation skipped: {}", e);
                }
            }
        }
    }

    // Run preflight checks if enabled
    if config.preflight.enabled {
        let preflight_report = crate::preflight::run_preflight(client, &config.preflight).await?;
        if !preflight_report.passed {
            let failed_checks: Vec<String> = preflight_report
                .checks
                .iter()
                .filter(|c| c.status == crate::preflight::CheckStatus::Fail)
                .map(|c| format!("{}: {}", c.name, c.detail))
                .collect();
            return Err(WaypointError::PreflightFailed {
                checks: failed_checks.join("; "),
            });
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
        .unwrap_or(&db_user)
        .to_string();

    // Parse target version if provided
    let target = target_version.map(MigrationVersion::parse).transpose()?;

    // Find the baseline version if any
    let baseline_version = applied
        .iter()
        .find(|a| a.migration_type == "BASELINE")
        .and_then(|a| a.version.as_ref())
        .map(|v| MigrationVersion::parse(v))
        .transpose()?;

    // Compute effective applied versions (respects undo state)
    let effective_versions = history::effective_applied_versions(&applied);

    // Find highest effectively-applied versioned migration
    let highest_applied = effective_versions
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .max();

    let applied_scripts: HashMap<String, Option<i32>> = applied
        .iter()
        .filter(|a| a.success && a.version.is_none())
        .map(|a| (a.script.clone(), a.checksum))
        .collect();

    let current_env = config.migrations.environment.as_deref();

    Ok(MigrateSetup {
        resolved,
        all_hooks,
        db_user,
        db_name,
        installed_by,
        target,
        baseline_version,
        effective_versions,
        highest_applied,
        applied_scripts,
        current_env,
    })
}

/// Filter resolved migrations down to pending versioned ones, applying
/// baseline/target/out-of-order checks.
fn filter_pending_versioned<'a>(
    versioned: &[&'a ResolvedMigration],
    setup: &MigrateSetup<'_>,
    config: &WaypointConfig,
) -> Result<Vec<&'a ResolvedMigration>> {
    let mut pending = Vec::new();
    for migration in versioned {
        let version = migration.version().unwrap();

        // Skip if already effectively applied (respects undo state)
        if setup.effective_versions.contains(&version.raw) {
            continue;
        }

        // Skip if below baseline
        if let Some(ref bv) = setup.baseline_version {
            if version <= bv {
                log::debug!("Skipping {} (below baseline)", migration.script);
                continue;
            }
        }

        // Check target version
        if let Some(ref tv) = setup.target {
            if version > tv {
                log::debug!("Skipping {} (above target {})", migration.script, tv);
                break;
            }
        }

        // Check out-of-order
        if !config.migrations.out_of_order {
            if let Some(ref highest) = setup.highest_applied {
                if version < highest {
                    return Err(WaypointError::OutOfOrder {
                        version: version.raw.clone(),
                        highest: highest.raw.clone(),
                    });
                }
            }
        }

        pending.push(*migration);
    }
    Ok(pending)
}

/// Filter resolved migrations down to pending repeatable ones (checksum changed or new).
fn filter_pending_repeatables<'a>(
    repeatables: &[&'a ResolvedMigration],
    setup: &MigrateSetup<'_>,
) -> Vec<&'a ResolvedMigration> {
    let mut pending = Vec::new();
    for migration in repeatables {
        if let Some(&applied_checksum) = setup.applied_scripts.get(&migration.script) {
            if applied_checksum == Some(migration.checksum) {
                continue;
            }
        }
        pending.push(*migration);
    }
    pending
}

/// Evaluate all `-- waypoint:require` guard preconditions for a migration.
///
/// Returns `GuardAction::Continue` if all guards pass, `GuardAction::Skip` if
/// the migration should be skipped (when `on_require_fail = Skip`), or
/// `GuardAction::Error` if a fatal guard failure occurs.
async fn evaluate_require_guards(
    client: &Client,
    schema: &str,
    migration: &ResolvedMigration,
    config: &WaypointConfig,
) -> Result<GuardAction> {
    if migration.directives.require.is_empty() {
        return Ok(GuardAction::Continue);
    }

    for expr_str in &migration.directives.require {
        match crate::guard::parse(expr_str) {
            Ok(expr) => {
                match crate::guard::evaluate(client, schema, &expr).await {
                    Ok(true) => {} // Precondition met
                    Ok(false) => {
                        match config.guards.on_require_fail {
                            crate::guard::OnRequireFail::Skip => {
                                log::info!(
                                    "Guard require failed, skipping migration; script={}, expr={}",
                                    migration.script,
                                    expr_str
                                );
                                return Ok(GuardAction::Skip);
                            }
                            crate::guard::OnRequireFail::Warn => {
                                log::warn!(
                                    "Guard require failed (continuing); script={}, expr={}",
                                    migration.script,
                                    expr_str
                                );
                                // Continue with the migration despite guard failure
                            }
                            crate::guard::OnRequireFail::Error => {
                                return Ok(GuardAction::Error(WaypointError::GuardFailed {
                                    kind: "require".to_string(),
                                    script: migration.script.clone(),
                                    expression: expr_str.clone(),
                                }));
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Guard evaluation error; script={}, expr={}, error={}",
                            migration.script,
                            expr_str,
                            e
                        );
                        return Ok(GuardAction::Error(WaypointError::GuardFailed {
                            kind: "require".to_string(),
                            script: migration.script.clone(),
                            expression: format!("{} (evaluation error: {})", expr_str, e),
                        }));
                    }
                }
            }
            Err(e) => {
                return Ok(GuardAction::Error(WaypointError::GuardFailed {
                    kind: "require".to_string(),
                    script: migration.script.clone(),
                    expression: format!("{} (parse error: {})", expr_str, e),
                }));
            }
        }
    }
    Ok(GuardAction::Continue)
}

/// Evaluate all `-- waypoint:ensure` guard postconditions for a migration.
///
/// Returns `Ok(())` if all postconditions pass. Returns an error if any
/// postcondition fails or cannot be evaluated.
async fn evaluate_ensure_guards(
    client: &Client,
    schema: &str,
    migration: &ResolvedMigration,
) -> Result<()> {
    for expr_str in &migration.directives.ensure {
        match crate::guard::parse(expr_str) {
            Ok(expr) => {
                match crate::guard::evaluate(client, schema, &expr).await {
                    Ok(true) => {} // Postcondition met
                    Ok(false) => {
                        return Err(WaypointError::GuardFailed {
                            kind: "ensure".to_string(),
                            script: migration.script.clone(),
                            expression: expr_str.clone(),
                        });
                    }
                    Err(e) => {
                        return Err(WaypointError::GuardFailed {
                            kind: "ensure".to_string(),
                            script: migration.script.clone(),
                            expression: format!("{} (evaluation error: {})", expr_str, e),
                        });
                    }
                }
            }
            Err(e) => {
                return Err(WaypointError::GuardFailed {
                    kind: "ensure".to_string(),
                    script: migration.script.clone(),
                    expression: format!("{} (parse error: {})", expr_str, e),
                });
            }
        }
    }
    Ok(())
}

/// Execute the migrate command (PostgreSQL).
#[cfg(feature = "postgres")]
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    execute_with_options(client, config, target_version, false).await
}

/// Execute the migrate command with additional options (PostgreSQL).
#[cfg(feature = "postgres")]
pub async fn execute_with_options(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force: bool,
) -> Result<MigrateReport> {
    let table = &config.migrations.table;

    // Acquire advisory lock
    db::acquire_advisory_lock(client, table).await?;

    let result = if config.migrations.batch_transaction {
        run_batch_migrate(client, config, target_version, force).await
    } else {
        run_migrate(client, config, target_version, force).await
    };

    // Always release the advisory lock
    if let Err(e) = db::release_advisory_lock(client, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => {
            log::info!(
                "Migrate completed; migrations_applied={}, total_time_ms={}, hooks_executed={}",
                report.migrations_applied,
                report.total_time_ms,
                report.hooks_executed
            );
        }
        Err(e) => {
            log::error!("Migrate failed: {}", e);
        }
    }

    result
}

async fn run_migrate(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force_override: bool,
) -> Result<MigrateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    let setup = prepare_migrate(client, config, target_version).await?;

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
        &setup.db_user,
        &setup.db_name,
        "beforeMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::BeforeMigrate,
        &before_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    // ── Apply versioned migrations ──
    let versioned: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter(|m| should_run_in_environment(&m.directives, setup.current_env))
        .collect();

    let pending_versioned = filter_pending_versioned(&versioned, &setup, config)?;

    for migration in &pending_versioned {
        let version = migration.version().unwrap();

        // beforeEachMigrate hooks
        let each_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &setup.db_user,
            &setup.db_name,
            &migration.script,
        );
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::BeforeEachMigrate,
            &each_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        // ── Safety analysis (before apply) ──
        if config.safety.enabled {
            let safety_report = crate::safety::analyze_migration(
                client,
                schema,
                &migration.sql,
                &migration.script,
                &config.safety,
            )
            .await?;
            if safety_report.overall_verdict == crate::safety::SafetyVerdict::Danger
                && config.safety.block_on_danger
                && !migration.directives.safety_override
                && !force_override
            {
                return Err(WaypointError::MigrationBlocked {
                    script: migration.script.clone(),
                    reason: safety_report.suggestions.join("; "),
                });
            }
        }

        // ── Guard preconditions (require) ──
        match evaluate_require_guards(client, schema, migration, config).await? {
            GuardAction::Continue => {}
            GuardAction::Skip => continue,
            GuardAction::Error(e) => return Err(e),
        }

        // ── Capture before-snapshot for auto-reversal ──
        let before_snapshot = if config.reversals.enabled && migration.is_versioned() {
            Some(crate::reversal::capture_before(client, schema).await?)
        } else {
            None
        };

        // Apply migration (hold transaction open if we need to evaluate ensure guards)
        let has_ensure_guards = !migration.directives.ensure.is_empty();
        let exec_time = apply_migration(
            client,
            config,
            migration,
            schema,
            table,
            &setup.installed_by,
            &setup.db_user,
            &setup.db_name,
            has_ensure_guards,
        )
        .await?;

        // ── Guard postconditions (ensure) — evaluated inside the open transaction ──
        if has_ensure_guards {
            if let Err(guard_err) = evaluate_ensure_guards(client, schema, migration).await {
                if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                    log::error!(
                        "Failed to rollback after ensure guard failure: {}",
                        rollback_err
                    );
                }
                return Err(guard_err);
            }
            // All ensure guards passed — commit the transaction
            client.batch_execute("COMMIT").await?;
        }

        // ── Auto-reversal generation (after successful apply) ──
        if let Some(ref before) = before_snapshot {
            if let Some(ver) = migration.version() {
                match crate::reversal::generate_reversal(
                    client,
                    schema,
                    before,
                    config.reversals.warn_data_loss,
                )
                .await
                {
                    Ok(result) => {
                        if let Some(ref reversal_sql) = result.reversal_sql {
                            if let Err(e) = crate::reversal::store_reversal(
                                client,
                                schema,
                                table,
                                &ver.raw,
                                reversal_sql,
                            )
                            .await
                            {
                                log::warn!(
                                    "Failed to store reversal SQL; version={}, error={}",
                                    ver.raw,
                                    e
                                );
                            }
                        }
                        for warning in &result.warnings {
                            log::warn!("Reversal warning for {}: {}", migration.script, warning);
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to generate reversal; script={}, error={}",
                            migration.script,
                            e
                        );
                    }
                }
            }
        }

        // afterEachMigrate hooks
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::AfterEachMigrate,
            &each_placeholders,
        )
        .await?;
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
    let repeatables: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| !m.is_versioned() && !m.is_undo())
        .filter(|m| should_run_in_environment(&m.directives, setup.current_env))
        .collect();

    for migration in &repeatables {
        // Check if already applied with same checksum
        if let Some(&applied_checksum) = setup.applied_scripts.get(&migration.script) {
            if applied_checksum == Some(migration.checksum) {
                continue; // Unchanged, skip
            }
            // Checksum differs — re-apply (outdated)
            log::info!(
                "Re-applying changed repeatable migration; migration={}",
                migration.script
            );
        }

        // beforeEachMigrate hooks
        let each_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &setup.db_user,
            &setup.db_name,
            &migration.script,
        );
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::BeforeEachMigrate,
            &each_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        let exec_time = apply_migration(
            client,
            config,
            migration,
            schema,
            table,
            &setup.installed_by,
            &setup.db_user,
            &setup.db_name,
            false,
        )
        .await?;

        // afterEachMigrate hooks
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::AfterEachMigrate,
            &each_placeholders,
        )
        .await?;
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
        &setup.db_user,
        &setup.db_name,
        "afterMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::AfterMigrate,
        &after_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    Ok(report)
}

/// Pre-compiled regexes for batch-compatibility checks.
mod batch_regexes {
    use std::sync::LazyLock;
    pub static DROP_INDEX_CONCURRENT: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)DROP\s+INDEX\s+CONCURRENTLY").unwrap());
    pub static CREATE_DATABASE: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bCREATE DATABASE\b").unwrap());
    pub static DROP_DATABASE: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bDROP DATABASE\b").unwrap());
    pub static VACUUM: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bVACUUM\b").unwrap());
    pub static CLUSTER: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bCLUSTER\b").unwrap());
    pub static REINDEX_CONCURRENT: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\bREINDEX\b.*\bCONCURRENTLY\b").unwrap());
}

/// Check that a migration's SQL does not contain statements that cannot run inside a transaction.
///
/// Returns an error if CONCURRENTLY, CREATE DATABASE, DROP DATABASE, VACUUM, CLUSTER,
/// or REINDEX CONCURRENTLY are found. Uses pre-compiled static regexes for efficiency.
fn validate_batch_compatible(script: &str, sql: &str) -> Result<()> {
    let upper = sql.to_uppercase();

    // Check CONCURRENTLY: verify via DDL parser first, then regex for DROP INDEX CONCURRENTLY
    if upper.contains("CONCURRENTLY") {
        let ops = crate::sql_parser::extract_ddl_operations(sql);
        for op in &ops {
            if let crate::sql_parser::DdlOperation::CreateIndex {
                is_concurrent: true,
                ..
            } = op
            {
                return Err(WaypointError::NonTransactionalStatement {
                    script: script.to_string(),
                    statement: op.to_string(),
                });
            }
        }
        if batch_regexes::DROP_INDEX_CONCURRENT.is_match(sql) {
            return Err(WaypointError::NonTransactionalStatement {
                script: script.to_string(),
                statement: "DROP INDEX CONCURRENTLY".to_string(),
            });
        }
    }

    // Check CREATE/DROP DATABASE
    if upper.contains("CREATE DATABASE") && batch_regexes::CREATE_DATABASE.is_match(sql) {
        return Err(WaypointError::NonTransactionalStatement {
            script: script.to_string(),
            statement: "CREATE DATABASE".to_string(),
        });
    }
    if upper.contains("DROP DATABASE") && batch_regexes::DROP_DATABASE.is_match(sql) {
        return Err(WaypointError::NonTransactionalStatement {
            script: script.to_string(),
            statement: "DROP DATABASE".to_string(),
        });
    }

    // Check VACUUM, CLUSTER, REINDEX CONCURRENTLY
    let checks: &[(&regex_lite::Regex, &str, &str)] = &[
        (&batch_regexes::VACUUM, "VACUUM", "VACUUM"),
        (&batch_regexes::CLUSTER, "CLUSTER", "CLUSTER"),
        (
            &batch_regexes::REINDEX_CONCURRENT,
            "REINDEX",
            "REINDEX CONCURRENTLY",
        ),
    ];
    for &(re, fast_check, desc) in checks {
        if upper.contains(fast_check) && re.is_match(sql) {
            return Err(WaypointError::NonTransactionalStatement {
                script: script.to_string(),
                statement: desc.to_string(),
            });
        }
    }

    Ok(())
}

/// Run all pending migrations in a single transaction (all-or-nothing batch mode).
async fn run_batch_migrate(
    client: &Client,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force_override: bool,
) -> Result<MigrateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    let setup = prepare_migrate(client, config, target_version).await?;

    let current_env = setup.current_env;

    // Build the list of pending versioned migrations
    let versioned: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter(|m| should_run_in_environment(&m.directives, current_env))
        .collect();

    let mut pending_versioned = filter_pending_versioned(&versioned, &setup, config)?;

    // Build list of pending repeatable migrations
    let repeatables: Vec<&ResolvedMigration> = setup
        .resolved
        .iter()
        .filter(|m| !m.is_versioned() && !m.is_undo())
        .filter(|m| should_run_in_environment(&m.directives, current_env))
        .collect();
    let pending_repeatables = filter_pending_repeatables(&repeatables, &setup);

    // Pre-validate: check all pending migrations are batch-compatible
    let placeholders_map = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "batch_validate",
    );
    for migration in pending_versioned.iter().chain(pending_repeatables.iter()) {
        let sql = replace_placeholders(&migration.sql, &placeholders_map)?;
        validate_batch_compatible(&migration.script, &sql)?;
    }

    // Safety analysis (before batch transaction)
    if config.safety.enabled {
        for migration in &pending_versioned {
            let safety_report = crate::safety::analyze_migration(
                client,
                schema,
                &migration.sql,
                &migration.script,
                &config.safety,
            )
            .await?;
            if safety_report.overall_verdict == crate::safety::SafetyVerdict::Danger
                && config.safety.block_on_danger
                && !migration.directives.safety_override
                && !force_override
            {
                return Err(WaypointError::MigrationBlocked {
                    script: migration.script.clone(),
                    reason: safety_report.suggestions.join("; "),
                });
            }
        }
    }

    // Guard preconditions (before batch transaction)
    let mut skipped_scripts: HashSet<&str> = HashSet::new();
    for migration in &pending_versioned {
        match evaluate_require_guards(client, schema, migration, config).await? {
            GuardAction::Continue => {}
            GuardAction::Skip => {
                skipped_scripts.insert(&migration.script);
            }
            GuardAction::Error(e) => return Err(e),
        }
    }
    // Remove skipped migrations
    pending_versioned.retain(|m| !skipped_scripts.contains(m.script.as_str()));

    let mut report = MigrateReport {
        migrations_applied: 0,
        total_time_ms: 0,
        details: Vec::new(),
        hooks_executed: 0,
        hooks_time_ms: 0,
    };

    // Run beforeMigrate hooks (outside the batch transaction)
    let before_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "beforeMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::BeforeMigrate,
        &before_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    // Nothing to apply?
    if pending_versioned.is_empty() && pending_repeatables.is_empty() {
        // Run afterMigrate hooks
        let after_placeholders = build_placeholders(
            &config.placeholders,
            schema,
            &setup.db_user,
            &setup.db_name,
            "afterMigrate",
        );
        let (count, ms) = hooks::run_hooks(
            client,
            &setup.all_hooks,
            &HookType::AfterMigrate,
            &after_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;
        return Ok(report);
    }

    // Capture before-snapshot for auto-reversal (before batch transaction)
    let before_snapshot = if config.reversals.enabled {
        match crate::reversal::capture_before(client, schema).await {
            Ok(snap) => Some(snap),
            Err(e) => {
                log::warn!(
                    "Failed to capture before-snapshot for batch reversal: {}",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    // ── BEGIN batch transaction ──
    let batch_start = std::time::Instant::now();
    client.batch_execute("BEGIN").await?;

    let installed_by = &setup.installed_by;
    let batch_result = async {
        // Apply versioned migrations inside the transaction
        for migration in &pending_versioned {
            let version = migration.version().unwrap();
            let each_placeholders = build_placeholders(
                &config.placeholders,
                schema,
                &setup.db_user,
                &setup.db_name,
                &migration.script,
            );

            // beforeEachMigrate hooks (inside transaction)
            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::BeforeEachMigrate,
                &each_placeholders,
            )
            .await?;
            report.hooks_executed += count;
            report.hooks_time_ms += ms;

            let sql = replace_placeholders(&migration.sql, &each_placeholders)?;
            let start = std::time::Instant::now();
            client
                .batch_execute(&sql)
                .await
                .map_err(|e| WaypointError::MigrationFailed {
                    script: migration.script.clone(),
                    reason: crate::error::format_db_error(&e),
                })?;
            let exec_time = start.elapsed().as_millis() as i32;

            // Record history inside the same transaction
            let version_str = Some(version.raw.as_str());
            let type_str = migration.migration_type().to_string();
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

            // afterEachMigrate hooks (inside transaction)
            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::AfterEachMigrate,
                &each_placeholders,
            )
            .await?;
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

        // Apply repeatable migrations inside the transaction
        for migration in &pending_repeatables {
            let each_placeholders = build_placeholders(
                &config.placeholders,
                schema,
                &setup.db_user,
                &setup.db_name,
                &migration.script,
            );

            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::BeforeEachMigrate,
                &each_placeholders,
            )
            .await?;
            report.hooks_executed += count;
            report.hooks_time_ms += ms;

            let sql = replace_placeholders(&migration.sql, &each_placeholders)?;
            let start = std::time::Instant::now();
            client
                .batch_execute(&sql)
                .await
                .map_err(|e| WaypointError::MigrationFailed {
                    script: migration.script.clone(),
                    reason: crate::error::format_db_error(&e),
                })?;
            let exec_time = start.elapsed().as_millis() as i32;

            let type_str = migration.migration_type().to_string();
            history::insert_applied_migration(
                client,
                schema,
                table,
                None,
                &migration.description,
                &type_str,
                &migration.script,
                Some(migration.checksum),
                installed_by,
                exec_time,
                true,
            )
            .await?;

            let (count, ms) = hooks::run_hooks(
                client,
                &setup.all_hooks,
                &HookType::AfterEachMigrate,
                &each_placeholders,
            )
            .await?;
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

        Ok::<(), WaypointError>(())
    }
    .await;

    match batch_result {
        Ok(()) => {
            client.batch_execute("COMMIT").await?;
            report.total_time_ms = batch_start.elapsed().as_millis() as i32;

            // Generate and store reversals for each versioned migration in the batch
            if let Some(ref before) = before_snapshot {
                for migration in &pending_versioned {
                    if let Some(ver) = migration.version() {
                        match crate::reversal::generate_reversal(
                            client,
                            schema,
                            before,
                            config.reversals.warn_data_loss,
                        )
                        .await
                        {
                            Ok(result) => {
                                if let Some(ref reversal_sql) = result.reversal_sql {
                                    if let Err(e) = crate::reversal::store_reversal(
                                        client,
                                        schema,
                                        table,
                                        &ver.raw,
                                        reversal_sql,
                                    )
                                    .await
                                    {
                                        log::warn!(
                                            "Failed to store reversal SQL; version={}, error={}",
                                            ver.raw,
                                            e
                                        );
                                    }
                                }
                                for warning in &result.warnings {
                                    log::warn!(
                                        "Reversal warning for {}: {}",
                                        migration.script,
                                        warning
                                    );
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to generate reversal; script={}, error={}",
                                    migration.script,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                log::error!("Failed to rollback batch transaction: {}", rollback_err);
            }
            log::error!("Batch migration failed, all changes rolled back: {}", e);
            return Err(e);
        }
    }

    // Run afterMigrate hooks (outside the batch transaction)
    let after_placeholders = build_placeholders(
        &config.placeholders,
        schema,
        &setup.db_user,
        &setup.db_name,
        "afterMigrate",
    );
    let (count, ms) = hooks::run_hooks(
        client,
        &setup.all_hooks,
        &HookType::AfterMigrate,
        &after_placeholders,
    )
    .await?;
    report.hooks_executed += count;
    report.hooks_time_ms += ms;

    Ok(report)
}

/// Apply a single migration within a transaction.
///
/// Executes the migration SQL, records it in the history table, and optionally
/// commits the transaction. When `hold_transaction` is `true`, the transaction
/// is left open so the caller can evaluate ensure guards before committing.
#[allow(clippy::too_many_arguments)]
async fn apply_migration(
    client: &Client,
    config: &WaypointConfig,
    migration: &ResolvedMigration,
    schema: &str,
    table: &str,
    installed_by: &str,
    db_user: &str,
    db_name: &str,
    hold_transaction: bool,
) -> Result<i32> {
    log::info!(
        "Applying migration; migration={}, schema={}",
        migration.script,
        schema
    );

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

    // Execute migration SQL and history insert atomically in one transaction
    let start = std::time::Instant::now();
    client.batch_execute("BEGIN").await?;

    match client.batch_execute(&sql).await {
        Ok(()) => {
            let exec_time = start.elapsed().as_millis() as i32;
            // Record success inside the same transaction
            match history::insert_applied_migration(
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
            .await
            {
                Ok(()) => {
                    if !hold_transaction {
                        client.batch_execute("COMMIT").await?;
                    }
                    Ok(exec_time)
                }
                Err(e) => {
                    if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                        log::error!("Failed to rollback transaction: {}", rollback_err);
                    }
                    Err(e)
                }
            }
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                log::error!("Failed to rollback transaction: {}", rollback_err);
            }

            // Record failure — best-effort outside the rolled-back transaction
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
                log::warn!(
                    "Failed to record migration failure in history table; script={}, error={}",
                    migration.script,
                    record_err
                );
            }

            // Extract detailed error message
            let reason = crate::error::format_db_error(&e);
            log::error!(
                "Migration failed; script={}, reason={}",
                migration.script,
                reason
            );
            Err(WaypointError::MigrationFailed {
                script: migration.script.clone(),
                reason,
            })
        }
    }
}

// ── MySQL migrate path (Phase 1: minimal viable) ──────────────────────────────
//
// This is a streamlined parallel implementation that supports the core flow on
// MySQL 8.0+: scan migrations, read history, apply pending migrations in
// installed_rank order, record results. It deliberately does NOT support:
//   - validate-on-migrate (Phase 2)
//   - preflight checks (Phase 3)
//   - guards (Phase 3)
//   - safety analysis / DANGER blocking (Phase 3)
//   - auto-reversal generation (Phase 3+)
//   - --transaction batch mode (intentional: MySQL DDL is non-transactional)
//   - per-statement progress output (cosmetic, easy add later)
//   - hooks (Phase 2)
//
// Things that DO work: placeholders, environment scoping, target-version,
// repeatable migrations, checksum recording, idempotent re-runs, advisory
// locking via GET_LOCK.

/// Execute the migrate command (MySQL).
pub async fn execute_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    execute_mysql_with_options(client, config, target_version, false).await
}

/// Execute the migrate command with options (MySQL).
pub async fn execute_mysql_with_options(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
    _force: bool,
) -> Result<MigrateReport> {
    let table = &config.migrations.table;

    client.acquire_lock(table).await?;

    let result = run_migrate_mysql(client, config, target_version).await;

    if let Err(e) = client.release_lock(table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    match &result {
        Ok(report) => {
            log::info!(
                "Migrate completed (mysql); migrations_applied={}, total_time_ms={}",
                report.migrations_applied,
                report.total_time_ms
            );
        }
        Err(e) => {
            log::error!("Migrate failed (mysql): {}", e);
        }
    }

    result
}

async fn run_migrate_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<MigrateReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    history::create_history_table_db(client, &schema, table).await?;

    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations_db(client, &schema, table).await?;

    // Load hooks (engine-agnostic file scanning + config-declared hooks).
    let mut all_hooks: Vec<ResolvedHook> = hooks::scan_hooks(&config.migrations.locations)?;
    let config_hooks = hooks::load_config_hooks(&config.hooks)?;
    all_hooks.extend(config_hooks);

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

    let target = target_version.map(MigrationVersion::parse).transpose()?;
    let baseline_version = applied
        .iter()
        .find(|a| a.migration_type == "BASELINE")
        .and_then(|a| a.version.as_ref())
        .map(|v| MigrationVersion::parse(v))
        .transpose()?;
    let effective_versions = history::effective_applied_versions(&applied);
    let highest_applied = effective_versions
        .iter()
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .max();
    let applied_scripts: HashMap<String, Option<i32>> = applied
        .iter()
        .filter(|a| a.success && a.version.is_none())
        .map(|a| (a.script.clone(), a.checksum))
        .collect();
    let current_env = config.migrations.environment.as_deref();

    // Filter pending versioned migrations.
    let pending_versioned: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| {
            if m.is_undo() {
                return false;
            }
            let v = match m.version() {
                Some(v) => v,
                None => return false,
            };
            if !m.is_versioned() {
                return false;
            }
            if effective_versions.contains(&v.raw) {
                return false;
            }
            if let Some(ref bl) = baseline_version {
                if v <= bl {
                    return false;
                }
            }
            if let Some(ref t) = target {
                if v > t {
                    return false;
                }
            }
            if !config.migrations.out_of_order {
                if let Some(ref hi) = highest_applied {
                    if v < hi {
                        return false;
                    }
                }
            }
            if !should_run_in_environment(&m.directives, current_env) {
                return false;
            }
            true
        })
        .collect();

    let pending_repeatables: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| {
            if m.version().is_some() || m.is_undo() {
                return false;
            }
            if !should_run_in_environment(&m.directives, current_env) {
                return false;
            }
            match applied_scripts.get(&m.script) {
                None => true,
                Some(prev) => prev != &Some(m.checksum),
            }
        })
        .collect();

    let mut report = MigrateReport {
        migrations_applied: 0,
        total_time_ms: 0,
        details: Vec::new(),
        hooks_executed: 0,
        hooks_time_ms: 0,
    };

    let mut sorted_versioned = pending_versioned.clone();
    sorted_versioned.sort_by(|a, b| a.version().unwrap().cmp(b.version().unwrap()));

    let has_pending = !sorted_versioned.is_empty() || !pending_repeatables.is_empty();

    // beforeMigrate hooks (only when there's actually pending work).
    if has_pending {
        let hook_placeholders = build_placeholders(
            &config.placeholders,
            &schema,
            &db_user,
            &db_name,
            "beforeMigrate",
        );
        let (count, ms) = hooks::run_hooks_db(
            client,
            &all_hooks,
            &HookType::BeforeMigrate,
            &hook_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;
    }

    for m in sorted_versioned {
        let placeholders =
            build_placeholders(&config.placeholders, &schema, &db_user, &db_name, &m.script);

        // beforeEachMigrate hooks fire before each individual migration.
        let (count, ms) = hooks::run_hooks_db(
            client,
            &all_hooks,
            &HookType::BeforeEachMigrate,
            &placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        let elapsed =
            apply_one_mysql(client, m, &schema, table, &installed_by, &placeholders).await?;
        report.migrations_applied += 1;
        report.total_time_ms += elapsed;
        report.details.push(MigrateDetail {
            version: m.version().map(|v| v.raw.clone()),
            description: m.description.clone(),
            script: m.script.clone(),
            execution_time_ms: elapsed,
        });

        // afterEachMigrate hooks fire after each individual migration.
        let (count, ms) = hooks::run_hooks_db(
            client,
            &all_hooks,
            &HookType::AfterEachMigrate,
            &placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;
    }

    for m in pending_repeatables {
        let placeholders =
            build_placeholders(&config.placeholders, &schema, &db_user, &db_name, &m.script);

        let (count, ms) = hooks::run_hooks_db(
            client,
            &all_hooks,
            &HookType::BeforeEachMigrate,
            &placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;

        let elapsed =
            apply_one_mysql(client, m, &schema, table, &installed_by, &placeholders).await?;
        report.migrations_applied += 1;
        report.total_time_ms += elapsed;
        report.details.push(MigrateDetail {
            version: None,
            description: m.description.clone(),
            script: m.script.clone(),
            execution_time_ms: elapsed,
        });

        let (count, ms) = hooks::run_hooks_db(
            client,
            &all_hooks,
            &HookType::AfterEachMigrate,
            &placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;
    }

    // afterMigrate hooks (only when there was actually pending work).
    if has_pending {
        let hook_placeholders = build_placeholders(
            &config.placeholders,
            &schema,
            &db_user,
            &db_name,
            "afterMigrate",
        );
        let (count, ms) = hooks::run_hooks_db(
            client,
            &all_hooks,
            &HookType::AfterMigrate,
            &hook_placeholders,
        )
        .await?;
        report.hooks_executed += count;
        report.hooks_time_ms += ms;
    }

    Ok(report)
}

async fn apply_one_mysql(
    client: &DbClient,
    m: &ResolvedMigration,
    schema: &str,
    table: &str,
    installed_by: &str,
    placeholders: &HashMap<String, String>,
) -> Result<i32> {
    let sql = replace_placeholders(&m.sql, placeholders)?;
    log::info!("Applying migration; script={}", m.script);
    let elapsed = client
        .execute_raw(&sql)
        .await
        .map_err(|e| WaypointError::MigrationFailed {
            script: m.script.clone(),
            reason: e.to_string(),
        })?;

    let migration_type = if m.version().is_some() {
        "SQL"
    } else {
        "SQL_REPEATABLE"
    };
    history::insert_applied_migration_db(
        client,
        schema,
        table,
        m.version().map(|v| v.raw.as_str()),
        &m.description,
        migration_type,
        &m.script,
        Some(m.checksum),
        installed_by,
        elapsed,
        true,
    )
    .await?;

    Ok(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_concurrent_index() {
        let sql = "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);";
        let result = validate_batch_compatible("V5__Add_index.sql", sql);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            WaypointError::NonTransactionalStatement { script, .. } => {
                assert_eq!(script, "V5__Add_index.sql");
            }
            _ => panic!("Expected NonTransactionalStatement, got {:?}", err),
        }
    }

    #[test]
    fn test_detect_drop_index_concurrently() {
        let sql = "DROP INDEX CONCURRENTLY idx_users_email;";
        let result = validate_batch_compatible("V6__Drop_index.sql", sql);
        assert!(result.is_err());
        match result.unwrap_err() {
            WaypointError::NonTransactionalStatement { statement, .. } => {
                assert!(statement.contains("DROP INDEX CONCURRENTLY"));
            }
            other => panic!("Expected NonTransactionalStatement, got {:?}", other),
        }
    }

    #[test]
    fn test_detect_vacuum() {
        let sql = "VACUUM ANALYZE users;";
        let result = validate_batch_compatible("V7__Vacuum.sql", sql);
        assert!(result.is_err());
        match result.unwrap_err() {
            WaypointError::NonTransactionalStatement { statement, .. } => {
                assert_eq!(statement, "VACUUM");
            }
            other => panic!("Expected NonTransactionalStatement, got {:?}", other),
        }
    }

    #[test]
    fn test_detect_create_database() {
        let sql = "CREATE DATABASE newdb;";
        let result = validate_batch_compatible("V8__Create_db.sql", sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_compatible_normal_ddl() {
        let sql =
            "CREATE TABLE users (id SERIAL PRIMARY KEY); CREATE INDEX idx_users ON users (id);";
        let result = validate_batch_compatible("V1__Init.sql", sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_run_in_environment_no_directives() {
        let directives = MigrationDirectives::default();
        assert!(should_run_in_environment(&directives, Some("production")));
        assert!(should_run_in_environment(&directives, None));
    }

    #[test]
    fn test_should_run_in_environment_matches() {
        let directives = MigrationDirectives {
            env: vec!["production".to_string(), "staging".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, Some("production")));
        assert!(should_run_in_environment(&directives, Some("staging")));
        assert!(!should_run_in_environment(&directives, Some("dev")));
    }

    #[test]
    fn test_should_run_in_environment_case_insensitive() {
        let directives = MigrationDirectives {
            env: vec!["PROD".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, Some("prod")));
        assert!(should_run_in_environment(&directives, Some("PROD")));
        assert!(should_run_in_environment(&directives, Some("Prod")));
        assert!(!should_run_in_environment(&directives, Some("dev")));
    }

    #[test]
    fn test_should_run_in_environment_no_env_configured() {
        let directives = MigrationDirectives {
            env: vec!["prod".to_string()],
            ..Default::default()
        };
        // No environment configured = runs everything
        assert!(should_run_in_environment(&directives, None));
    }
}
