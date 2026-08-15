//! Repair the schema history table (remove failed entries, update checksums).

use std::collections::HashMap;

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db;
use crate::db::DbClient;
use crate::error::{Result, WaypointError};
use crate::history::{self, AppliedMigration};
use crate::migration::{ResolvedMigration, scan_migrations};

/// Report returned after a repair operation.
#[derive(Debug, Serialize)]
pub struct RepairReport {
    /// Number of failed migration entries removed from history — or, under a
    /// dry run, the number that *would* be removed.
    pub failed_removed: u64,
    /// Number of checksum values updated to match current files — or, under a
    /// dry run, the number that *would* be updated.
    pub checksums_updated: usize,
    /// Human-readable descriptions of each repair action. Phrased in the past
    /// tense for a real repair and as a proposal ("Would remove …") for a dry
    /// run, so the text alone tells an operator which one they got.
    pub details: Vec<String>,
    /// True when nothing was written: the counts above describe the work a
    /// real `repair` would perform, and the history table is untouched.
    pub dry_run: bool,
}

/// Execute the repair command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
#[deprecated(
    since = "0.6.0",
    note = "Unused PostgreSQL-only entry point superseded by `execute_db`, which handles both engines. Will be removed in 1.0."
)]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<RepairReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    db::acquire_advisory_lock(client, schema, table).await?;

    let result = execute_inner_pg(client, config, schema, table).await;

    if let Err(e) = db::release_advisory_lock(client, schema, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

#[cfg(feature = "postgres")]
async fn execute_inner_pg(
    client: &Client,
    config: &WaypointConfig,
    schema: &str,
    table: &str,
) -> Result<RepairReport> {
    ensure_migration_locations(&config.migrations.locations)?;

    history::create_history_table(client, schema, table).await?;

    let applied = history::get_applied_migrations(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;

    let (checksum_details, checksums_to_apply) = compute_repair(&applied, &resolved, false);
    let checksums_updated = checksums_to_apply.len();

    let failed_removed = history::delete_failed_migrations(client, schema, table).await?;
    for ck in checksums_to_apply {
        history::update_checksum(client, schema, table, &ck.version, ck.new).await?;
    }

    let details = assemble_details(failed_removed, checksum_details, false);

    log::info!(
        "Repair completed; failed_removed={}, checksums_updated={}",
        failed_removed,
        checksums_updated
    );

    Ok(RepairReport {
        failed_removed,
        checksums_updated,
        details,
        dry_run: false,
    })
}

/// Execute the repair command (dialect-aware entry).
///
/// Equivalent to [`execute_db_with`] with `dry_run = false`.
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<RepairReport> {
    execute_db_with(client, config, false).await
}

/// Execute the repair command, optionally as a preview.
///
/// With `dry_run = true` no statement that changes the schema history table is
/// issued — not the `DELETE` of failed rows, not the checksum `UPDATE`s, and
/// not the `CREATE TABLE IF NOT EXISTS` that a real repair uses to bootstrap a
/// missing ledger. The returned [`RepairReport`] describes the work a real
/// repair would perform, so calling this and then [`execute_db`] must report
/// the same counts.
pub async fn execute_db_with(
    client: &DbClient,
    config: &WaypointConfig,
    dry_run: bool,
) -> Result<RepairReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    client.acquire_lock(&schema, table).await?;

    let result = execute_inner_db(client, config, &schema, table, dry_run).await;

    if let Err(e) = client.release_lock(&schema, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

async fn execute_inner_db(
    client: &DbClient,
    config: &WaypointConfig,
    schema: &str,
    table: &str,
    dry_run: bool,
) -> Result<RepairReport> {
    ensure_migration_locations(&config.migrations.locations)?;

    if dry_run {
        // Creating the history table is itself a write, so a dry run must not
        // do it. A ledger that does not exist has nothing to repair.
        if !history::history_table_exists_db(client, schema, table).await? {
            log::info!(
                "Repair dry run: history table {}.{} does not exist; nothing to repair",
                schema,
                table
            );
            return Ok(RepairReport {
                failed_removed: 0,
                checksums_updated: 0,
                details: Vec::new(),
                dry_run: true,
            });
        }
    } else {
        history::create_history_table_db(client, schema, table).await?;
    }

    // Read the ledger *before* deleting anything so the same plan can be
    // computed with or without applying it. `compute_repair` ignores failed
    // rows, so reading pre-delete yields the checksum plan a post-delete read
    // would have — and both happen under the same lock.
    let applied = history::get_applied_migrations_db(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;

    let (checksum_details, checksums_to_apply) = compute_repair(&applied, &resolved, dry_run);
    let checksums_updated = checksums_to_apply.len();

    let failed_removed = if dry_run {
        applied.iter().filter(|m| !m.success).count() as u64
    } else {
        let removed = history::delete_failed_migrations_db(client, schema, table).await?;
        for ck in checksums_to_apply {
            history::update_checksum_db(client, schema, table, &ck.version, ck.new).await?;
        }
        removed
    };

    let details = assemble_details(failed_removed, checksum_details, dry_run);

    if dry_run {
        log::info!(
            "Repair dry run (no changes made); would remove failed={}, would update checksums={}",
            failed_removed,
            checksums_updated
        );
    } else {
        log::info!(
            "Repair completed; failed_removed={}, checksums_updated={}",
            failed_removed,
            checksums_updated
        );
    }

    Ok(RepairReport {
        failed_removed,
        checksums_updated,
        details,
        dry_run,
    })
}

/// Refuse to repair against migration locations that do not exist.
///
/// `scan_migrations` merely warns and skips a missing directory, which is right
/// for commands that only read files. For `repair` it is not: with no files on
/// disk every checksum comparison is vacuous, so the command would report
/// "nothing to update" for the wrong reason while still deleting failed rows.
/// A repair that cannot see what it is repairing against has no basis for any
/// decision, so it stops before touching the ledger.
fn ensure_migration_locations(locations: &[std::path::PathBuf]) -> Result<()> {
    let missing: Vec<String> = locations
        .iter()
        .filter(|l| !l.exists())
        .map(|l| l.display().to_string())
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    Err(WaypointError::ConfigError(format!(
        "repair: migration location(s) not found: {}. Repair compares the schema \
         history table against the migration files on disk; with no files to \
         compare against it cannot tell a stale checksum from a correct one, so \
         it will not modify the history table. Check `[migrations] locations` or \
         run from the project root.",
        missing.join(", ")
    )))
}

/// Build the human-readable action list, newest concern first.
fn assemble_details(failed: u64, mut checksum_details: Vec<String>, dry_run: bool) -> Vec<String> {
    if failed > 0 {
        let verb = if dry_run { "Would remove" } else { "Removed" };
        checksum_details.insert(0, format!("{} {} failed migration(s)", verb, failed));
    }
    checksum_details
}

/// A planned checksum realignment: one versioned migration's stored checksum
/// brought into line with the file on disk.
struct RepairChecksum {
    version: String,
    new: i32,
}

fn compute_repair(
    applied: &[AppliedMigration],
    resolved: &[ResolvedMigration],
    dry_run: bool,
) -> (Vec<String>, Vec<RepairChecksum>) {
    let verb = if dry_run { "Would update" } else { "Updated" };

    let resolved_by_version: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter_map(|m| m.version().map(|v| (v.raw.clone(), m)))
        .collect();

    let mut details = Vec::new();
    let mut updates = Vec::new();

    for am in applied {
        // Skip rows that have no V file to compare against. UNDO_SQL rows carry
        // the U file's checksum and BASELINE rows carry none, so realigning
        // either against `resolved_by_version` would store a value that was
        // never true. `validate` skips the same types, and the engines' UPDATE
        // predicates enforce it — see `history::NON_REALIGNABLE_TYPES`.
        if !am.success
            || crate::history::NON_REALIGNABLE_TYPES.contains(&am.migration_type.as_str())
        {
            continue;
        }

        // Repeatable migrations are deliberately excluded. A repeatable is
        // pending precisely when its stored checksum differs from the file
        // (`commands::migrate::select_pending`), so "realigning" it would mark
        // a modified script as applied *without running it* — the database
        // keeps the old definition and nothing reports the discrepancy. Drift
        // here is the designed re-run signal, not corruption, which is why
        // `validate` does not flag it either.
        let Some(ref version) = am.version else {
            continue;
        };

        if let Some(resolved) = resolved_by_version.get(version)
            && am.checksum != Some(resolved.checksum)
        {
            details.push(format!(
                "{} checksum for version {} ({} -> {})",
                verb,
                version,
                am.checksum.unwrap_or(0),
                resolved.checksum
            ));
            updates.push(RepairChecksum {
                version: version.clone(),
                new: resolved.checksum,
            });
        }
    }
    (details, updates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directive::MigrationDirectives;
    use crate::migration::{MigrationKind, MigrationVersion};
    use chrono::Utc;

    fn applied(version: &str, checksum: i32, success: bool) -> AppliedMigration {
        AppliedMigration {
            installed_rank: 1,
            version: Some(version.to_string()),
            description: "Test".to_string(),
            migration_type: "SQL".to_string(),
            script: format!("V{}__Test.sql", version),
            checksum: Some(checksum),
            installed_by: "test".to_string(),
            installed_on: Utc::now(),
            execution_time: 0,
            success,
            reversal_sql: None,
        }
    }

    fn resolved(version: &str, checksum: i32) -> ResolvedMigration {
        ResolvedMigration {
            kind: MigrationKind::Versioned(MigrationVersion::parse(version).unwrap()),
            description: "Test".to_string(),
            script: format!("V{}__Test.sql", version),
            checksum,
            sql: String::new(),
            directives: MigrationDirectives::default(),
        }
    }

    #[test]
    fn test_compute_repair_dry_run_phrases_details_as_a_proposal() {
        let applied = vec![applied("1", 111, true)];
        let files = vec![resolved("1", 222)];

        let (dry_details, dry_updates) = compute_repair(&applied, &files, true);
        let (real_details, real_updates) = compute_repair(&applied, &files, false);

        // Same plan either way — only the wording differs.
        assert_eq!(dry_updates.len(), real_updates.len());
        assert!(
            dry_details[0].starts_with("Would update checksum for version 1"),
            "dry-run detail was: {}",
            dry_details[0]
        );
        assert!(
            real_details[0].starts_with("Updated checksum for version 1"),
            "real detail was: {}",
            real_details[0]
        );
    }

    #[test]
    fn test_compute_repair_plan_is_identical_dry_and_real() {
        // Regression guard for issue #2, suggestion 4: a dry run followed by a
        // real repair must report the same pending work.
        let applied = vec![
            applied("1", 111, true),  // checksum drifted
            applied("2", 222, true),  // matches, no work
            applied("3", 333, false), // failed row
        ];
        let files = vec![resolved("1", 999), resolved("2", 222), resolved("3", 333)];

        let (_, dry) = compute_repair(&applied, &files, true);
        let (_, real) = compute_repair(&applied, &files, false);
        assert_eq!(dry.len(), 1);
        assert_eq!(real.len(), dry.len());

        // The failed row is counted the same way the dry-run path counts it.
        assert_eq!(applied.iter().filter(|m| !m.success).count(), 1);
    }

    #[test]
    fn test_assemble_details_uses_would_remove_under_dry_run() {
        assert_eq!(
            assemble_details(4, Vec::new(), true),
            vec!["Would remove 4 failed migration(s)".to_string()]
        );
        assert_eq!(
            assemble_details(4, Vec::new(), false),
            vec!["Removed 4 failed migration(s)".to_string()]
        );
        assert!(assemble_details(0, Vec::new(), true).is_empty());
    }

    #[test]
    fn test_ensure_migration_locations_rejects_missing_directory() {
        let missing = std::path::PathBuf::from("/nonexistent/waypoint/db/migrations");
        let err = ensure_migration_locations(&[missing]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("/nonexistent/waypoint/db/migrations"),
            "{}",
            msg
        );
        assert!(msg.contains("will not modify the history table"), "{}", msg);
    }

    #[test]
    fn test_ensure_migration_locations_accepts_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        ensure_migration_locations(&[dir.path().to_path_buf()]).unwrap();
    }

    #[test]
    fn test_ensure_migration_locations_rejects_when_only_one_of_many_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let locations = vec![
            dir.path().to_path_buf(),
            std::path::PathBuf::from("/nonexistent/waypoint/second"),
        ];
        assert!(ensure_migration_locations(&locations).is_err());
    }
}
