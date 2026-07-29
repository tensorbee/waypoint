//! Repair the schema history table (remove failed entries, update checksums).

use std::collections::HashMap;

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db;
use crate::db::DbClient;
use crate::error::Result;
use crate::history::{self, AppliedMigration};
use crate::migration::{ResolvedMigration, scan_migrations};

/// Report returned after a repair operation.
#[derive(Debug, Serialize)]
pub struct RepairReport {
    /// Number of failed migration entries removed from history.
    pub failed_removed: u64,
    /// Number of checksum values updated to match current files.
    pub checksums_updated: usize,
    /// Human-readable descriptions of each repair action taken.
    pub details: Vec<String>,
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

    db::acquire_advisory_lock(client, table).await?;

    let result = execute_inner_pg(client, config, schema, table).await;

    if let Err(e) = db::release_advisory_lock(client, table).await {
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
    history::create_history_table(client, schema, table).await?;

    let failed_removed = history::delete_failed_migrations(client, schema, table).await?;
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;

    let (mut details, checksums_to_apply) = compute_repair(&applied, &resolved);
    if failed_removed > 0 {
        details.insert(0, format!("Removed {} failed migration(s)", failed_removed));
    }
    let mut checksums_updated = 0;
    for ck in checksums_to_apply {
        match ck {
            RepairChecksum::Versioned { version, new } => {
                history::update_checksum(client, schema, table, &version, new).await?;
            }
            RepairChecksum::Repeatable { script, new } => {
                history::update_repeatable_checksum(client, schema, table, &script, new).await?;
            }
        }
        checksums_updated += 1;
    }

    log::info!(
        "Repair completed; failed_removed={}, checksums_updated={}",
        failed_removed,
        checksums_updated
    );

    Ok(RepairReport {
        failed_removed,
        checksums_updated,
        details,
    })
}

/// Execute the repair command (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<RepairReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    client.acquire_lock(table).await?;

    let result = execute_inner_db(client, config, &schema, table).await;

    if let Err(e) = client.release_lock(table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

async fn execute_inner_db(
    client: &DbClient,
    config: &WaypointConfig,
    schema: &str,
    table: &str,
) -> Result<RepairReport> {
    history::create_history_table_db(client, schema, table).await?;

    let failed_removed = history::delete_failed_migrations_db(client, schema, table).await?;
    let applied = history::get_applied_migrations_db(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;

    let (mut details, checksums_to_apply) = compute_repair(&applied, &resolved);
    if failed_removed > 0 {
        details.insert(0, format!("Removed {} failed migration(s)", failed_removed));
    }
    let mut checksums_updated = 0;
    for ck in checksums_to_apply {
        match ck {
            RepairChecksum::Versioned { version, new } => {
                history::update_checksum_db(client, schema, table, &version, new).await?;
            }
            RepairChecksum::Repeatable { script, new } => {
                history::update_repeatable_checksum_db(client, schema, table, &script, new).await?;
            }
        }
        checksums_updated += 1;
    }

    log::info!(
        "Repair completed; failed_removed={}, checksums_updated={}",
        failed_removed,
        checksums_updated
    );

    Ok(RepairReport {
        failed_removed,
        checksums_updated,
        details,
    })
}

enum RepairChecksum {
    Versioned { version: String, new: i32 },
    Repeatable { script: String, new: i32 },
}

fn compute_repair(
    applied: &[AppliedMigration],
    resolved: &[ResolvedMigration],
) -> (Vec<String>, Vec<RepairChecksum>) {
    let resolved_by_version: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| m.is_versioned())
        .filter_map(|m| m.version().map(|v| (v.raw.clone(), m)))
        .collect();

    let resolved_by_script: HashMap<String, &ResolvedMigration> = resolved
        .iter()
        .filter(|m| !m.is_versioned())
        .map(|m| (m.script.clone(), m))
        .collect();

    let mut details = Vec::new();
    let mut updates = Vec::new();

    for am in applied {
        // Skip rows that have no migration file to compare against. UNDO_SQL
        // rows carry the U-file's checksum, not the V-file's, so matching them
        // against `resolved_by_version` (which holds only V files) would
        // rewrite the undo row's checksum to the forward migration's.
        // `validate` skips the same two types — the pair must agree.
        if !am.success || am.migration_type == "BASELINE" || am.migration_type == "UNDO_SQL" {
            continue;
        }

        if let Some(ref version) = am.version {
            if let Some(resolved) = resolved_by_version.get(version)
                && am.checksum != Some(resolved.checksum)
            {
                details.push(format!(
                    "Updated checksum for version {} ({} -> {})",
                    version,
                    am.checksum.unwrap_or(0),
                    resolved.checksum
                ));
                updates.push(RepairChecksum::Versioned {
                    version: version.clone(),
                    new: resolved.checksum,
                });
            }
        } else if let Some(resolved) = resolved_by_script.get(&am.script)
            && am.checksum != Some(resolved.checksum)
        {
            details.push(format!(
                "Updated checksum for repeatable '{}' ({} -> {})",
                am.script,
                am.checksum.unwrap_or(0),
                resolved.checksum
            ));
            updates.push(RepairChecksum::Repeatable {
                script: am.script.clone(),
                new: resolved.checksum,
            });
        }
    }
    (details, updates)
}
