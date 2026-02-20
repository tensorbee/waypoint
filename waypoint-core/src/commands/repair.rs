//! Repair the schema history table (remove failed entries, update checksums).

use std::collections::HashMap;

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::Result;
use crate::history;
use crate::migration::{scan_migrations, ResolvedMigration};

/// Report returned after a repair operation.
#[derive(Debug, Serialize)]
pub struct RepairReport {
    pub failed_removed: u64,
    pub checksums_updated: usize,
    pub details: Vec<String>,
}

/// Execute the repair command.
///
/// 1. Delete all rows where success=FALSE
/// 2. For each remaining applied migration, recalculate checksum from current file and update if changed
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<RepairReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    // Ensure history table exists
    history::create_history_table(client, schema, table).await?;

    // Step 1: Delete failed migrations
    let failed_removed = history::delete_failed_migrations(client, schema, table).await?;

    let mut details = Vec::new();
    if failed_removed > 0 {
        details.push(format!("Removed {} failed migration(s)", failed_removed));
    }

    // Step 2: Update checksums for remaining applied migrations
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;

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

    let mut checksums_updated = 0;

    for am in &applied {
        if !am.success || am.migration_type == "BASELINE" {
            continue;
        }

        // Distinguish by version presence for Flyway compatibility
        if let Some(ref version) = am.version {
            if let Some(resolved) = resolved_by_version.get(version) {
                if am.checksum != Some(resolved.checksum) {
                    history::update_checksum(client, schema, table, version, resolved.checksum)
                        .await?;
                    details.push(format!(
                        "Updated checksum for version {} ({} -> {})",
                        version,
                        am.checksum.unwrap_or(0),
                        resolved.checksum
                    ));
                    checksums_updated += 1;
                }
            }
        } else if let Some(resolved) = resolved_by_script.get(&am.script) {
            if am.checksum != Some(resolved.checksum) {
                history::update_repeatable_checksum(
                    client,
                    schema,
                    table,
                    &am.script,
                    resolved.checksum,
                )
                .await?;
                details.push(format!(
                    "Updated checksum for repeatable '{}' ({} -> {})",
                    am.script,
                    am.checksum.unwrap_or(0),
                    resolved.checksum
                ));
                checksums_updated += 1;
            }
        }
    }

    tracing::info!(
        failed_removed = failed_removed,
        checksums_updated = checksums_updated,
        "Repair completed"
    );

    Ok(RepairReport {
        failed_removed,
        checksums_updated,
        details,
    })
}
