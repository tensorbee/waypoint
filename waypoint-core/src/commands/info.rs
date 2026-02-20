use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::Result;
use crate::history;
use crate::migration::{scan_migrations, MigrationKind, MigrationVersion, ResolvedMigration};

/// The state of a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum MigrationState {
    Pending,
    Applied,
    Failed,
    Missing,
    Outdated,
    OutOfOrder,
    BelowBaseline,
    Ignored,
    Baseline,
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationState::Pending => write!(f, "Pending"),
            MigrationState::Applied => write!(f, "Applied"),
            MigrationState::Failed => write!(f, "Failed"),
            MigrationState::Missing => write!(f, "Missing"),
            MigrationState::Outdated => write!(f, "Outdated"),
            MigrationState::OutOfOrder => write!(f, "Out of Order"),
            MigrationState::BelowBaseline => write!(f, "Below Baseline"),
            MigrationState::Ignored => write!(f, "Ignored"),
            MigrationState::Baseline => write!(f, "Baseline"),
        }
    }
}

/// Combined view of a migration (file + history).
#[derive(Debug, Clone, Serialize)]
pub struct MigrationInfo {
    pub version: Option<String>,
    pub description: String,
    pub migration_type: String,
    pub script: String,
    pub state: MigrationState,
    pub installed_on: Option<DateTime<Utc>>,
    pub execution_time: Option<i32>,
    pub checksum: Option<i32>,
}

/// Execute the info command: merge resolved files and applied history into a unified view.
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<Vec<MigrationInfo>> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    // Ensure history table exists
    if !history::history_table_exists(client, schema, table).await? {
        // No history table — all resolved migrations are Pending
        let resolved = scan_migrations(&config.migrations.locations)?;
        return Ok(resolved
            .into_iter()
            .map(|m| {
                let version = m.version().map(|v| v.raw.clone());
                let migration_type = m.migration_type().to_string();
                MigrationInfo {
                    version,
                    description: m.description,
                    migration_type,
                    script: m.script,
                    state: MigrationState::Pending,
                    installed_on: None,
                    execution_time: None,
                    checksum: Some(m.checksum),
                }
            })
            .collect());
    }

    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations(client, schema, table).await?;

    // Build lookup maps
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

    // Find baseline version
    let baseline_version = applied
        .iter()
        .find(|a| a.migration_type == "BASELINE")
        .and_then(|a| a.version.as_ref())
        .map(|v| MigrationVersion::parse(v))
        .transpose()?;

    // Highest applied version (use version presence, not type string, for Flyway compat)
    let highest_applied = applied
        .iter()
        .filter(|a| a.success && a.version.is_some())
        .filter_map(|a| a.version.as_ref())
        .filter_map(|v| MigrationVersion::parse(v).ok())
        .max();

    let mut infos: Vec<MigrationInfo> = Vec::new();

    // Process applied migrations first (to track what's in history)
    let mut seen_versions: HashMap<String, bool> = HashMap::new();
    let mut seen_scripts: HashMap<String, bool> = HashMap::new();

    for am in &applied {
        // Distinguish versioned vs repeatable by presence of version (not type string),
        // for compatibility with Flyway which stores both as type "SQL".
        let is_versioned = am.version.is_some();
        let is_repeatable = am.version.is_none() && am.migration_type != "BASELINE";

        let state = if am.migration_type == "BASELINE" {
            MigrationState::Baseline
        } else if !am.success {
            MigrationState::Failed
        } else if is_versioned {
            if let Some(ref version) = am.version {
                if resolved_by_version.contains_key(version) {
                    MigrationState::Applied
                } else {
                    MigrationState::Missing
                }
            } else {
                MigrationState::Applied
            }
        } else if is_repeatable {
            // Check if file still exists and if checksum changed
            if let Some(resolved) = resolved_by_script.get(&am.script) {
                if Some(resolved.checksum) != am.checksum {
                    MigrationState::Outdated
                } else {
                    MigrationState::Applied
                }
            } else {
                MigrationState::Missing
            }
        } else {
            MigrationState::Applied
        };

        if let Some(ref v) = am.version {
            seen_versions.insert(v.clone(), true);
        }
        if am.version.is_none() {
            seen_scripts.insert(am.script.clone(), true);
        }

        infos.push(MigrationInfo {
            version: am.version.clone(),
            description: am.description.clone(),
            migration_type: am.migration_type.clone(),
            script: am.script.clone(),
            state,
            installed_on: Some(am.installed_on),
            execution_time: Some(am.execution_time),
            checksum: am.checksum,
        });
    }

    // Add pending resolved migrations not in history
    for m in &resolved {
        match &m.kind {
            MigrationKind::Versioned(version) => {
                if seen_versions.contains_key(&version.raw) {
                    continue;
                }

                let state = if let Some(ref bv) = baseline_version {
                    if version <= bv {
                        MigrationState::BelowBaseline
                    } else if let Some(ref highest) = highest_applied {
                        if version < highest {
                            MigrationState::OutOfOrder
                        } else {
                            MigrationState::Pending
                        }
                    } else {
                        MigrationState::Pending
                    }
                } else if let Some(ref highest) = highest_applied {
                    if version < highest {
                        MigrationState::OutOfOrder
                    } else {
                        MigrationState::Pending
                    }
                } else {
                    MigrationState::Pending
                };

                infos.push(MigrationInfo {
                    version: Some(version.raw.clone()),
                    description: m.description.clone(),
                    migration_type: m.migration_type().to_string(),
                    script: m.script.clone(),
                    state,
                    installed_on: None,
                    execution_time: None,
                    checksum: Some(m.checksum),
                });
            }
            MigrationKind::Repeatable => {
                if seen_scripts.contains_key(&m.script) {
                    continue; // Already handled above (Applied or Outdated)
                }

                infos.push(MigrationInfo {
                    version: None,
                    description: m.description.clone(),
                    migration_type: m.migration_type().to_string(),
                    script: m.script.clone(),
                    state: MigrationState::Pending,
                    installed_on: None,
                    execution_time: None,
                    checksum: Some(m.checksum),
                });
            }
        }
    }

    // Sort: versioned by version, then repeatable by description
    infos.sort_by(|a, b| match (&a.version, &b.version) {
        (Some(av), Some(bv)) => {
            let pa = MigrationVersion::parse(av);
            let pb = MigrationVersion::parse(bv);
            match (pa, pb) {
                (Ok(pa), Ok(pb)) => pa.cmp(&pb),
                _ => av.cmp(bv),
            }
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.description.cmp(&b.description),
    });

    Ok(infos)
}
