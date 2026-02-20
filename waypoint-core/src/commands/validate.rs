use std::collections::HashMap;

use serde::Serialize;
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::error::{Result, WaypointError};
use crate::history;
use crate::migration::{ResolvedMigration, scan_migrations};

/// Report returned after a validate operation.
#[derive(Debug, Serialize)]
pub struct ValidateReport {
    pub valid: bool,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}

/// Execute the validate command.
///
/// For each applied (success=TRUE) migration in history:
/// - Find the corresponding file on disk
/// - Recalculate the checksum
/// - Report mismatches
/// - Warn if file is missing
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<ValidateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    if !history::history_table_exists(client, schema, table).await? {
        return Ok(ValidateReport {
            valid: true,
            issues: Vec::new(),
            warnings: vec!["No history table found — nothing to validate.".to_string()],
        });
    }

    let applied = history::get_applied_migrations(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;

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

    let mut issues = Vec::new();
    let mut warnings = Vec::new();

    for am in &applied {
        if !am.success {
            continue;
        }
        if am.migration_type == "BASELINE" {
            continue;
        }

        // Distinguish by version presence for Flyway compatibility
        if am.version.is_some() {
            // Versioned migration
            if let Some(ref version) = am.version {
                if let Some(resolved) = resolved_by_version.get(version) {
                    if let Some(expected_checksum) = am.checksum {
                        if resolved.checksum != expected_checksum {
                            issues.push(format!(
                                "Checksum mismatch for version {}: applied={}, resolved={}. \
                                 Migration file '{}' has been modified after it was applied.",
                                version, expected_checksum, resolved.checksum, resolved.script
                            ));
                        }
                    }
                } else {
                    warnings.push(format!(
                        "Applied migration version {} (script: {}) not found on disk.",
                        version, am.script
                    ));
                }
            }
        } else {
            // Repeatable (version is NULL) — we only warn on missing, don't fail on checksum diff
            // (checksum diff is expected and triggers re-apply)
            if !resolved_by_script.contains_key(&am.script) {
                warnings.push(format!(
                    "Applied repeatable migration '{}' not found on disk.",
                    am.script
                ));
            }
        }
    }

    let valid = issues.is_empty();

    tracing::info!(
        valid = valid,
        issue_count = issues.len(),
        warning_count = warnings.len(),
        "Validation completed"
    );

    if !valid {
        return Err(WaypointError::ValidationFailed(issues.join("\n")));
    }

    Ok(ValidateReport {
        valid,
        issues,
        warnings,
    })
}
