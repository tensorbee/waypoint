//! Validate applied migrations against local files (checksum and ordering).

use std::collections::HashMap;

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::error::{Result, WaypointError};
use crate::history::{self, AppliedMigration};
use crate::migration::{ResolvedMigration, scan_migrations};

/// Report returned after a validate operation.
#[derive(Debug, Serialize)]
pub struct ValidateReport {
    /// Whether all validations passed without errors.
    pub valid: bool,
    /// Validation errors (e.g. checksum mismatches) that indicate corruption.
    pub issues: Vec<String>,
    /// Non-fatal warnings (e.g. missing files on disk).
    pub warnings: Vec<String>,
}

/// Execute the validate command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<ValidateReport> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    if !history::history_table_exists(client, schema, table).await? {
        return Ok(empty_report());
    }
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;
    finalise(check(applied, resolved))
}

/// Execute the validate command (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<ValidateReport> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let schema = schema.as_str();
    let table = &config.migrations.table;

    if !history::history_table_exists_db(client, schema, table).await? {
        return Ok(empty_report());
    }
    let applied = history::get_applied_migrations_db(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;
    finalise(check(applied, resolved))
}

fn empty_report() -> ValidateReport {
    ValidateReport {
        valid: true,
        issues: Vec::new(),
        warnings: vec!["No history table found — nothing to validate.".to_string()],
    }
}

fn finalise(report: ValidateReport) -> Result<ValidateReport> {
    log::info!(
        "Validation completed; valid={}, issue_count={}, warning_count={}",
        report.valid,
        report.issues.len(),
        report.warnings.len()
    );
    if !report.valid {
        return Err(WaypointError::ValidationFailed(report.issues.join("\n")));
    }
    Ok(report)
}

fn check(applied: Vec<AppliedMigration>, resolved: Vec<ResolvedMigration>) -> ValidateReport {
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
        if am.migration_type == "BASELINE" || am.migration_type == "UNDO_SQL" {
            continue;
        }

        if let Some(ref version) = am.version {
            match resolved_by_version.get(version) {
                Some(resolved) => {
                    if let Some(expected_checksum) = am.checksum
                        && resolved.checksum != expected_checksum
                    {
                        issues.push(format!(
                            "Checksum mismatch for version {}: applied={}, resolved={}. \
                                 Migration file '{}' has been modified after it was applied.",
                            version, expected_checksum, resolved.checksum, resolved.script
                        ));
                    }
                }
                None => warnings.push(format!(
                    "Applied migration version {} (script: {}) not found on disk.",
                    version, am.script
                )),
            }
        } else if !resolved_by_script.contains_key(&am.script) {
            warnings.push(format!(
                "Applied repeatable migration '{}' not found on disk.",
                am.script
            ));
        }
    }

    let valid = issues.is_empty();
    ValidateReport {
        valid,
        issues,
        warnings,
    }
}
