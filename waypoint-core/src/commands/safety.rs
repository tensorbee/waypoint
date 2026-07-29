//! Standalone `waypoint safety` command for analyzing migration files.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::error::Result;
use crate::safety;

/// Report from the standalone safety analysis command.
#[derive(Debug, Clone, Serialize)]
pub struct SafetyCommandReport {
    /// Per-file safety reports.
    pub reports: Vec<safety::SafetyReport>,
    /// Overall verdict across all files.
    pub overall_verdict: safety::SafetyVerdict,
}

/// Analyze a single migration file for safety (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute_file(
    client: &Client,
    config: &WaypointConfig,
    file_path: &str,
) -> Result<safety::SafetyReport> {
    let sql = std::fs::read_to_string(file_path)?;
    let script = filename_from_path(file_path);
    safety::analyze_migration(
        client,
        &config.migrations.schema,
        &sql,
        &script,
        &config.safety,
    )
    .await
}

/// Analyze a single migration file for safety (dialect-aware entry).
pub async fn execute_file_db(
    client: &DbClient,
    config: &WaypointConfig,
    file_path: &str,
) -> Result<safety::SafetyReport> {
    let sql = std::fs::read_to_string(file_path)?;
    let script = filename_from_path(file_path);
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    safety::analyze_migration_db(client, &schema, &sql, &script, &config.safety).await
}

fn filename_from_path(file_path: &str) -> String {
    std::path::Path::new(file_path)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| file_path.to_string())
}

/// Analyze all pending migration files for safety (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
#[deprecated(
    since = "0.6.0",
    note = "Unused PostgreSQL-only entry point superseded by `execute_db`, which handles both engines. Will be removed in 1.0."
)]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<SafetyCommandReport> {
    use crate::history;
    use crate::migration::scan_migrations;

    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    history::create_history_table(client, schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations(client, schema, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let mut reports = Vec::new();
    let mut overall = safety::SafetyVerdict::Safe;

    for migration in &resolved {
        if migration.is_undo() {
            continue;
        }
        if let Some(version) = migration.version()
            && effective.contains(&version.raw)
        {
            continue;
        }

        let report = safety::analyze_migration(
            client,
            schema,
            &migration.sql,
            &migration.script,
            &config.safety,
        )
        .await?;

        if report.overall_verdict > overall {
            overall = report.overall_verdict;
        }
        reports.push(report);
    }

    Ok(SafetyCommandReport {
        reports,
        overall_verdict: overall,
    })
}

/// Analyze all pending migration files for safety (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<SafetyCommandReport> {
    use crate::history;
    use crate::migration::scan_migrations;

    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    history::create_history_table_db(client, &schema, table).await?;
    let resolved = scan_migrations(&config.migrations.locations)?;
    let applied = history::get_applied_migrations_db(client, &schema, table).await?;
    let effective = history::effective_applied_versions(&applied);

    let mut reports = Vec::new();
    let mut overall = safety::SafetyVerdict::Safe;

    for migration in &resolved {
        if migration.is_undo() {
            continue;
        }
        if let Some(version) = migration.version()
            && effective.contains(&version.raw)
        {
            continue;
        }

        let report = safety::analyze_migration_db(
            client,
            &schema,
            &migration.sql,
            &migration.script,
            &config.safety,
        )
        .await?;

        if report.overall_verdict > overall {
            overall = report.overall_verdict;
        }
        reports.push(report);
    }

    Ok(SafetyCommandReport {
        reports,
        overall_verdict: overall,
    })
}
