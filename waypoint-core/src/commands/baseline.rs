//! Baseline an existing database at a specific version.

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db;
use crate::db::DbClient;
use crate::error::{Result, WaypointError};
use crate::history;

/// Execute the baseline command (PostgreSQL legacy entry).
///
/// 1. Fail if history table already has entries
/// 2. Create history table
/// 3. Insert a single baseline row
#[cfg(feature = "postgres")]
#[deprecated(
    since = "0.6.0",
    note = "Unused PostgreSQL-only entry point superseded by `execute_db`, which handles both engines. Will be removed in 1.0."
)]
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    baseline_version: Option<&str>,
    baseline_description: Option<&str>,
) -> Result<()> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;

    db::acquire_advisory_lock(client, schema, table).await?;

    let result = execute_inner_pg(client, config, baseline_version, baseline_description).await;

    if let Err(e) = db::release_advisory_lock(client, schema, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

#[cfg(feature = "postgres")]
async fn execute_inner_pg(
    client: &Client,
    config: &WaypointConfig,
    baseline_version: Option<&str>,
    baseline_description: Option<&str>,
) -> Result<()> {
    let schema = &config.migrations.schema;
    let table = &config.migrations.table;
    let version = baseline_version.unwrap_or(&config.migrations.baseline_version);
    let description = baseline_description.unwrap_or("<< Waypoint Baseline >>");

    history::create_history_table(client, schema, table).await?;

    if history::has_entries(client, schema, table).await? {
        return Err(WaypointError::BaselineExists);
    }

    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or("waypoint");

    history::insert_applied_migration(
        client,
        schema,
        table,
        Some(version),
        description,
        "BASELINE",
        "<< Waypoint Baseline >>",
        None,
        installed_by,
        0,
        true,
    )
    .await?;

    log::info!(
        "Successfully baselined schema; version={}, schema={}",
        version,
        schema
    );
    Ok(())
}

/// Execute the baseline command (dialect-aware entry).
pub async fn execute_db(
    client: &DbClient,
    config: &WaypointConfig,
    baseline_version: Option<&str>,
    baseline_description: Option<&str>,
) -> Result<()> {
    // Resolve before locking: the lock key is scoped by schema, so it has to
    // be the same name the rest of the command uses.
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;

    client.acquire_lock(&schema, table).await?;

    let result = execute_inner_db(client, config, baseline_version, baseline_description).await;

    if let Err(e) = client.release_lock(&schema, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

async fn execute_inner_db(
    client: &DbClient,
    config: &WaypointConfig,
    baseline_version: Option<&str>,
    baseline_description: Option<&str>,
) -> Result<()> {
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let table = &config.migrations.table;
    let version = baseline_version.unwrap_or(&config.migrations.baseline_version);
    let description = baseline_description.unwrap_or("<< Waypoint Baseline >>");

    history::create_history_table_db(client, &schema, table).await?;

    if history::has_entries_db(client, &schema, table).await? {
        return Err(WaypointError::BaselineExists);
    }

    let installed_by = config
        .migrations
        .installed_by
        .as_deref()
        .unwrap_or("waypoint");

    history::insert_applied_migration_db(
        client,
        &schema,
        table,
        Some(version),
        description,
        "BASELINE",
        "<< Waypoint Baseline >>",
        None,
        installed_by,
        0,
        true,
    )
    .await?;

    log::info!(
        "Successfully baselined schema; version={}, schema={}",
        version,
        schema
    );
    Ok(())
}
