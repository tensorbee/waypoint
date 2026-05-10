//! Drop all objects in managed schemas (destructive).

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
#[cfg(feature = "postgres")]
use crate::db;
#[cfg(feature = "postgres")]
use crate::db::quote_ident;
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};

/// Execute the clean command (PostgreSQL legacy entry).
///
/// Drop all tables, views, functions, sequences, types in managed schema(s).
/// Requires clean_enabled=true or allow_clean=true.
#[cfg(feature = "postgres")]
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    allow_clean: bool,
) -> Result<Vec<String>> {
    if !config.migrations.clean_enabled && !allow_clean {
        return Err(WaypointError::CleanDisabled);
    }

    let table = &config.migrations.table;

    // Acquire advisory lock to prevent concurrent operations
    db::acquire_advisory_lock(client, table).await?;

    let result = execute_inner_pg(client, config).await;

    // Always release the lock
    if let Err(e) = db::release_advisory_lock(client, table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

/// Execute the clean command (dialect-aware entry).
pub async fn execute_db(
    client: &DbClient,
    config: &WaypointConfig,
    allow_clean: bool,
) -> Result<Vec<String>> {
    if !config.migrations.clean_enabled && !allow_clean {
        return Err(WaypointError::CleanDisabled);
    }

    let table = &config.migrations.table;
    client.acquire_lock(table).await?;

    let result = match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => execute_inner_pg(client.as_postgres()?, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => execute_inner_mysql(client, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    };

    if let Err(e) = client.release_lock(table).await {
        log::error!("Failed to release advisory lock: {}", e);
    }

    result
}

#[cfg(feature = "postgres")]
async fn execute_inner_pg(client: &Client, config: &WaypointConfig) -> Result<Vec<String>> {
    let schema = &config.migrations.schema;
    let schema_q = quote_ident(schema);
    let mut dropped = Vec::new();

    log::warn!(
        "Starting clean — this will DROP all objects in the schema; schema={}",
        schema
    );

    // Drop materialized views
    let rows = client
        .query(
            "SELECT matviewname FROM pg_matviews WHERE schemaname = $1",
            &[&schema],
        )
        .await?;
    for row in rows {
        let name: String = row.get(0);
        let sql = format!(
            "DROP MATERIALIZED VIEW IF EXISTS {}.{} CASCADE",
            schema_q,
            quote_ident(&name)
        );
        client.batch_execute(&sql).await?;
        dropped.push(format!("Materialized view: {}.{}", schema, name));
    }

    // Drop views
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.views WHERE table_schema = $1",
            &[&schema],
        )
        .await?;
    for row in rows {
        let name: String = row.get(0);
        let sql = format!(
            "DROP VIEW IF EXISTS {}.{} CASCADE",
            schema_q,
            quote_ident(&name)
        );
        client.batch_execute(&sql).await?;
        dropped.push(format!("View: {}.{}", schema, name));
    }

    // Drop tables
    let rows = client
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname = $1",
            &[&schema],
        )
        .await?;
    for row in rows {
        let name: String = row.get(0);
        let sql = format!(
            "DROP TABLE IF EXISTS {}.{} CASCADE",
            schema_q,
            quote_ident(&name)
        );
        client.batch_execute(&sql).await?;
        dropped.push(format!("Table: {}.{}", schema, name));
    }

    // Drop sequences
    let rows = client
        .query(
            "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $1",
            &[&schema],
        )
        .await?;
    for row in rows {
        let name: String = row.get(0);
        let sql = format!(
            "DROP SEQUENCE IF EXISTS {}.{} CASCADE",
            schema_q,
            quote_ident(&name)
        );
        client.batch_execute(&sql).await?;
        dropped.push(format!("Sequence: {}.{}", schema, name));
    }

    // Drop functions/procedures
    let rows = client
        .query(
            "SELECT p.proname, pg_get_function_identity_arguments(p.oid) as args \
             FROM pg_proc p \
             JOIN pg_namespace n ON p.pronamespace = n.oid \
             WHERE n.nspname = $1",
            &[&schema],
        )
        .await?;
    for row in rows {
        let name: String = row.get(0);
        let args: String = row.get(1);
        let sql = format!(
            "DROP FUNCTION IF EXISTS {}.{}({}) CASCADE",
            schema_q,
            quote_ident(&name),
            args
        );
        client.batch_execute(&sql).await?;
        dropped.push(format!("Function: {}.{}", schema, name));
    }

    // Drop custom types (enums, composites)
    let rows = client
        .query(
            "SELECT t.typname \
             FROM pg_type t \
             JOIN pg_namespace n ON t.typnamespace = n.oid \
             WHERE n.nspname = $1 \
             AND t.typtype IN ('e', 'c') \
             AND t.typname NOT LIKE '\\_%'",
            &[&schema],
        )
        .await?;
    for row in rows {
        let name: String = row.get(0);
        let sql = format!(
            "DROP TYPE IF EXISTS {}.{} CASCADE",
            schema_q,
            quote_ident(&name)
        );
        client.batch_execute(&sql).await?;
        dropped.push(format!("Type: {}.{}", schema, name));
    }

    log::warn!(
        "Clean completed; schema={}, objects_dropped={}",
        schema,
        dropped.len()
    );

    Ok(dropped)
}

#[cfg(feature = "mysql")]
async fn execute_inner_mysql(client: &DbClient, config: &WaypointConfig) -> Result<Vec<String>> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let mut dropped = Vec::new();

    log::warn!(
        "Starting clean — this will DROP all objects in the database; database={}",
        schema
    );

    let mut conn = pool.get_conn().await?;
    // Disable FK checks for the duration of the clean so we don't have to
    // worry about drop order. Restored before returning.
    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0").await?;

    // Drop views first — they can reference tables and dropping the table
    // first leaves "invalid view" warnings.
    let views: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?",
            (schema.as_str(),),
        )
        .await?;
    for name in views {
        let sql = format!("DROP VIEW IF EXISTS `{}`.`{}`", schema, name);
        conn.query_drop(&sql).await?;
        dropped.push(format!("View: {}.{}", schema, name));
    }

    // Drop base tables
    let tables: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
            (schema.as_str(),),
        )
        .await?;
    for name in tables {
        let sql = format!("DROP TABLE IF EXISTS `{}`.`{}`", schema, name);
        conn.query_drop(&sql).await?;
        dropped.push(format!("Table: {}.{}", schema, name));
    }

    // Drop routines (procedures + functions)
    let routines: Vec<(String, String)> = conn
        .exec(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = ?",
            (schema.as_str(),),
        )
        .await?;
    for (name, kind) in routines {
        let kw = if kind.eq_ignore_ascii_case("PROCEDURE") {
            "PROCEDURE"
        } else {
            "FUNCTION"
        };
        let sql = format!("DROP {} IF EXISTS `{}`.`{}`", kw, schema, name);
        conn.query_drop(&sql).await?;
        dropped.push(format!("{}: {}.{}", kw.to_ascii_lowercase(), schema, name));
    }

    // Drop events (rare but possible)
    let events: Vec<String> = conn
        .exec(
            "SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?",
            (schema.as_str(),),
        )
        .await?;
    for name in events {
        let sql = format!("DROP EVENT IF EXISTS `{}`.`{}`", schema, name);
        conn.query_drop(&sql).await?;
        dropped.push(format!("Event: {}.{}", schema, name));
    }

    // Restore FK checks regardless of whether anything failed above. (Errors
    // above propagate via `?` and skip this — that's fine because the
    // connection is short-lived; if the user runs clean again the session
    // resets.)
    conn.query_drop("SET FOREIGN_KEY_CHECKS = 1").await.ok();

    log::warn!(
        "Clean completed; database={}, objects_dropped={}",
        schema,
        dropped.len()
    );

    Ok(dropped)
}
