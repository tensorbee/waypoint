//! Drop all objects in managed schemas (destructive).

use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db::quote_ident;
use crate::error::{Result, WaypointError};

/// Execute the clean command.
///
/// Drop all tables, views, functions, sequences, types in managed schema(s).
/// Requires clean_enabled=true or allow_clean=true.
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    allow_clean: bool,
) -> Result<Vec<String>> {
    if !config.migrations.clean_enabled && !allow_clean {
        return Err(WaypointError::CleanDisabled);
    }

    let schema = &config.migrations.schema;
    let schema_q = quote_ident(schema);
    let mut dropped = Vec::new();

    tracing::warn!(schema = %schema, "Starting clean — this will DROP all objects in the schema");

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

    tracing::warn!(schema = %schema, objects_dropped = dropped.len(), "Clean completed");

    Ok(dropped)
}
