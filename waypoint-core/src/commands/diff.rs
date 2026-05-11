//! Compare live database schema against a target and generate migration SQL.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};
use crate::schema::{self, SchemaDiff};

/// Target to compare the current schema against.
pub enum DiffTarget {
    /// Compare against another database identified by its connection URL.
    Database(String),
}

/// Report produced by the diff command.
#[derive(Debug, Serialize)]
pub struct DiffReport {
    /// List of individual schema differences found.
    pub diffs: Vec<SchemaDiff>,
    /// DDL SQL statements generated to reconcile the differences.
    pub generated_sql: String,
    /// Whether any differences were detected.
    pub has_changes: bool,
}

/// Execute the diff command (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(
    client: &Client,
    config: &WaypointConfig,
    target: DiffTarget,
) -> Result<DiffReport> {
    let schema_name = &config.migrations.schema;

    let current = schema::introspect(client, schema_name).await?;

    let target_snapshot = match target {
        DiffTarget::Database(ref url) => {
            let target_client = crate::db::connect(url).await?;
            schema::introspect(&target_client, schema_name).await?
        }
    };

    let diffs = schema::diff(&current, &target_snapshot);
    let generated_sql = schema::generate_ddl(&diffs);
    let has_changes = !diffs.is_empty();

    Ok(DiffReport {
        diffs,
        generated_sql,
        has_changes,
    })
}

/// Execute the diff command (dialect-aware entry).
///
/// Generated SQL is PostgreSQL-flavored when comparing PG schemas. On MySQL
/// the structural `diffs` list is populated correctly but `generated_sql` is
/// best-effort PG-shaped — consume the structured diffs for MySQL until a
/// MySQL DDL generator lands.
pub async fn execute_db(
    client: &DbClient,
    config: &WaypointConfig,
    target: DiffTarget,
) -> Result<DiffReport> {
    let schema_name = client.resolve_schema(&config.migrations.schema).await?;

    let current = schema::introspect_db(client, &schema_name).await?;

    let target_snapshot = match target {
        DiffTarget::Database(ref url) => {
            let target_client = connect_for_url(url).await?;
            // The target's "schema" may not be the same as the source's on
            // MySQL — resolve the target's current database too.
            let target_schema = target_client
                .resolve_schema(&config.migrations.schema)
                .await?;
            schema::introspect_db(&target_client, &target_schema).await?
        }
    };

    let diffs = schema::diff(&current, &target_snapshot);
    let generated_sql = schema::generate_ddl(&diffs);
    let has_changes = !diffs.is_empty();

    Ok(DiffReport {
        diffs,
        generated_sql,
        has_changes,
    })
}

async fn connect_for_url(url: &str) -> Result<DbClient> {
    let kind = DialectKind::from_url(url).unwrap_or(DialectKind::Postgres);
    match kind {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            let c = crate::db::connect(url).await?;
            Ok(DbClient::with_postgres(c))
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            let pool = mysql_async::Pool::from_url(url)
                .map_err(|e| WaypointError::ConfigError(format!("Invalid MySQL URL: {}", e)))?;
            Ok(DbClient::with_mysql(pool))
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}
