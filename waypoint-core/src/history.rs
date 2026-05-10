//! Schema history table operations (create, query, insert, update, delete).
//!
//! The original PostgreSQL-specific functions take `&tokio_postgres::Client` and
//! are still in use by command modules that haven't been ported to dialect-aware
//! operation. The dialect-aware variants (suffixed `_db`) take `&DbClient` and
//! dispatch to either the PG path or a MySQL implementation.

#[cfg(feature = "mysql")]
use chrono::NaiveDateTime;
use chrono::{DateTime, Utc};

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::db::{quote_ident, DbClient};
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};

/// A row from the schema history table.
#[derive(Debug, Clone)]
pub struct AppliedMigration {
    /// Monotonically increasing rank indicating the order of installation.
    pub installed_rank: i32,
    /// Migration version string, or `None` for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description of the migration.
    pub description: String,
    /// Type of migration (e.g., `"SQL"`, `"SQL_REPEATABLE"`, `"UNDO_SQL"`, `"BASELINE"`).
    pub migration_type: String,
    /// Filename of the migration script.
    pub script: String,
    /// CRC32 checksum of the migration SQL, or `None` for baselines.
    pub checksum: Option<i32>,
    /// Database user or custom identifier that applied the migration.
    pub installed_by: String,
    /// Timestamp when the migration was applied.
    pub installed_on: DateTime<Utc>,
    /// Time in milliseconds the migration took to execute.
    pub execution_time: i32,
    /// Whether the migration completed successfully.
    pub success: bool,
    /// Auto-generated reverse SQL, if available.
    pub reversal_sql: Option<String>,
}

/// Create the schema history table if it does not exist.
#[cfg(feature = "postgres")]
pub async fn create_history_table(client: &Client, schema: &str, table: &str) -> Result<()> {
    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let idx_name = format!("{}_s_idx", table);
    let ver_idx_name = format!("{}_v_idx", table);
    let sql = format!(
        r#"
CREATE TABLE IF NOT EXISTS {fq} (
    installed_rank INTEGER PRIMARY KEY,
    version        VARCHAR(50),
    description    VARCHAR(200) NOT NULL,
    type           VARCHAR(20) NOT NULL,
    script         VARCHAR(1000) NOT NULL,
    checksum       INTEGER,
    installed_by   VARCHAR(100) NOT NULL,
    installed_on   TIMESTAMPTZ NOT NULL DEFAULT now(),
    execution_time INTEGER NOT NULL,
    success        BOOLEAN NOT NULL,
    reversal_sql   TEXT
);

CREATE INDEX IF NOT EXISTS {idx_name} ON {fq} (success);
CREATE INDEX IF NOT EXISTS {ver_idx_name} ON {fq} (version);
"#,
        fq = fq,
        idx_name = quote_ident(&idx_name),
        ver_idx_name = quote_ident(&ver_idx_name),
    );

    client.batch_execute(&sql).await?;

    // Auto-upgrade: add reversal_sql column if table already existed without it
    upgrade_history_table(client, schema, table).await?;

    Ok(())
}

/// Auto-upgrade the history table to add new columns if they don't exist.
#[cfg(feature = "postgres")]
async fn upgrade_history_table(client: &Client, schema: &str, table: &str) -> Result<()> {
    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    // Add reversal_sql column if it doesn't exist
    let sql = format!(
        "ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS reversal_sql TEXT",
        fq = fq,
    );
    // Ignore errors (e.g., if the column already exists on older PG without IF NOT EXISTS)
    if let Err(e) = client.batch_execute(&sql).await {
        log::debug!("History table upgrade (reversal_sql): {}", e);
    }
    Ok(())
}

/// Check if the history table exists.
#[cfg(feature = "postgres")]
pub async fn history_table_exists(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &table],
        )
        .await?;

    Ok(row.get::<_, bool>(0))
}

/// Get the next installed_rank value.
#[cfg(feature = "postgres")]
pub async fn next_installed_rank(client: &Client, schema: &str, table: &str) -> Result<i32> {
    let sql = format!(
        "SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {}.{}",
        quote_ident(schema),
        quote_ident(table)
    );
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get::<_, i32>(0))
}

/// Query all applied migrations from the history table.
#[cfg(feature = "postgres")]
pub async fn get_applied_migrations(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<AppliedMigration>> {
    let sql = format!(
        "SELECT installed_rank, version, description, type, script, checksum, \
         installed_by, installed_on, execution_time, success, reversal_sql \
         FROM {}.{} ORDER BY installed_rank",
        quote_ident(schema),
        quote_ident(table)
    );

    let rows = client.query(&sql, &[]).await?;

    let mut migrations = Vec::with_capacity(rows.len());
    for row in rows {
        migrations.push(AppliedMigration {
            installed_rank: row.get(0),
            version: row.get(1),
            description: row.get(2),
            migration_type: row.get(3),
            script: row.get(4),
            checksum: row.get(5),
            installed_by: row.get(6),
            installed_on: row.get(7),
            execution_time: row.get(8),
            success: row.get(9),
            reversal_sql: row.get(10),
        });
    }

    Ok(migrations)
}

/// Insert a migration record into the history table with atomic rank assignment.
///
/// Uses a subquery to atomically compute the next installed_rank within the INSERT,
/// eliminating the race between reading the max rank and inserting.
#[cfg(feature = "postgres")]
#[allow(clippy::too_many_arguments)]
pub async fn insert_applied_migration(
    client: &Client,
    schema: &str,
    table: &str,
    version: Option<&str>,
    description: &str,
    migration_type: &str,
    script: &str,
    checksum: Option<i32>,
    installed_by: &str,
    execution_time: i32,
    success: bool,
) -> Result<()> {
    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let sql = format!(
        "INSERT INTO {fq} \
         (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) \
         VALUES (\
            (SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {fq}), \
            $1, $2, $3, $4, $5, $6, $7, $8\
         )",
        fq = fq,
    );

    client
        .execute(
            &sql,
            &[
                &version,
                &description,
                &migration_type,
                &script,
                &checksum,
                &installed_by,
                &execution_time,
                &success,
            ],
        )
        .await?;

    Ok(())
}

/// Delete all failed migration records (success = FALSE).
#[cfg(feature = "postgres")]
pub async fn delete_failed_migrations(client: &Client, schema: &str, table: &str) -> Result<u64> {
    let sql = format!(
        "DELETE FROM {}.{} WHERE success = FALSE",
        quote_ident(schema),
        quote_ident(table)
    );
    let count = client.execute(&sql, &[]).await?;
    Ok(count)
}

/// Update the checksum for a specific migration by version.
#[cfg(feature = "postgres")]
pub async fn update_checksum(
    client: &Client,
    schema: &str,
    table: &str,
    version: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {}.{} SET checksum = $1 WHERE version = $2",
        quote_ident(schema),
        quote_ident(table)
    );
    client.execute(&sql, &[&new_checksum, &version]).await?;
    Ok(())
}

/// Update the checksum for a repeatable migration by script name (version is NULL).
#[cfg(feature = "postgres")]
pub async fn update_repeatable_checksum(
    client: &Client,
    schema: &str,
    table: &str,
    script: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {}.{} SET checksum = $1 WHERE script = $2 AND version IS NULL",
        quote_ident(schema),
        quote_ident(table)
    );
    client.execute(&sql, &[&new_checksum, &script]).await?;
    Ok(())
}

/// Compute the set of versions that are currently effectively applied.
///
/// Processes history rows in `installed_rank` order (assumed already sorted).
/// For each version, tracks whether the latest successful action was a forward
/// migration (`"SQL"` / `"BASELINE"`) or an undo (`"UNDO_SQL"`).
/// Returns the set of version strings that are currently applied.
pub fn effective_applied_versions(
    applied: &[AppliedMigration],
) -> std::collections::HashSet<String> {
    let mut effective = std::collections::HashSet::new();
    for am in applied {
        if !am.success {
            continue;
        }
        if let Some(ref version) = am.version {
            if am.migration_type == "UNDO_SQL" {
                effective.remove(version);
            } else {
                effective.insert(version.clone());
            }
        }
    }
    effective
}

/// Check if the history table has any entries.
#[cfg(feature = "postgres")]
pub async fn has_entries(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM {}.{})",
        quote_ident(schema),
        quote_ident(table)
    );
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get::<_, bool>(0))
}

// ── Dialect-aware variants ────────────────────────────────────────────────────
//
// These take `&DbClient` and dispatch on engine. They are the long-term API;
// the `_db` suffix distinguishes them from the legacy PG-only entries above
// (which will be removed once every command is ported).

/// Create the schema history table if it does not exist (dialect-aware).
pub async fn create_history_table_db(client: &DbClient, schema: &str, table: &str) -> Result<()> {
    let dialect = client.dialect();
    let ddl = dialect.history_table_ddl(schema, table);
    // Run the DDL. PG can take the multi-statement string directly; MySQL needs
    // statement-by-statement execution. `execute_raw` handles both.
    if let Err(e) = client.execute_raw(&ddl).await {
        // CREATE INDEX has no IF NOT EXISTS in MySQL pre-8.0.29 reliably; if the
        // index already exists, MySQL returns ER_DUP_KEYNAME (1061). We treat
        // that as benign here so re-running on an existing schema is idempotent.
        if !is_benign_index_dup(&e) {
            return Err(e);
        }
    }

    // Auto-upgrade: ensure reversal_sql column exists on legacy installations.
    upgrade_history_table_db(client, schema, table).await?;
    Ok(())
}

/// Auto-upgrade the history table to add new columns if they don't exist.
async fn upgrade_history_table_db(client: &DbClient, schema: &str, table: &str) -> Result<()> {
    let dialect = client.dialect();
    let fq = dialect.qualified_table(schema, table);
    let sql = match client.dialect_kind() {
        DialectKind::Postgres => {
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS reversal_sql TEXT",
                fq
            )
        }
        DialectKind::Mysql => {
            // MySQL 8.0 supports IF NOT EXISTS on ADD COLUMN (since 8.0.29);
            // for older patch versions we fall back to ignoring the duplicate
            // column error.
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS reversal_sql LONGTEXT",
                fq
            )
        }
    };
    if let Err(e) = client.execute_raw(&sql).await {
        log::debug!("History table upgrade (reversal_sql): {}", e);
    }
    Ok(())
}

/// Whether an error message indicates a benign "duplicate index/key name" that
/// occurs when re-running idempotent CREATE INDEX statements on MySQL.
fn is_benign_index_dup(e: &WaypointError) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("er_dup_keyname")
        || msg.contains("duplicate key name")
        || msg.contains("already exists")
}

/// Check if the history table exists (dialect-aware).
pub async fn history_table_exists_db(client: &DbClient, schema: &str, table: &str) -> Result<bool> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => history_table_exists(c, schema, table).await,
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let mut conn = pool.get_conn().await?;
            let exists: Option<i64> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.tables \
                     WHERE table_schema = ? AND table_name = ? LIMIT 1",
                    (schema, table),
                )
                .await?;
            Ok(exists.is_some())
        }
    }
}

/// Read all applied migrations ordered by `installed_rank` (dialect-aware).
pub async fn get_applied_migrations_db(
    client: &DbClient,
    schema: &str,
    table: &str,
) -> Result<Vec<AppliedMigration>> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => get_applied_migrations(c, schema, table).await,
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let dialect = client.dialect();
            let fq = dialect.qualified_table(schema, table);
            let sql = format!(
                "SELECT installed_rank, version, description, type, script, checksum, \
                 installed_by, installed_on, execution_time, success, reversal_sql \
                 FROM {} ORDER BY installed_rank",
                fq
            );
            let mut conn = pool.get_conn().await?;
            let rows: Vec<mysql_async::Row> = conn.query(&sql).await?;
            let mut out = Vec::with_capacity(rows.len());
            for mut row in rows {
                let installed_rank: i32 = row
                    .take("installed_rank")
                    .ok_or_else(|| WaypointError::ConfigError("missing installed_rank".into()))?;
                let version: Option<String> = row.take("version").unwrap_or(None);
                let description: String = row
                    .take("description")
                    .ok_or_else(|| WaypointError::ConfigError("missing description".into()))?;
                let migration_type: String = row
                    .take("type")
                    .ok_or_else(|| WaypointError::ConfigError("missing type".into()))?;
                let script: String = row
                    .take("script")
                    .ok_or_else(|| WaypointError::ConfigError("missing script".into()))?;
                let checksum: Option<i32> = row.take("checksum").unwrap_or(None);
                let installed_by: String = row
                    .take("installed_by")
                    .ok_or_else(|| WaypointError::ConfigError("missing installed_by".into()))?;
                // MySQL TIMESTAMP comes back as NaiveDateTime (UTC by our DDL).
                let installed_on_raw: NaiveDateTime = row
                    .take("installed_on")
                    .ok_or_else(|| WaypointError::ConfigError("missing installed_on".into()))?;
                let installed_on =
                    DateTime::<Utc>::from_naive_utc_and_offset(installed_on_raw, Utc);
                let execution_time: i32 = row
                    .take("execution_time")
                    .ok_or_else(|| WaypointError::ConfigError("missing execution_time".into()))?;
                let success_raw: i8 = row
                    .take("success")
                    .ok_or_else(|| WaypointError::ConfigError("missing success".into()))?;
                let success = success_raw != 0;
                let reversal_sql: Option<String> = row.take("reversal_sql").unwrap_or(None);

                out.push(AppliedMigration {
                    installed_rank,
                    version,
                    description,
                    migration_type,
                    script,
                    checksum,
                    installed_by,
                    installed_on,
                    execution_time,
                    success,
                    reversal_sql,
                });
            }
            Ok(out)
        }
    }
}

/// Insert a migration record into the history table (dialect-aware).
///
/// Uses an atomic subquery to compute the next `installed_rank`, eliminating
/// the read-then-insert race.
#[allow(clippy::too_many_arguments)]
pub async fn insert_applied_migration_db(
    client: &DbClient,
    schema: &str,
    table: &str,
    version: Option<&str>,
    description: &str,
    migration_type: &str,
    script: &str,
    checksum: Option<i32>,
    installed_by: &str,
    execution_time: i32,
    success: bool,
) -> Result<()> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            insert_applied_migration(
                c,
                schema,
                table,
                version,
                description,
                migration_type,
                script,
                checksum,
                installed_by,
                execution_time,
                success,
            )
            .await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let dialect = client.dialect();
            let fq = dialect.qualified_table(schema, table);
            // MySQL doesn't allow referencing the target table in the same
            // INSERT subquery, so we read the next rank first and insert with
            // that constant. We hold the advisory lock during migrate, which
            // serialises this against other waypoint runs; in-tx concurrency
            // within a single migration run isn't a concern.
            let sql_max = format!("SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {}", fq);
            let mut conn = pool.get_conn().await?;
            let next_rank: i32 = conn.query_first(&sql_max).await?.unwrap_or(1);
            let insert_sql = format!(
                "INSERT INTO {} \
                 (installed_rank, version, description, type, script, checksum, \
                  installed_by, execution_time, success) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                fq
            );
            conn.exec_drop(
                &insert_sql,
                (
                    next_rank,
                    version,
                    description,
                    migration_type,
                    script,
                    checksum,
                    installed_by,
                    execution_time,
                    success as i8,
                ),
            )
            .await?;
            Ok(())
        }
    }
}

/// Check if the history table has any entries (dialect-aware).
pub async fn has_entries_db(client: &DbClient, schema: &str, table: &str) -> Result<bool> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => has_entries(c, schema, table).await,
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let dialect = client.dialect();
            let fq = dialect.qualified_table(schema, table);
            let sql = format!("SELECT 1 FROM {} LIMIT 1", fq);
            let mut conn = pool.get_conn().await?;
            let row: Option<i64> = conn.query_first(&sql).await?;
            Ok(row.is_some())
        }
    }
}

/// Delete all failed migration records (dialect-aware).
pub async fn delete_failed_migrations_db(
    client: &DbClient,
    schema: &str,
    table: &str,
) -> Result<u64> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => delete_failed_migrations(c, schema, table).await,
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let dialect = client.dialect();
            let fq = dialect.qualified_table(schema, table);
            let sql = format!("DELETE FROM {} WHERE success = 0", fq);
            let mut conn = pool.get_conn().await?;
            conn.query_drop(&sql).await?;
            Ok(conn.affected_rows())
        }
    }
}

/// Update the checksum for a versioned migration (dialect-aware).
pub async fn update_checksum_db(
    client: &DbClient,
    schema: &str,
    table: &str,
    version: &str,
    new_checksum: i32,
) -> Result<()> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => update_checksum(c, schema, table, version, new_checksum).await,
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let dialect = client.dialect();
            let fq = dialect.qualified_table(schema, table);
            let sql = format!("UPDATE {} SET checksum = ? WHERE version = ?", fq);
            let mut conn = pool.get_conn().await?;
            conn.exec_drop(&sql, (new_checksum, version)).await?;
            Ok(())
        }
    }
}

/// Update the checksum for a repeatable migration (dialect-aware).
pub async fn update_repeatable_checksum_db(
    client: &DbClient,
    schema: &str,
    table: &str,
    script: &str,
    new_checksum: i32,
) -> Result<()> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            update_repeatable_checksum(c, schema, table, script, new_checksum).await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let dialect = client.dialect();
            let fq = dialect.qualified_table(schema, table);
            let sql = format!(
                "UPDATE {} SET checksum = ? WHERE script = ? AND version IS NULL",
                fq
            );
            let mut conn = pool.get_conn().await?;
            conn.exec_drop(&sql, (new_checksum, script)).await?;
            Ok(())
        }
    }
}
