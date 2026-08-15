//! Schema history table operations.
//!
//! This module hosts the engine-agnostic types ([`AppliedMigration`],
//! [`effective_applied_versions`]) and the dialect-aware dispatchers
//! (the `*_db` functions) that route to the per-engine implementations
//! in [`crate::engines::postgres::history`] / [`crate::engines::mysql::history`].
//!
//! Legacy PostgreSQL-only entry points (`create_history_table`,
//! `get_applied_migrations`, etc.) are re-exported from
//! [`crate::engines::postgres::history`] for back-compat — code that
//! previously called `crate::history::create_history_table(&Client, …)`
//! keeps working unchanged.

use chrono::{DateTime, Utc};

use crate::db::DbClient;
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

/// Row types whose checksum must never be realigned against a `V` file.
///
/// A `BASELINE` row has no script and carries a NULL checksum. An `UNDO_SQL`
/// row shares the forward migration's *version* but records the `U` file's
/// checksum, so matching it by version alone and rewriting it would store a
/// value the U file never had.
///
/// Two places must agree on this list: `commands::repair::compute_repair`,
/// which decides what to realign, and the `update_checksum` predicate in each
/// engine, which applies it. Keeping the list here is what stops them drifting
/// — the guard used to exist only in the deciding half.
pub const NON_REALIGNABLE_TYPES: [&str; 2] = ["UNDO_SQL", "BASELINE"];

// ── Re-exports of the legacy PG-only entry points ────────────────────────────
//
// External callers expect these names at `crate::history::*`. They live in
// `crate::engines::postgres::history` now; this just makes the rename a
// no-op for downstream code.

#[cfg(feature = "postgres")]
pub use crate::engines::postgres::history::{
    create_history_table, delete_failed_migrations, get_applied_migrations, has_entries,
    history_table_exists, insert_applied_migration, next_installed_rank, update_checksum,
    update_repeatable_checksum,
};

// ── Dialect-aware dispatchers ────────────────────────────────────────────────

/// Create the schema history table if it does not exist (dialect-aware).
pub async fn create_history_table_db(client: &DbClient, schema: &str, table: &str) -> Result<()> {
    let dialect = client.dialect();
    let ddl = dialect.history_table_ddl(schema, table);
    // PG accepts the multi-statement string; MySQL needs per-statement
    // execution. `execute_raw` handles both.
    if let Err(e) = client.execute_raw(&ddl).await {
        // Idempotent re-runs hit ER_DUP_KEYNAME on MySQL when an index already
        // exists; treat that as benign.
        if !is_benign_index_dup(&e) {
            return Err(e);
        }
    }
    upgrade_history_table_db(client, schema, table).await?;
    Ok(())
}

/// Auto-upgrade the history table to add new columns if they don't exist.
///
/// The column is looked up before the `ALTER` rather than issuing it blindly
/// and swallowing whatever comes back. A blind attempt cannot distinguish
/// "already there" (benign, and the common case) from "no ALTER privilege"
/// (fatal) — and the latter used to surface much later as an opaque
/// "column reversal_sql does not exist" from an unrelated `SELECT`.
async fn upgrade_history_table_db(client: &DbClient, schema: &str, table: &str) -> Result<()> {
    if history_column_exists_db(client, schema, table, "reversal_sql").await? {
        return Ok(());
    }

    let fq = client.dialect().qualified_table(schema, table);
    let sql = match client.dialect_kind() {
        crate::dialect::DialectKind::Postgres => {
            format!("ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS reversal_sql TEXT")
        }
        crate::dialect::DialectKind::Mysql => {
            // MySQL 8.0.29+ supports IF NOT EXISTS on ADD COLUMN; older patch
            // versions reject it, so plain ADD COLUMN is used after the
            // existence check above has already ruled out a duplicate.
            format!("ALTER TABLE {fq} ADD COLUMN reversal_sql LONGTEXT")
        }
    };

    client.execute_raw(&sql).await.map(|_| ()).map_err(|e| {
        WaypointError::ConfigError(format!(
            "Could not add the `reversal_sql` column to {}.{}: {}. Waypoint reads \
             this column on every history query, so the connecting role needs \
             ALTER on the history table at least once to complete the upgrade.",
            schema, table, e
        ))
    })
}

/// Whether a named column exists on the history table (dialect-aware).
async fn history_column_exists_db(
    client: &DbClient,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<bool> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            let row = c
                .query_one(
                    "SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
                    )",
                    &[&schema, &table, &column],
                )
                .await?;
            Ok(row.get::<_, bool>(0))
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let mut conn = pool.get_conn().await?;
            let found: Option<i64> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.columns \
                     WHERE table_schema = ? AND table_name = ? AND column_name = ? LIMIT 1",
                    (schema, table, column),
                )
                .await?;
            Ok(found.is_some())
        }
    }
}

/// Whether an error message indicates a benign "duplicate index/key name"
/// that occurs when re-running idempotent CREATE INDEX statements on MySQL.
fn is_benign_index_dup(e: &WaypointError) -> bool {
    // MySQL <8.0.29 lacks `CREATE INDEX IF NOT EXISTS` and emits ER_DUP_KEYNAME
    // (error 1061, message "Duplicate key name '...'") when an index already
    // exists. The CREATE TABLE / CREATE INDEX statements in our history DDL
    // are otherwise idempotent (IF NOT EXISTS on the table, MySQL 8.0.29+
    // accepts it on the index), so this is the only benign error we want to
    // swallow. Avoid matching a broader "already exists" substring — that
    // would also accept genuinely-broken cases like a table-creation race.
    let msg = e.to_string().to_lowercase();
    msg.contains("er_dup_keyname") || msg.contains("duplicate key name")
}

/// Check if the history table exists (dialect-aware).
pub async fn history_table_exists_db(client: &DbClient, schema: &str, table: &str) -> Result<bool> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            crate::engines::postgres::history::history_table_exists(c, schema, table).await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            crate::engines::mysql::history::history_table_exists(pool, schema, table).await
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
        DbClient::Postgres(c) => {
            crate::engines::postgres::history::get_applied_migrations(c, schema, table).await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            crate::engines::mysql::history::get_applied_migrations(pool, schema, table).await
        }
    }
}

/// Insert a migration record into the history table (dialect-aware).
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
            crate::engines::postgres::history::insert_applied_migration(
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
            crate::engines::mysql::history::insert_applied_migration(
                pool,
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
    }
}

/// Check if the history table has any entries (dialect-aware).
pub async fn has_entries_db(client: &DbClient, schema: &str, table: &str) -> Result<bool> {
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            crate::engines::postgres::history::has_entries(c, schema, table).await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            crate::engines::mysql::history::has_entries(pool, schema, table).await
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
        DbClient::Postgres(c) => {
            crate::engines::postgres::history::delete_failed_migrations(c, schema, table).await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            crate::engines::mysql::history::delete_failed_migrations(pool, schema, table).await
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
        DbClient::Postgres(c) => {
            crate::engines::postgres::history::update_checksum(
                c,
                schema,
                table,
                version,
                new_checksum,
            )
            .await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            crate::engines::mysql::history::update_checksum(
                pool,
                schema,
                table,
                version,
                new_checksum,
            )
            .await
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
            crate::engines::postgres::history::update_repeatable_checksum(
                c,
                schema,
                table,
                script,
                new_checksum,
            )
            .await
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            crate::engines::mysql::history::update_repeatable_checksum(
                pool,
                schema,
                table,
                script,
                new_checksum,
            )
            .await
        }
    }
}

// ── Engine-agnostic helpers ──────────────────────────────────────────────────

/// Compute the set of versions that are currently effectively applied.
///
/// Processes history rows in `installed_rank` order (assumed already sorted).
/// For each version, tracks whether the latest successful action was a
/// forward migration (`"SQL"` / `"BASELINE"`) or an undo (`"UNDO_SQL"`).
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
