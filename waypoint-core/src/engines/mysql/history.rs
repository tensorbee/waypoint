//! MySQL implementation of schema-history-table operations.
//!
//! The dialect-aware dispatchers live in [`crate::history`]. This module
//! provides the engine-specific bodies for the MySQL backend.

use chrono::{DateTime, NaiveDateTime, Utc};
use mysql_async::prelude::*;
use mysql_async::Pool;

use crate::dialect::{mysql::MysqlDialect, DatabaseDialect};
use crate::error::{Result, WaypointError};
use crate::history::AppliedMigration;

/// Build a fully-qualified MySQL-quoted table name (`` `schema`.`table` ``).
fn fq(schema: &str, table: &str) -> String {
    MysqlDialect.qualified_table(schema, table)
}

/// Check if the history table exists on MySQL.
pub async fn history_table_exists(pool: &Pool, schema: &str, table: &str) -> Result<bool> {
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

/// Read all applied migrations ordered by `installed_rank` from MySQL.
pub async fn get_applied_migrations(
    pool: &Pool,
    schema: &str,
    table: &str,
) -> Result<Vec<AppliedMigration>> {
    let sql = format!(
        "SELECT installed_rank, version, description, type, script, checksum, \
         installed_by, installed_on, execution_time, success, reversal_sql \
         FROM {} ORDER BY installed_rank",
        fq(schema, table)
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
        let installed_on = DateTime::<Utc>::from_naive_utc_and_offset(installed_on_raw, Utc);
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

/// Insert a migration record. MySQL doesn't allow self-referencing the
/// target table in an INSERT subquery, so we compute the next rank with a
/// read-then-insert pair. Safe under the advisory lock that migrate holds.
#[allow(clippy::too_many_arguments)]
pub async fn insert_applied_migration(
    pool: &Pool,
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
    let fq = fq(schema, table);
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

/// Check if the history table has any entries.
pub async fn has_entries(pool: &Pool, schema: &str, table: &str) -> Result<bool> {
    let sql = format!("SELECT 1 FROM {} LIMIT 1", fq(schema, table));
    let mut conn = pool.get_conn().await?;
    let row: Option<i64> = conn.query_first(&sql).await?;
    Ok(row.is_some())
}

/// Delete all failed migration records.
pub async fn delete_failed_migrations(pool: &Pool, schema: &str, table: &str) -> Result<u64> {
    let sql = format!("DELETE FROM {} WHERE success = 0", fq(schema, table));
    let mut conn = pool.get_conn().await?;
    conn.query_drop(&sql).await?;
    Ok(conn.affected_rows())
}

/// Update the checksum for a versioned migration.
pub async fn update_checksum(
    pool: &Pool,
    schema: &str,
    table: &str,
    version: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {} SET checksum = ? WHERE version = ?",
        fq(schema, table)
    );
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(&sql, (new_checksum, version)).await?;
    Ok(())
}

/// Update the checksum for a repeatable migration.
pub async fn update_repeatable_checksum(
    pool: &Pool,
    schema: &str,
    table: &str,
    script: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {} SET checksum = ? WHERE script = ? AND version IS NULL",
        fq(schema, table)
    );
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(&sql, (new_checksum, script)).await?;
    Ok(())
}
