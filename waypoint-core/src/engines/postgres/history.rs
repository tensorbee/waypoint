//! PostgreSQL implementation of schema-history-table operations.
//!
//! The dialect-aware dispatchers live in [`crate::history`]. This module
//! provides the engine-specific bodies for the PostgreSQL backend.

use tokio_postgres::Client;

use crate::db::quote_ident;
use crate::error::Result;
use crate::history::AppliedMigration;

/// Create the schema history table if it does not exist.
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
    upgrade_history_table(client, schema, table).await?;
    Ok(())
}

/// Auto-upgrade the history table to add new columns if they don't exist.
///
/// Checks for the column before altering, so a genuine permission failure is
/// reported rather than swallowed. See the dialect-aware twin in
/// `crate::history::upgrade_history_table_db`.
async fn upgrade_history_table(client: &Client, schema: &str, table: &str) -> Result<()> {
    let exists = client
        .query_one(
            "SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = 'reversal_sql'
            )",
            &[&schema, &table],
        )
        .await?
        .get::<_, bool>(0);
    if exists {
        return Ok(());
    }

    let fq = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let sql = format!("ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS reversal_sql TEXT");
    client.batch_execute(&sql).await.map_err(|e| {
        crate::error::WaypointError::ConfigError(format!(
            "Could not add the `reversal_sql` column to {}.{}: {}. Waypoint reads \
             this column on every history query, so the connecting role needs \
             ALTER on the history table at least once to complete the upgrade.",
            schema, table, e
        ))
    })
}

/// Check if the history table exists.
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

/// Insert a migration record with atomic rank assignment.
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
///
/// `UNDO_SQL` and `BASELINE` rows are excluded even when they carry the same
/// version. An undo row records the *U* file's checksum and a baseline row has
/// none, so realigning them against the forward migration's file would write a
/// value that was never true. This predicate is the enforcement of the skip
/// list in `commands::repair::compute_repair` — the two must agree, and
/// `crate::history::REALIGNABLE_TYPES` names them in one place.
pub async fn update_checksum(
    client: &Client,
    schema: &str,
    table: &str,
    version: &str,
    new_checksum: i32,
) -> Result<()> {
    let sql = format!(
        "UPDATE {}.{} SET checksum = $1 WHERE version = $2 AND type <> ALL($3)",
        quote_ident(schema),
        quote_ident(table)
    );
    let excluded: Vec<&str> = crate::history::NON_REALIGNABLE_TYPES.to_vec();
    client
        .execute(&sql, &[&new_checksum, &version, &excluded])
        .await?;
    Ok(())
}

/// Update the checksum for a repeatable migration by script (version IS NULL).
///
/// **Not used by `repair`, deliberately.** A repeatable migration is pending
/// exactly when its stored checksum differs from the file, so realigning that
/// value without executing the script marks a modified migration as applied
/// while the database keeps the previous definition. Call this only from a path
/// that has actually run the script.
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

/// Check if the history table has any entries.
pub async fn has_entries(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM {}.{})",
        quote_ident(schema),
        quote_ident(table)
    );
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get::<_, bool>(0))
}
