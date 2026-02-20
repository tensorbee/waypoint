use chrono::NaiveDateTime;
use tokio_postgres::Client;

use crate::db::quote_ident;
use crate::error::Result;

/// A row from the schema history table.
#[derive(Debug, Clone)]
pub struct AppliedMigration {
    pub installed_rank: i32,
    pub version: Option<String>,
    pub description: String,
    pub migration_type: String,
    pub script: String,
    pub checksum: Option<i32>,
    pub installed_by: String,
    pub installed_on: NaiveDateTime,
    pub execution_time: i32,
    pub success: bool,
}

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
    success        BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS {idx_name} ON {fq} (success);
CREATE INDEX IF NOT EXISTS {ver_idx_name} ON {fq} (version);
"#,
        fq = fq,
        idx_name = quote_ident(&idx_name),
        ver_idx_name = quote_ident(&ver_idx_name),
    );

    client.batch_execute(&sql).await?;
    Ok(())
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
         installed_by, installed_on, execution_time, success \
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
        });
    }

    Ok(migrations)
}

/// Insert a migration record into the history table with atomic rank assignment.
///
/// Uses a subquery to atomically compute the next installed_rank within the INSERT,
/// eliminating the race between reading the max rank and inserting.
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
