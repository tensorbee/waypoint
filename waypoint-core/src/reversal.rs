//! Auto-reversal generation from schema diffs.
//!
//! Captures before/after schema snapshots around migration execution
//! and generates reverse DDL that can undo the migration without
//! requiring manual `U{version}__*.sql` files.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::db::DbClient;
#[cfg(feature = "postgres")]
use crate::db::quote_ident;
use crate::dialect::DialectKind;
use crate::error::Result;
use crate::schema::{self, SchemaDiff, SchemaSnapshot};

/// Make generated PostgreSQL reversal SQL target the configured schema.
///
/// `schema::generate_ddl` emits object names unqualified (`CREATE TABLE
/// "orders"`) while embedding qualified references it read from the catalog
/// (`DEFAULT nextval('revchk.orders_id_seq'::regclass)`). Neither `migrate` nor
/// `undo` sets `search_path` — hand-written migrations are expected to qualify
/// their own names — so an auto-reversal for a schema other than the one on the
/// connection's search path recreated its objects in the *wrong schema*, and
/// then failed on the first qualified reference that did not resolve.
///
/// Prepending the setting makes the stored reversal self-contained: it is
/// correct wherever it is later executed, without changing `search_path`
/// semantics for user-written SQL. `SET LOCAL` because `undo` runs this inside
/// a transaction, so it reverts on COMMIT or ROLLBACK rather than leaking into
/// the session.
#[cfg(feature = "postgres")]
fn scope_reversal_to_schema(schema: &str, sql: &str) -> String {
    format!(
        "SET LOCAL search_path TO {};\n\n{}",
        quote_ident(schema),
        sql
    )
}

/// Configuration for auto-reversal generation.
#[derive(Debug, Clone)]
pub struct ReversalConfig {
    /// Whether auto-reversal generation is enabled.
    pub enabled: bool,
    /// Whether to emit warnings for data-loss operations in reversal SQL.
    pub warn_data_loss: bool,
}

impl Default for ReversalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            warn_data_loss: true,
        }
    }
}

/// Result of generating a reversal for a migration.
#[derive(Debug, Clone, Serialize)]
pub struct ReversalResult {
    /// The generated reverse SQL, or None if reversal is not possible.
    pub reversal_sql: Option<String>,
    /// Whether the reversal involves data loss (DROP TABLE, DROP COLUMN, etc.).
    pub has_data_loss: bool,
    /// Warnings about the generated reversal.
    pub warnings: Vec<String>,
}

/// Capture a schema snapshot before migration execution (PostgreSQL legacy).
#[cfg(feature = "postgres")]
pub async fn capture_before(client: &Client, schema: &str) -> Result<SchemaSnapshot> {
    schema::introspect(client, schema).await
}

/// Capture a schema snapshot before migration execution (dialect-aware).
pub async fn capture_before_db(client: &DbClient, schema: &str) -> Result<SchemaSnapshot> {
    schema::introspect_db(client, schema).await
}

/// Capture a schema snapshot after migration execution and generate reverse
/// DDL (PostgreSQL legacy).
#[cfg(feature = "postgres")]
pub async fn generate_reversal(
    client: &Client,
    schema_name: &str,
    before: &SchemaSnapshot,
    warn_data_loss: bool,
) -> Result<ReversalResult> {
    let after = schema::introspect(client, schema_name).await?;

    // Compute the reverse diff: diff(after, before) gives us what we need to
    // "undo" the migration (go from after back to before)
    let reverse_diffs = schema::diff(&after, before);

    if reverse_diffs.is_empty() {
        return Ok(ReversalResult {
            reversal_sql: None,
            has_data_loss: false,
            warnings: vec!["No schema changes detected; no reversal generated.".to_string()],
        });
    }

    let mut has_data_loss = false;
    let mut warnings = Vec::new();

    // Check for data-loss operations in the reverse diff
    for d in &reverse_diffs {
        match d {
            SchemaDiff::TableDropped(name) => {
                has_data_loss = true;
                if warn_data_loss {
                    warnings.push(format!(
                        "DATA_LOSS: DROP TABLE {} — original data cannot be restored",
                        name
                    ));
                }
            }
            SchemaDiff::ColumnDropped { table, column } => {
                has_data_loss = true;
                if warn_data_loss {
                    warnings.push(format!(
                        "DATA_LOSS: DROP COLUMN {}.{} — original data cannot be restored",
                        table, column
                    ));
                }
            }
            _ => {}
        }
    }

    // Generate DDL from the reverse diff
    let mut sql = scope_reversal_to_schema(schema_name, &schema::generate_ddl(&reverse_diffs));

    // Prepend data-loss warnings as SQL comments
    if has_data_loss && warn_data_loss {
        let warning_comments: Vec<String> = warnings
            .iter()
            .map(|w| format!("-- WARNING: {}", w))
            .collect();
        sql = format!("{}\n\n{}", warning_comments.join("\n"), sql);
    }

    Ok(ReversalResult {
        reversal_sql: Some(sql),
        has_data_loss,
        warnings,
    })
}

/// Store reversal SQL in the history table for a specific version (PG legacy).
#[cfg(feature = "postgres")]
pub async fn store_reversal(
    client: &Client,
    schema: &str,
    table: &str,
    version: &str,
    reversal_sql: &str,
) -> Result<()> {
    let sql = format!(
        "UPDATE {}.{} SET reversal_sql = $1 WHERE version = $2 AND success = TRUE \
         AND installed_rank = (SELECT MAX(installed_rank) FROM {}.{} WHERE version = $2 AND success = TRUE)",
        quote_ident(schema),
        quote_ident(table),
        quote_ident(schema),
        quote_ident(table),
    );
    client.execute(&sql, &[&reversal_sql, &version]).await?;
    Ok(())
}

/// Retrieve stored reversal SQL for a specific version (PG legacy).
#[cfg(feature = "postgres")]
pub async fn get_reversal(
    client: &Client,
    schema: &str,
    table: &str,
    version: &str,
) -> Result<Option<String>> {
    let sql = format!(
        "SELECT reversal_sql FROM {}.{} WHERE version = $1 AND success = TRUE \
         ORDER BY installed_rank DESC LIMIT 1",
        quote_ident(schema),
        quote_ident(table),
    );
    let rows = client.query(&sql, &[&version]).await?;
    if let Some(row) = rows.first() {
        Ok(row.get::<_, Option<String>>(0))
    } else {
        Ok(None)
    }
}

// ── Dialect-aware reversal generation/storage/retrieval ──────────────────────

/// Capture an after-snapshot, diff against `before`, and produce a reversal
/// SQL string suitable for storing in `waypoint_schema_history.reversal_sql`.
/// Dispatches to the right DDL generator (PG vs MySQL) based on the connection.
pub async fn generate_reversal_db(
    client: &DbClient,
    schema_name: &str,
    before: &SchemaSnapshot,
    warn_data_loss: bool,
) -> Result<ReversalResult> {
    let after = schema::introspect_db(client, schema_name).await?;
    let reverse_diffs = schema::diff(&after, before);

    if reverse_diffs.is_empty() {
        return Ok(ReversalResult {
            reversal_sql: None,
            has_data_loss: false,
            warnings: vec!["No schema changes detected; no reversal generated.".to_string()],
        });
    }

    let mut has_data_loss = false;
    let mut warnings = Vec::new();
    for d in &reverse_diffs {
        match d {
            SchemaDiff::TableDropped(name) => {
                has_data_loss = true;
                if warn_data_loss {
                    warnings.push(format!(
                        "DATA_LOSS: DROP TABLE {} — original data cannot be restored",
                        name
                    ));
                }
            }
            SchemaDiff::ColumnDropped { table, column } => {
                has_data_loss = true;
                if warn_data_loss {
                    warnings.push(format!(
                        "DATA_LOSS: DROP COLUMN {}.{} — original data cannot be restored",
                        table, column
                    ));
                }
            }
            _ => {}
        }
    }

    // Emit DDL in the dialect of the connection.
    let mut sql = match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            scope_reversal_to_schema(schema_name, &schema::generate_ddl(&reverse_diffs))
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => schema::generate_ddl(&reverse_diffs),
        // MySQL has no search_path; unqualified names resolve against the
        // connection's default database, which `undo` already targets.
        DialectKind::Mysql => schema::generate_ddl_mysql(&reverse_diffs),
    };

    if has_data_loss && warn_data_loss {
        let warning_comments: Vec<String> = warnings
            .iter()
            .map(|w| format!("-- WARNING: {}", w))
            .collect();
        sql = format!("{}\n\n{}", warning_comments.join("\n"), sql);
    }

    Ok(ReversalResult {
        reversal_sql: Some(sql),
        has_data_loss,
        warnings,
    })
}

/// Store reversal SQL in the history table (dialect-aware).
pub async fn store_reversal_db(
    client: &DbClient,
    schema: &str,
    table: &str,
    version: &str,
    reversal_sql: &str,
) -> Result<()> {
    let dialect = client.dialect();
    let fq = dialect.qualified_table(schema, table);
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            let sql = format!(
                "UPDATE {fq} SET reversal_sql = $1 WHERE version = $2 AND success = TRUE \
                 AND installed_rank = (SELECT MAX(installed_rank) FROM {fq} \
                 WHERE version = $2 AND success = TRUE)"
            );
            c.execute(&sql, &[&reversal_sql, &version]).await?;
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let mut conn = pool.get_conn().await?;
            // MySQL doesn't allow self-referencing the target table in a
            // subquery during UPDATE, so we read the max rank first.
            // SELECT MAX over a filtered set returns a row with NULL when no
            // rows match, so we type as Option<Option<i32>> (outer = row exists,
            // inner = MAX value or NULL) and flatten.
            let max_rank: Option<Option<i32>> = conn
                .exec_first(
                    format!(
                        "SELECT MAX(installed_rank) FROM {fq} \
                         WHERE version = ? AND success = TRUE"
                    ),
                    (version,),
                )
                .await?;
            if let Some(rank) = max_rank.flatten() {
                conn.exec_drop(
                    format!("UPDATE {fq} SET reversal_sql = ? WHERE installed_rank = ?"),
                    (reversal_sql, rank),
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Retrieve stored reversal SQL for a specific version (dialect-aware).
pub async fn get_reversal_db(
    client: &DbClient,
    schema: &str,
    table: &str,
    version: &str,
) -> Result<Option<String>> {
    let dialect = client.dialect();
    let fq = dialect.qualified_table(schema, table);
    match client {
        #[cfg(feature = "postgres")]
        DbClient::Postgres(c) => {
            let sql = format!(
                "SELECT reversal_sql FROM {fq} WHERE version = $1 AND success = TRUE \
                 ORDER BY installed_rank DESC LIMIT 1"
            );
            let rows = c.query(&sql, &[&version]).await?;
            Ok(rows.first().and_then(|r| r.get::<_, Option<String>>(0)))
        }
        #[cfg(feature = "mysql")]
        DbClient::Mysql(pool) => {
            use mysql_async::prelude::*;
            let mut conn = pool.get_conn().await?;
            let row: Option<Option<String>> = conn
                .exec_first(
                    format!(
                        "SELECT reversal_sql FROM {fq} WHERE version = ? AND success = TRUE \
                         ORDER BY installed_rank DESC LIMIT 1"
                    ),
                    (version,),
                )
                .await?;
            Ok(row.flatten())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn empty_snapshot() -> SchemaSnapshot {
        SchemaSnapshot {
            tables: vec![],
            views: vec![],
            indexes: vec![],
            sequences: vec![],
            functions: vec![],
            enums: vec![],
            constraints: vec![],
            triggers: vec![],
            extensions: vec![],
        }
    }

    #[test]
    fn test_reverse_diff_create_table_generates_drop() {
        let before = empty_snapshot();
        let mut after = empty_snapshot();
        after.tables.push(TableDef {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_nullable: false,
                default: None,
                ordinal_position: 1,
            }],
        });

        // Reverse diff: from after back to before → should generate DROP TABLE
        let diffs = schema::diff(&after, &before);
        assert_eq!(diffs.len(), 1);
        match &diffs[0] {
            SchemaDiff::TableDropped(name) => assert_eq!(name, "users"),
            other => panic!("Expected TableDropped, got {:?}", other),
        }
    }

    #[test]
    fn test_reverse_diff_add_column_generates_drop_column() {
        let mut before = empty_snapshot();
        before.tables.push(TableDef {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_nullable: false,
                default: None,
                ordinal_position: 1,
            }],
        });

        let mut after = empty_snapshot();
        after.tables.push(TableDef {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    is_nullable: false,
                    default: None,
                    ordinal_position: 1,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: "character varying".to_string(),
                    is_nullable: true,
                    default: None,
                    ordinal_position: 2,
                },
            ],
        });

        let diffs = schema::diff(&after, &before);
        assert_eq!(diffs.len(), 1);
        match &diffs[0] {
            SchemaDiff::ColumnDropped { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            other => panic!("Expected ColumnDropped, got {:?}", other),
        }
    }

    #[test]
    fn test_reverse_diff_drop_table_generates_create() {
        let mut before = empty_snapshot();
        before.tables.push(TableDef {
            schema: "public".to_string(),
            name: "old_table".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_nullable: false,
                default: None,
                ordinal_position: 1,
            }],
        });

        let after = empty_snapshot();

        let diffs = schema::diff(&after, &before);
        assert_eq!(diffs.len(), 1);
        match &diffs[0] {
            SchemaDiff::TableAdded(t) => {
                assert_eq!(t.name, "old_table");
                assert_eq!(t.columns.len(), 1);
            }
            other => panic!("Expected TableAdded, got {:?}", other),
        }
    }

    #[test]
    fn test_reverse_ddl_generation() {
        let before = empty_snapshot();
        let mut after = empty_snapshot();
        after.tables.push(TableDef {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                is_nullable: false,
                default: None,
                ordinal_position: 1,
            }],
        });

        let diffs = schema::diff(&after, &before);
        let ddl = schema::generate_ddl(&diffs);
        assert!(ddl.contains("DROP TABLE"));
        assert!(ddl.contains("users"));
    }

    #[test]
    fn test_no_changes_empty_diff() {
        let snapshot = empty_snapshot();
        let diffs = schema::diff(&snapshot, &snapshot);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_reversal_config_default() {
        let config = ReversalConfig::default();
        assert!(config.enabled);
        assert!(config.warn_data_loss);
    }
}
