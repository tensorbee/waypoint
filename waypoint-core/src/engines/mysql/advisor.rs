//! MySQL advisor rules (M001-M005).
//!
//! The dialect-aware dispatcher and shared types live in [`crate::advisor`].

use mysql_async::prelude::*;

use crate::advisor::{AdvisorConfig, AdvisorReport, Advisory, AdvisorySeverity};
use crate::db::DbClient;
use crate::error::Result;

/// Run all MySQL advisory rules against a schema.
///
/// Current rule set:
/// - M001: foreign key column without an index (analog of A001)
/// - M002: table without a primary key (analog of A004)
/// - M003: non-utf8mb4 charset in use (MySQL-specific)
/// - M004: non-InnoDB storage engine (MyISAM etc.) — risky for transactions
/// - M005: duplicate indexes on the same columns (analog of A007)
pub async fn analyze(
    client: &DbClient,
    schema: &str,
    config: &AdvisorConfig,
) -> Result<AdvisorReport> {
    let mut advisories = Vec::new();
    if !config.disabled_rules.contains(&"M001".to_string()) {
        advisories.extend(check_m001_fk_without_index(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M002".to_string()) {
        advisories.extend(check_m002_table_without_pk(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M003".to_string()) {
        advisories.extend(check_m003_non_utf8mb4_charset(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M004".to_string()) {
        advisories.extend(check_m004_non_innodb_engine(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"M005".to_string()) {
        advisories.extend(check_m005_duplicate_indexes(client, schema).await?);
    }

    let warning_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Warning)
        .count();
    let suggestion_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Suggestion)
        .count();
    let info_count = advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Info)
        .count();

    Ok(AdvisorReport {
        schema: schema.to_string(),
        advisories,
        warning_count,
        suggestion_count,
        info_count,
    })
}

// ── M001: Foreign key column missing index ──
async fn check_m001_fk_without_index(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    // Foreign keys whose first column has no covering index. We approximate
    // by joining KEY_COLUMN_USAGE (FK columns) against STATISTICS (indexed
    // columns), filtering FKs whose column doesn't appear as the FIRST column
    // of any index. MySQL automatically creates an index on FK columns at FK
    // creation time — but a later DROP INDEX can leave the FK without one.
    let rows: Vec<(String, String, String)> = conn
        .exec(
            "SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.CONSTRAINT_NAME \
             FROM information_schema.KEY_COLUMN_USAGE kcu \
             WHERE kcu.TABLE_SCHEMA = ? \
               AND kcu.REFERENCED_TABLE_NAME IS NOT NULL \
               AND NOT EXISTS ( \
                 SELECT 1 FROM information_schema.STATISTICS s \
                 WHERE s.TABLE_SCHEMA = kcu.TABLE_SCHEMA \
                   AND s.TABLE_NAME = kcu.TABLE_NAME \
                   AND s.COLUMN_NAME = kcu.COLUMN_NAME \
                   AND s.SEQ_IN_INDEX = 1 \
               ) \
             ORDER BY kcu.TABLE_NAME, kcu.COLUMN_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, column, _constraint)| Advisory {
            rule_id: "M001".to_string(),
            category: "Performance".to_string(),
            severity: AdvisorySeverity::Warning,
            object: format!("{}.{}", table, column),
            explanation: format!(
                "Foreign key column {}.{} has no covering index — joins and FK \
                 constraint checks will perform a full table scan",
                table, column
            ),
            fix_sql: Some(format!(
                "CREATE INDEX `idx_{table}_{column}` ON `{table}` (`{column}`);"
            )),
        })
        .collect())
}

// ── M002: Table without primary key ──
async fn check_m002_table_without_pk(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    let rows: Vec<String> = conn
        .exec(
            "SELECT t.TABLE_NAME FROM information_schema.TABLES t \
             WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM information_schema.TABLE_CONSTRAINTS tc \
                 WHERE tc.TABLE_SCHEMA = t.TABLE_SCHEMA \
                   AND tc.TABLE_NAME = t.TABLE_NAME \
                   AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
               ) \
             ORDER BY t.TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|table| Advisory {
            rule_id: "M002".to_string(),
            category: "Correctness".to_string(),
            severity: AdvisorySeverity::Warning,
            object: table.clone(),
            explanation: format!(
                "Table {} has no primary key — InnoDB will create a hidden \
                 6-byte rowid index, replication and crash recovery suffer",
                table
            ),
            fix_sql: None,
        })
        .collect())
}

// ── M003: Non-utf8mb4 charset ──
async fn check_m003_non_utf8mb4_charset(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    // Tables whose default charset isn't utf8mb4. utf8 (3-byte) is a frequent
    // legacy footgun that can't store 4-byte characters (emoji, some Asian
    // scripts) and may surface as silent data corruption.
    let rows: Vec<(String, String)> = conn
        .exec(
            "SELECT t.TABLE_NAME, ccsa.CHARACTER_SET_NAME \
             FROM information_schema.TABLES t \
             JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa \
               ON ccsa.COLLATION_NAME = t.TABLE_COLLATION \
             WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' \
               AND ccsa.CHARACTER_SET_NAME <> 'utf8mb4' \
             ORDER BY t.TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, charset)| Advisory {
            rule_id: "M003".to_string(),
            category: "Correctness".to_string(),
            severity: AdvisorySeverity::Suggestion,
            object: table.clone(),
            explanation: format!(
                "Table {} uses charset '{}' — utf8mb4 is the modern default \
                 and supports the full Unicode range (4-byte chars)",
                table, charset
            ),
            fix_sql: Some(format!(
                "ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 \
                 COLLATE utf8mb4_0900_ai_ci;"
            )),
        })
        .collect())
}

// ── M004: Non-InnoDB storage engine ──
async fn check_m004_non_innodb_engine(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
               AND ENGINE IS NOT NULL AND ENGINE <> 'InnoDB' \
             ORDER BY TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, engine)| Advisory {
            rule_id: "M004".to_string(),
            category: "Correctness".to_string(),
            severity: AdvisorySeverity::Warning,
            object: table.clone(),
            explanation: format!(
                "Table {} uses storage engine '{}' — InnoDB is the modern \
                 default and the only engine with full transaction + crash-\
                 recovery support",
                table, engine
            ),
            fix_sql: Some(format!("ALTER TABLE `{table}` ENGINE = InnoDB;")),
        })
        .collect())
}

// ── M005: Duplicate indexes ──
async fn check_m005_duplicate_indexes(client: &DbClient, schema: &str) -> Result<Vec<Advisory>> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    // Indexes that index exactly the same column sequence on the same table.
    // We group by (table, column-sequence) and emit an advisory when more
    // than one index name appears in a group. Concatenating column names with
    // a delimiter is a coarse fingerprint; for the same physical leaf it's
    // sufficient.
    let rows: Vec<(String, String, i64)> = conn
        .exec(
            "SELECT TABLE_NAME, GROUP_CONCAT(INDEX_NAME ORDER BY INDEX_NAME), COUNT(*) \
             FROM ( \
                 SELECT TABLE_NAME, INDEX_NAME, \
                        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = ? \
                 GROUP BY TABLE_NAME, INDEX_NAME \
             ) g \
             GROUP BY TABLE_NAME, cols \
             HAVING COUNT(*) > 1 \
             ORDER BY TABLE_NAME",
            (schema,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(table, names, count)| Advisory {
            rule_id: "M005".to_string(),
            category: "Performance".to_string(),
            severity: AdvisorySeverity::Suggestion,
            object: format!("{}: {}", table, names),
            explanation: format!(
                "Table {} has {} indexes ({}) covering the same columns — \
                 drop the redundant ones to reduce write amplification and \
                 storage",
                table, count, names
            ),
            fix_sql: None,
        })
        .collect())
}
