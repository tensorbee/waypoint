//! PostgreSQL advisor rules (A001-A010).
//!
//! The dialect-aware dispatcher and shared types live in [`crate::advisor`].

use tokio_postgres::Client;

use crate::advisor::{AdvisorConfig, AdvisorReport, Advisory, AdvisorySeverity};
use crate::db::quote_ident;
use crate::error::Result;

/// Run all PostgreSQL advisory rules against the database schema.
pub async fn analyze(
    client: &Client,
    schema: &str,
    config: &AdvisorConfig,
) -> Result<AdvisorReport> {
    let mut advisories = Vec::new();

    if !config.disabled_rules.contains(&"A001".to_string()) {
        advisories.extend(check_a001_fk_without_index(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A002".to_string()) {
        advisories.extend(check_a002_unused_indexes(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A003".to_string()) {
        advisories.extend(check_a003_timestamp_without_tz(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A004".to_string()) {
        advisories.extend(check_a004_table_without_pk(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A005".to_string()) {
        advisories.extend(check_a005_nullable_all_nonnull(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A006".to_string()) {
        advisories.extend(check_a006_varchar_without_limit(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A007".to_string()) {
        advisories.extend(check_a007_duplicate_indexes(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A008".to_string()) {
        advisories.extend(check_a008_seq_scan_large_table(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A009".to_string()) {
        advisories.extend(check_a009_large_enum(client, schema).await?);
    }
    if !config.disabled_rules.contains(&"A010".to_string()) {
        advisories.extend(check_a010_orphaned_sequences(client, schema).await?);
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

// ── A001: Foreign key column missing index ──

async fn check_a001_fk_without_index(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            tc.table_name,
            kcu.column_name,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1
            AND NOT EXISTS (
                SELECT 1 FROM pg_indexes pi
                WHERE pi.schemaname = $1
                    AND pi.tablename = tc.table_name
                    AND pi.indexdef LIKE '%' || kcu.column_name || '%'
            )
        ORDER BY tc.table_name, kcu.column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let column: String = r.get(1);
            Advisory {
                rule_id: "A001".to_string(),
                category: "Performance".to_string(),
                severity: AdvisorySeverity::Warning,
                object: format!("{}.{}", table, column),
                explanation: format!(
                    "Foreign key column {}.{} has no index, which can cause slow joins and constraint checks",
                    table, column
                ),
                fix_sql: Some(format!(
                    "CREATE INDEX idx_{}_{} ON {} ({});",
                    table, column,
                    quote_ident(&table),
                    quote_ident(&column)
                )),
            }
        })
        .collect())
}

// ── A002: Unused indexes ──

async fn check_a002_unused_indexes(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            s.indexrelname AS index_name,
            s.relname AS table_name,
            s.idx_scan AS scans
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        WHERE s.schemaname = $1
            AND s.idx_scan = 0
            AND NOT i.indisprimary
            AND NOT i.indisunique
        ORDER BY s.relname, s.indexrelname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let index_name: String = r.get(0);
            let table_name: String = r.get(1);
            Advisory {
                rule_id: "A002".to_string(),
                category: "Performance".to_string(),
                severity: AdvisorySeverity::Suggestion,
                object: index_name.clone(),
                explanation: format!(
                    "Index {} on {} has never been used (0 scans). Consider removing it to reduce write overhead",
                    index_name, table_name
                ),
                fix_sql: Some(format!("DROP INDEX {};", quote_ident(&index_name))),
            }
        })
        .collect())
}

// ── A003: TIMESTAMP without timezone ──

async fn check_a003_timestamp_without_tz(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = $1
            AND data_type = 'timestamp without time zone'
        ORDER BY table_name, column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let column: String = r.get(1);
            Advisory {
                rule_id: "A003".to_string(),
                category: "Correctness".to_string(),
                severity: AdvisorySeverity::Warning,
                object: format!("{}.{}", table, column),
                explanation: format!(
                    "Column {}.{} uses TIMESTAMP WITHOUT TIME ZONE. Use TIMESTAMPTZ to avoid timezone ambiguity",
                    table, column
                ),
                fix_sql: Some(format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE TIMESTAMPTZ;",
                    quote_ident(&table),
                    quote_ident(&column)
                )),
            }
        })
        .collect())
}

// ── A004: Table without primary key ──

async fn check_a004_table_without_pk(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
            AND NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints tc
                WHERE tc.table_schema = $1
                    AND tc.table_name = t.table_name
                    AND tc.constraint_type = 'PRIMARY KEY'
            )
        ORDER BY t.table_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            Advisory {
                rule_id: "A004".to_string(),
                category: "Correctness".to_string(),
                severity: AdvisorySeverity::Warning,
                object: table.clone(),
                explanation: format!(
                    "Table {} has no primary key. This prevents logical replication and makes row identification unreliable",
                    table
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A005: Nullable column where all values are non-null ──

async fn check_a005_nullable_all_nonnull(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN pg_stat_user_tables s
            ON c.table_name = s.relname AND s.schemaname = $1
        WHERE c.table_schema = $1
            AND c.is_nullable = 'YES'
            AND s.n_live_tup > 100
            AND c.column_default IS NULL
        ORDER BY c.table_name, c.column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    let mut advisories = Vec::new();

    for row in &rows {
        let table: String = row.get(0);
        let column: String = row.get(1);

        let null_check = format!(
            "SELECT EXISTS (SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1)",
            quote_ident(&table),
            quote_ident(&column)
        );
        if let Ok(result) = client.query_one(&null_check, &[]).await {
            let has_nulls: bool = result.get(0);
            if !has_nulls {
                advisories.push(Advisory {
                    rule_id: "A005".to_string(),
                    category: "Correctness".to_string(),
                    severity: AdvisorySeverity::Info,
                    object: format!("{}.{}", table, column),
                    explanation: format!(
                        "Column {}.{} is nullable but contains no NULL values. Consider adding NOT NULL constraint",
                        table, column
                    ),
                    fix_sql: Some(format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                        quote_ident(&table),
                        quote_ident(&column)
                    )),
                });
            }
        }
    }

    Ok(advisories)
}

// ── A006: VARCHAR without length limit ──

async fn check_a006_varchar_without_limit(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = $1
            AND data_type = 'character varying'
            AND character_maximum_length IS NULL
        ORDER BY table_name, column_name
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let column: String = r.get(1);
            Advisory {
                rule_id: "A006".to_string(),
                category: "Design".to_string(),
                severity: AdvisorySeverity::Info,
                object: format!("{}.{}", table, column),
                explanation: format!(
                    "Column {}.{} is VARCHAR without length limit. Consider using TEXT or adding a length constraint",
                    table, column
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A007: Duplicate indexes ──

async fn check_a007_duplicate_indexes(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            a.indexname AS index_a,
            b.indexname AS index_b,
            a.tablename
        FROM pg_indexes a
        JOIN pg_indexes b
            ON a.tablename = b.tablename
            AND a.schemaname = b.schemaname
            AND a.indexname < b.indexname
            AND a.indexdef = b.indexdef
        WHERE a.schemaname = $1
        ORDER BY a.tablename, a.indexname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let index_a: String = r.get(0);
            let index_b: String = r.get(1);
            let table: String = r.get(2);
            Advisory {
                rule_id: "A007".to_string(),
                category: "Design".to_string(),
                severity: AdvisorySeverity::Warning,
                object: format!("{}, {}", index_a, index_b),
                explanation: format!(
                    "Indexes {} and {} on table {} have identical definitions. Remove the duplicate",
                    index_a, index_b, table
                ),
                fix_sql: Some(format!("DROP INDEX {};", quote_ident(&index_b))),
            }
        })
        .collect())
}

// ── A008: Sequential scan on large table ──

async fn check_a008_seq_scan_large_table(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT
            relname,
            seq_scan,
            n_live_tup
        FROM pg_stat_user_tables
        WHERE schemaname = $1
            AND n_live_tup > 100000
            AND seq_scan > 0
            AND seq_scan > idx_scan
        ORDER BY seq_scan DESC
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let table: String = r.get(0);
            let seq_scans: i64 = r.get(1);
            let row_count: i64 = r.get(2);
            Advisory {
                rule_id: "A008".to_string(),
                category: "Performance".to_string(),
                severity: AdvisorySeverity::Warning,
                object: table.clone(),
                explanation: format!(
                    "Table {} (~{} rows) has {} sequential scans exceeding index scans. Consider adding indexes",
                    table, row_count, seq_scans
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A009: Enum with >20 values ──

async fn check_a009_large_enum(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT t.typname, count(e.enumlabel)::int AS label_count
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = $1
        GROUP BY t.typname
        HAVING count(e.enumlabel) > 20
        ORDER BY t.typname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            let count: i32 = r.get(1);
            Advisory {
                rule_id: "A009".to_string(),
                category: "Design".to_string(),
                severity: AdvisorySeverity::Suggestion,
                object: name.clone(),
                explanation: format!(
                    "Enum type {} has {} values. Enums with many values are hard to maintain; consider a lookup table",
                    name, count
                ),
                fix_sql: None,
            }
        })
        .collect())
}

// ── A010: Orphaned sequences ──

async fn check_a010_orphaned_sequences(client: &Client, schema: &str) -> Result<Vec<Advisory>> {
    let sql = r#"
        SELECT s.relname
        FROM pg_class s
        JOIN pg_namespace n ON n.oid = s.relnamespace
        WHERE s.relkind = 'S'
            AND n.nspname = $1
            AND NOT EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = s.oid
                    AND d.deptype IN ('a', 'i')
            )
        ORDER BY s.relname
    "#;

    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            Advisory {
                rule_id: "A010".to_string(),
                category: "Correctness".to_string(),
                severity: AdvisorySeverity::Suggestion,
                object: name.clone(),
                explanation: format!(
                    "Sequence {} is not attached to any column. It may be orphaned",
                    name
                ),
                fix_sql: Some(format!("DROP SEQUENCE IF EXISTS {};", quote_ident(&name))),
            }
        })
        .collect())
}
