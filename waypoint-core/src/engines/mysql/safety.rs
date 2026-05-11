//! MySQL safety analysis. Uses worst-case lock mapping (ALGORITHM=COPY
//! semantics for ALTER TABLE) and `information_schema.tables.table_rows`
//! for table size estimation. Shared types and dispatcher live in
//! [`crate::safety`].

use mysql_async::prelude::*;

use crate::db::DbClient;
use crate::error::Result;
use crate::safety::{
    affected_table, classify_row_count, compute_verdict, is_data_loss, LockLevel, SafetyConfig,
    SafetyReport, SafetyVerdict, StatementAnalysis, TableSize,
};
use crate::sql_parser::DdlOperation;

/// Determine the approximate MySQL lock level required by a DDL operation.
///
/// Conservative mapping: MySQL 8.0+ supports `ALGORITHM=INSTANT` for many
/// ALTER TABLE operations, but the planner can fall back to
/// `ALGORITHM=COPY` (full table rewrite, blocking) when INSTANT/INPLACE
/// isn't applicable, so we map ALTER TABLE operations to the worst
/// plausible case. Safety verdicts on MySQL are pessimistic by design.
pub fn lock_level_for_ddl(op: &DdlOperation) -> LockLevel {
    match op {
        DdlOperation::CreateTable { .. } => LockLevel::None,
        DdlOperation::CreateView { .. } => LockLevel::None,
        DdlOperation::AlterTableAddColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::AlterTableDropColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::AlterTableAlterColumn { .. } => LockLevel::AccessExclusiveLock,
        // CREATE INDEX uses INPLACE by default on InnoDB. Reads continue;
        // concurrent writes are blocked briefly. Closer to ShareLock than to
        // ShareUpdateExclusiveLock (no PG-equivalent CONCURRENTLY on MySQL).
        DdlOperation::CreateIndex { .. } => LockLevel::ShareLock,
        DdlOperation::DropIndex { .. } => LockLevel::ShareLock,
        DdlOperation::DropTable { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::DropView { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::TruncateTable { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateFunction { .. } => LockLevel::None,
        DdlOperation::DropFunction { .. } => LockLevel::None,
        DdlOperation::CreateEnum { .. } => LockLevel::None,
        DdlOperation::AddConstraint { .. } => LockLevel::ShareLock,
        DdlOperation::DropConstraint { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::Other { .. } => LockLevel::None,
    }
}

/// Estimate a table's row count via MySQL `information_schema.tables.table_rows`.
///
/// `table_rows` is approximate for InnoDB (driven by the engine's row-count
/// estimator). For accuracy you'd run `SELECT COUNT(*)` — that's
/// O(table_size) and we explicitly avoid it because safety analysis must be
/// cheap. An absent or NULL row count is treated as 0 (Small).
pub async fn classify_table_size(
    client: &DbClient,
    schema: &str,
    table: &str,
    large_threshold: i64,
    huge_threshold: i64,
) -> Result<(TableSize, i64)> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    let rows: Option<Option<i64>> = conn
        .exec_first(
            "SELECT table_rows FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?",
            (schema, table),
        )
        .await?;
    let estimated_rows = rows.flatten().unwrap_or(0);
    let size = classify_row_count(estimated_rows, large_threshold, huge_threshold);
    Ok((size, estimated_rows))
}

/// Generate MySQL-specific suggestions for a DDL operation.
fn generate_suggestions(op: &DdlOperation, size: TableSize) -> Vec<String> {
    let mut suggestions = Vec::new();
    match op {
        DdlOperation::AlterTableAddColumn {
            is_not_null: true,
            has_default: false,
            ..
        } if matches!(size, TableSize::Large | TableSize::Huge) => {
            suggestions.push(
                "ADD COLUMN NOT NULL on a large table will fall back to ALGORITHM=COPY \
                 (full table rewrite). Add the column nullable + backfill + ALTER to NOT NULL."
                    .to_string(),
            );
        }
        DdlOperation::AlterTableAlterColumn { .. }
            if matches!(size, TableSize::Large | TableSize::Huge) =>
        {
            suggestions.push(
                "ALTER COLUMN TYPE on large tables uses ALGORITHM=COPY (full rewrite). \
                 Consider add-column + backfill + swap, or pt-online-schema-change."
                    .to_string(),
            );
        }
        DdlOperation::CreateIndex { .. } if matches!(size, TableSize::Large | TableSize::Huge) => {
            suggestions.push(
                "Index creation on a large MySQL table uses ALGORITHM=INPLACE by default \
                 (reads OK, brief metadata lock). For zero downtime consider gh-ost."
                    .to_string(),
            );
        }
        DdlOperation::DropTable { .. } | DdlOperation::AlterTableDropColumn { .. } => {
            suggestions.push("Consider soft-delete pattern for reversibility".to_string());
        }
        DdlOperation::TruncateTable { .. } => {
            suggestions
                .push("TRUNCATE TABLE drops/recreates the table on InnoDB — irreversible".into());
        }
        _ => {}
    }
    suggestions
}

/// Analyse a migration's SQL for safety verdicts (MySQL).
pub async fn analyze_migration(
    client: &DbClient,
    schema: &str,
    sql: &str,
    script: &str,
    config: &SafetyConfig,
) -> Result<SafetyReport> {
    let ops = crate::sql_parser::extract_ddl_operations(sql);
    let mut statements = Vec::new();
    let mut all_suggestions = Vec::new();
    let mut worst_verdict = SafetyVerdict::Safe;

    for op in &ops {
        let lock = lock_level_for_ddl(op);
        let table = affected_table(op);
        let data_loss = is_data_loss(op);

        let (table_size, estimated_rows) = if let Some(ref t) = table {
            match classify_table_size(
                client,
                schema,
                t,
                config.large_table_threshold,
                config.huge_table_threshold,
            )
            .await
            {
                Ok((size, rows)) => (Some(size), Some(rows)),
                Err(_) => (Some(TableSize::Small), None),
            }
        } else {
            (None, None)
        };

        let size_for_verdict = table_size.unwrap_or(TableSize::Small);
        let verdict = compute_verdict(lock, size_for_verdict, data_loss);
        let suggestions = generate_suggestions(op, size_for_verdict);
        all_suggestions.extend(suggestions.clone());

        if verdict == SafetyVerdict::Danger
            || (verdict == SafetyVerdict::Caution && worst_verdict == SafetyVerdict::Safe)
        {
            worst_verdict = verdict;
        }

        let preview: String = op.to_string().chars().take(120).collect();
        statements.push(StatementAnalysis {
            statement_preview: preview,
            lock_level: lock,
            affected_table: table,
            table_size,
            estimated_rows,
            verdict,
            suggestions,
            data_loss,
        });
    }

    all_suggestions.sort();
    all_suggestions.dedup();

    Ok(SafetyReport {
        script: script.to_string(),
        overall_verdict: worst_verdict,
        statements,
        suggestions: all_suggestions,
    })
}
