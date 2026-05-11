//! PostgreSQL safety analysis: lock-level mapping, table size lookup,
//! verdict generation. Shared types and dispatcher live in [`crate::safety`].

use tokio_postgres::Client;

use crate::error::{Result, WaypointError};
use crate::safety::{
    affected_table, classify_row_count, compute_verdict, is_data_loss, LockLevel, SafetyConfig,
    SafetyReport, SafetyVerdict, StatementAnalysis, TableSize,
};
use crate::sql_parser::DdlOperation;

/// Determine the PostgreSQL lock level required by a DDL operation.
pub fn lock_level_for_ddl(op: &DdlOperation) -> LockLevel {
    match op {
        DdlOperation::CreateTable { .. } => LockLevel::None,
        DdlOperation::AlterTableAddColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::AlterTableDropColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::AlterTableAlterColumn { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateIndex { is_concurrent, .. } => {
            if *is_concurrent {
                LockLevel::ShareUpdateExclusiveLock
            } else {
                LockLevel::ShareLock
            }
        }
        DdlOperation::DropTable { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::DropIndex { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateView { .. } => LockLevel::None,
        DdlOperation::DropView { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateFunction { .. } => LockLevel::None,
        DdlOperation::DropFunction { .. } => LockLevel::None,
        DdlOperation::AddConstraint { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::DropConstraint { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::CreateEnum { .. } => LockLevel::None,
        DdlOperation::TruncateTable { .. } => LockLevel::AccessExclusiveLock,
        DdlOperation::Other { .. } => LockLevel::None,
    }
}

/// Classify a table's size by querying PostgreSQL statistics.
///
/// Returns the classification and the estimated row count from
/// `pg_stat_user_tables.n_live_tup`.
pub async fn classify_table_size(
    client: &Client,
    schema: &str,
    table: &str,
    large_threshold: i64,
    huge_threshold: i64,
) -> Result<(TableSize, i64)> {
    let row = client
        .query_opt(
            "SELECT n_live_tup FROM pg_stat_user_tables \
             WHERE schemaname = $1 AND relname = $2",
            &[&schema, &table],
        )
        .await
        .map_err(WaypointError::DatabaseError)?;

    let estimated_rows: i64 = match row {
        Some(r) => r.get::<_, i64>(0),
        None => 0,
    };

    let size = classify_row_count(estimated_rows, large_threshold, huge_threshold);
    Ok((size, estimated_rows))
}

/// Generate actionable suggestions for a DDL operation based on table size.
fn generate_suggestions(op: &DdlOperation, size: TableSize) -> Vec<String> {
    let mut suggestions = Vec::new();

    match op {
        DdlOperation::CreateIndex {
            is_concurrent: false,
            ..
        } if size == TableSize::Large || size == TableSize::Huge => {
            suggestions.push("Use CREATE INDEX CONCURRENTLY".to_string());
        }
        DdlOperation::AlterTableAddColumn {
            is_not_null: true,
            has_default: true,
            ..
        } if size == TableSize::Large || size == TableSize::Huge => {
            suggestions.push("Split into: add nullable column, backfill, set NOT NULL".to_string());
        }
        DdlOperation::AlterTableAlterColumn { .. }
            if size == TableSize::Large || size == TableSize::Huge =>
        {
            suggestions.push("Use add-column + backfill + swap pattern".to_string());
        }
        DdlOperation::DropTable { .. } | DdlOperation::AlterTableDropColumn { .. } => {
            suggestions.push("Consider soft-delete pattern for reversibility".to_string());
        }
        DdlOperation::TruncateTable { .. } => {
            suggestions.push("Consider DELETE with batching for large tables".to_string());
        }
        _ => {}
    }

    suggestions
}

/// Analyze a migration script for safety concerns (PostgreSQL).
///
/// Parses the SQL into individual DDL operations, queries the database
/// for table size statistics, and produces a [`SafetyReport`] with
/// per-statement verdicts and suggestions.
pub async fn analyze_migration(
    client: &Client,
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Lock level mapping tests ──────────────────────────────────────

    #[test]
    fn test_lock_create_table_is_none() {
        let op = DdlOperation::CreateTable {
            table: "users".into(),
            if_not_exists: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_alter_table_add_column() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "email".into(),
            data_type: "text".into(),
            has_default: false,
            is_not_null: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_alter_table_drop_column() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "users".into(),
            column: "email".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_alter_table_alter_column() {
        let op = DdlOperation::AlterTableAlterColumn {
            table: "users".into(),
            column: "name".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_index_concurrent() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: true,
            is_unique: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::ShareUpdateExclusiveLock);
    }

    #[test]
    fn test_lock_create_index_non_concurrent() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::ShareLock);
    }

    #[test]
    fn test_lock_drop_table() {
        let op = DdlOperation::DropTable {
            table: "users".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_drop_index() {
        let op = DdlOperation::DropIndex {
            name: "idx_email".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_view() {
        let op = DdlOperation::CreateView {
            name: "user_stats".into(),
            is_materialized: false,
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_drop_view() {
        let op = DdlOperation::DropView {
            name: "user_stats".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_function() {
        let op = DdlOperation::CreateFunction {
            name: "my_func".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_drop_function() {
        let op = DdlOperation::DropFunction {
            name: "my_func".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_add_constraint() {
        let op = DdlOperation::AddConstraint {
            table: "users".into(),
            constraint_type: "FOREIGN KEY".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_drop_constraint() {
        let op = DdlOperation::DropConstraint {
            table: "users".into(),
            name: "fk_user_org".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_create_enum() {
        let op = DdlOperation::CreateEnum {
            name: "mood".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    #[test]
    fn test_lock_truncate_table() {
        let op = DdlOperation::TruncateTable {
            table: "logs".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::AccessExclusiveLock);
    }

    #[test]
    fn test_lock_other_is_none() {
        let op = DdlOperation::Other {
            statement_preview: "INSERT INTO ...".into(),
        };
        assert_eq!(lock_level_for_ddl(&op), LockLevel::None);
    }

    // ── Suggestion generation tests ───────────────────────────────────

    #[test]
    fn test_suggestion_non_concurrent_index_large() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("CONCURRENTLY"));
    }

    #[test]
    fn test_suggestion_non_concurrent_index_huge() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Huge);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("CONCURRENTLY"));
    }

    #[test]
    fn test_suggestion_non_concurrent_index_small_no_suggestion() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: false,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Small);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_concurrent_index_large_no_suggestion() {
        let op = DdlOperation::CreateIndex {
            name: "idx_email".into(),
            table: "users".into(),
            is_concurrent: true,
            is_unique: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_add_not_null_default_large() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "status".into(),
            data_type: "text".into(),
            has_default: true,
            is_not_null: true,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("nullable column"));
    }

    #[test]
    fn test_suggestion_add_nullable_column_large_no_suggestion() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "bio".into(),
            data_type: "text".into(),
            has_default: false,
            is_not_null: false,
        };
        let suggestions = generate_suggestions(&op, TableSize::Large);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_alter_column_type_huge() {
        let op = DdlOperation::AlterTableAlterColumn {
            table: "users".into(),
            column: "name".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Huge);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("backfill"));
    }

    #[test]
    fn test_suggestion_alter_column_type_small_no_suggestion() {
        let op = DdlOperation::AlterTableAlterColumn {
            table: "users".into(),
            column: "name".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Small);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_suggestion_drop_table() {
        let op = DdlOperation::DropTable {
            table: "users".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Small);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("soft-delete"));
    }

    #[test]
    fn test_suggestion_drop_column() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "users".into(),
            column: "email".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Medium);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("soft-delete"));
    }

    #[test]
    fn test_suggestion_truncate() {
        let op = DdlOperation::TruncateTable {
            table: "logs".into(),
        };
        let suggestions = generate_suggestions(&op, TableSize::Huge);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("DELETE with batching"));
    }
}
