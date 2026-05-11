//! Migration safety analysis: lock levels, impact estimation, and verdicts.
//!
//! This module owns the engine-agnostic types ([`LockLevel`], [`SafetyReport`],
//! [`SafetyConfig`], etc.), the dialect-aware dispatcher
//! ([`analyze_migration_db`]), and a handful of shared helpers used by both
//! engine paths. The actual per-engine analysers live in
//! [`crate::engines::postgres::safety`] and [`crate::engines::mysql::safety`].

use serde::Serialize;

use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::Result;
use crate::sql_parser::DdlOperation;

// ── Re-exports of the engine-specific entry points ──────────────────────────

#[cfg(feature = "mysql")]
pub use crate::engines::mysql::safety::{
    analyze_migration as analyze_migration_mysql, classify_table_size as classify_table_size_mysql,
    lock_level_for_ddl as lock_level_for_ddl_mysql,
};
#[cfg(feature = "postgres")]
pub use crate::engines::postgres::safety::{
    analyze_migration, classify_table_size, lock_level_for_ddl,
};

// ── Shared types ────────────────────────────────────────────────────────────

/// PostgreSQL lock levels, ordered from least to most restrictive.
///
/// The ordering matches PostgreSQL's internal lock hierarchy so that
/// comparisons (e.g. `lock > LockLevel::ShareLock`) work correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum LockLevel {
    /// No lock acquired (new objects, functions, enums).
    None,
    /// ACCESS SHARE — acquired by SELECT.
    AccessShareLock,
    /// ROW SHARE — acquired by SELECT FOR UPDATE/SHARE.
    RowShareLock,
    /// ROW EXCLUSIVE — acquired by INSERT/UPDATE/DELETE.
    RowExclusiveLock,
    /// SHARE UPDATE EXCLUSIVE — acquired by VACUUM, CREATE INDEX CONCURRENTLY.
    ShareUpdateExclusiveLock,
    /// SHARE — acquired by CREATE INDEX (non-concurrent).
    ShareLock,
    /// SHARE ROW EXCLUSIVE — acquired by some constraint triggers.
    ShareRowExclusiveLock,
    /// EXCLUSIVE — blocks all reads/writes except ACCESS SHARE.
    ExclusiveLock,
    /// ACCESS EXCLUSIVE — the strongest lock; blocks everything.
    AccessExclusiveLock,
}

impl std::fmt::Display for LockLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockLevel::None => write!(f, "None"),
            LockLevel::AccessShareLock => write!(f, "ACCESS SHARE"),
            LockLevel::RowShareLock => write!(f, "ROW SHARE"),
            LockLevel::RowExclusiveLock => write!(f, "ROW EXCLUSIVE"),
            LockLevel::ShareUpdateExclusiveLock => write!(f, "SHARE UPDATE EXCLUSIVE"),
            LockLevel::ShareLock => write!(f, "SHARE"),
            LockLevel::ShareRowExclusiveLock => write!(f, "SHARE ROW EXCLUSIVE"),
            LockLevel::ExclusiveLock => write!(f, "EXCLUSIVE"),
            LockLevel::AccessExclusiveLock => write!(f, "ACCESS EXCLUSIVE"),
        }
    }
}

/// Rough classification of table size based on estimated row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TableSize {
    /// Fewer than 10,000 rows.
    Small,
    /// 10,000 to 1,000,000 rows.
    Medium,
    /// 1,000,000 to 100,000,000 rows.
    Large,
    /// More than 100,000,000 rows.
    Huge,
}

impl std::fmt::Display for TableSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableSize::Small => write!(f, "Small (<10k rows)"),
            TableSize::Medium => write!(f, "Medium (10k-1M rows)"),
            TableSize::Large => write!(f, "Large (1M-100M rows)"),
            TableSize::Huge => write!(f, "Huge (>100M rows)"),
        }
    }
}

/// Overall safety verdict for a migration statement or script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum SafetyVerdict {
    /// No significant risk detected.
    Safe,
    /// Moderate risk — review recommended.
    Caution,
    /// High risk — may cause downtime or data loss.
    Danger,
}

impl std::fmt::Display for SafetyVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SafetyVerdict::Safe => write!(f, "SAFE"),
            SafetyVerdict::Caution => write!(f, "CAUTION"),
            SafetyVerdict::Danger => write!(f, "DANGER"),
        }
    }
}

/// Safety analysis for a single SQL statement within a migration.
#[derive(Debug, Clone, Serialize)]
pub struct StatementAnalysis {
    /// A short preview of the analyzed statement.
    pub statement_preview: String,
    /// The lock level this statement acquires.
    pub lock_level: LockLevel,
    /// The table affected by this statement, if identifiable.
    pub affected_table: Option<String>,
    /// Estimated table size classification, if known.
    pub table_size: Option<TableSize>,
    /// Estimated live row count, if available from statistics.
    pub estimated_rows: Option<i64>,
    /// The safety verdict for this statement.
    pub verdict: SafetyVerdict,
    /// Actionable suggestions for reducing risk.
    pub suggestions: Vec<String>,
    /// Whether this statement causes irreversible data loss.
    pub data_loss: bool,
}

/// Full safety report for a migration script.
#[derive(Debug, Clone, Serialize)]
pub struct SafetyReport {
    /// The migration script filename or identifier.
    pub script: String,
    /// The worst-case verdict across all statements.
    pub overall_verdict: SafetyVerdict,
    /// Per-statement analysis results.
    pub statements: Vec<StatementAnalysis>,
    /// Aggregated suggestions across all statements.
    pub suggestions: Vec<String>,
}

/// Configuration for safety analysis.
#[derive(Debug, Clone)]
pub struct SafetyConfig {
    /// Whether safety analysis is enabled.
    pub enabled: bool,
    /// Whether to block migrations that receive a DANGER verdict.
    pub block_on_danger: bool,
    /// Row count threshold for classifying a table as Large.
    pub large_table_threshold: i64,
    /// Row count threshold for classifying a table as Huge.
    pub huge_table_threshold: i64,
    /// MySQL only: run `ANALYZE TABLE <name>` on each affected table before
    /// reading `information_schema.tables.table_rows` for size classification.
    /// Off by default because `ANALYZE TABLE` acquires a brief metadata lock
    /// and rewrites stats. Enable when you need accurate size classification
    /// at the cost of touching the table — typically during a CI safety check
    /// rather than at production-migrate time.
    pub refresh_stats_mysql: bool,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            block_on_danger: false,
            large_table_threshold: 1_000_000,
            huge_table_threshold: 100_000_000,
            refresh_stats_mysql: false,
        }
    }
}

// ── Dispatcher ──────────────────────────────────────────────────────────────

/// Analyse a migration's SQL for safety verdicts (dialect-aware entry).
pub async fn analyze_migration_db(
    client: &DbClient,
    schema: &str,
    sql: &str,
    script: &str,
    config: &SafetyConfig,
) -> Result<SafetyReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            analyze_migration(client.as_postgres()?, schema, sql, script, config).await
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(crate::error::WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => analyze_migration_mysql(client, schema, sql, script, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(crate::error::WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}

// ── Shared helpers (used by both engine paths) ──────────────────────────────

/// Classify a row count into a [`TableSize`] using the given thresholds.
pub(crate) fn classify_row_count(
    rows: i64,
    large_threshold: i64,
    huge_threshold: i64,
) -> TableSize {
    if rows > huge_threshold {
        TableSize::Huge
    } else if rows > large_threshold {
        TableSize::Large
    } else if rows >= 10_000 {
        TableSize::Medium
    } else {
        TableSize::Small
    }
}

/// Determine the safety verdict for a statement given its lock level,
/// affected table size, and whether it causes data loss.
pub(crate) fn compute_verdict(lock: LockLevel, size: TableSize, data_loss: bool) -> SafetyVerdict {
    if lock == LockLevel::AccessExclusiveLock
        && (size == TableSize::Large || size == TableSize::Huge)
    {
        return SafetyVerdict::Danger;
    }

    if data_loss && (size == TableSize::Large || size == TableSize::Huge) {
        return SafetyVerdict::Danger;
    }

    if lock == LockLevel::AccessExclusiveLock {
        return SafetyVerdict::Caution;
    }

    if lock == LockLevel::ShareLock && (size == TableSize::Large || size == TableSize::Huge) {
        return SafetyVerdict::Caution;
    }

    SafetyVerdict::Safe
}

/// Check whether a DDL operation causes irreversible data loss.
pub(crate) fn is_data_loss(op: &DdlOperation) -> bool {
    matches!(
        op,
        DdlOperation::DropTable { .. }
            | DdlOperation::AlterTableDropColumn { .. }
            | DdlOperation::TruncateTable { .. }
    )
}

/// Extract the affected table name from a DDL operation, if applicable.
pub(crate) fn affected_table(op: &DdlOperation) -> Option<String> {
    match op {
        DdlOperation::CreateTable { table, .. }
        | DdlOperation::DropTable { table }
        | DdlOperation::AlterTableAddColumn { table, .. }
        | DdlOperation::AlterTableDropColumn { table, .. }
        | DdlOperation::AlterTableAlterColumn { table, .. }
        | DdlOperation::CreateIndex { table, .. }
        | DdlOperation::AddConstraint { table, .. }
        | DdlOperation::DropConstraint { table, .. }
        | DdlOperation::TruncateTable { table } => Some(table.clone()),
        DdlOperation::DropIndex { .. }
        | DdlOperation::CreateView { .. }
        | DdlOperation::DropView { .. }
        | DdlOperation::CreateFunction { .. }
        | DdlOperation::DropFunction { .. }
        | DdlOperation::CreateEnum { .. }
        | DdlOperation::Other { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Lock level ordering ───────────────────────────────────────────

    #[test]
    fn test_lock_level_ordering() {
        assert!(LockLevel::None < LockLevel::AccessShareLock);
        assert!(LockLevel::AccessShareLock < LockLevel::RowShareLock);
        assert!(LockLevel::RowShareLock < LockLevel::RowExclusiveLock);
        assert!(LockLevel::RowExclusiveLock < LockLevel::ShareUpdateExclusiveLock);
        assert!(LockLevel::ShareUpdateExclusiveLock < LockLevel::ShareLock);
        assert!(LockLevel::ShareLock < LockLevel::ShareRowExclusiveLock);
        assert!(LockLevel::ShareRowExclusiveLock < LockLevel::ExclusiveLock);
        assert!(LockLevel::ExclusiveLock < LockLevel::AccessExclusiveLock);
    }

    // ── Verdict computation ───────────────────────────────────────────

    #[test]
    fn test_verdict_access_exclusive_large_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Large, false),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_access_exclusive_huge_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Huge, false),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_data_loss_on_large_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Large, true),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_data_loss_on_huge_is_danger() {
        assert_eq!(
            compute_verdict(LockLevel::None, TableSize::Huge, true),
            SafetyVerdict::Danger
        );
    }

    #[test]
    fn test_verdict_access_exclusive_small_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Small, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_access_exclusive_medium_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::AccessExclusiveLock, TableSize::Medium, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_share_lock_large_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::ShareLock, TableSize::Large, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_share_lock_huge_is_caution() {
        assert_eq!(
            compute_verdict(LockLevel::ShareLock, TableSize::Huge, false),
            SafetyVerdict::Caution
        );
    }

    #[test]
    fn test_verdict_share_lock_small_is_safe() {
        assert_eq!(
            compute_verdict(LockLevel::ShareLock, TableSize::Small, false),
            SafetyVerdict::Safe
        );
    }

    #[test]
    fn test_verdict_none_lock_small_is_safe() {
        assert_eq!(
            compute_verdict(LockLevel::None, TableSize::Small, false),
            SafetyVerdict::Safe
        );
    }

    #[test]
    fn test_verdict_concurrent_index_large_is_safe() {
        assert_eq!(
            compute_verdict(LockLevel::ShareUpdateExclusiveLock, TableSize::Large, false),
            SafetyVerdict::Safe
        );
    }

    // ── Data loss detection ───────────────────────────────────────────

    #[test]
    fn test_data_loss_drop_table() {
        let op = DdlOperation::DropTable {
            table: "users".into(),
        };
        assert!(is_data_loss(&op));
    }

    #[test]
    fn test_data_loss_drop_column() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "users".into(),
            column: "email".into(),
        };
        assert!(is_data_loss(&op));
    }

    #[test]
    fn test_data_loss_truncate() {
        let op = DdlOperation::TruncateTable {
            table: "logs".into(),
        };
        assert!(is_data_loss(&op));
    }

    #[test]
    fn test_no_data_loss_create_table() {
        let op = DdlOperation::CreateTable {
            table: "users".into(),
            if_not_exists: false,
        };
        assert!(!is_data_loss(&op));
    }

    #[test]
    fn test_no_data_loss_add_column() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "users".into(),
            column: "email".into(),
            data_type: "text".into(),
            has_default: false,
            is_not_null: false,
        };
        assert!(!is_data_loss(&op));
    }

    #[test]
    fn test_no_data_loss_create_index() {
        let op = DdlOperation::CreateIndex {
            name: "idx".into(),
            table: "users".into(),
            is_concurrent: true,
            is_unique: false,
        };
        assert!(!is_data_loss(&op));
    }

    // ── Affected table extraction ─────────────────────────────────────

    #[test]
    fn test_affected_table_create_table() {
        let op = DdlOperation::CreateTable {
            table: "orders".into(),
            if_not_exists: false,
        };
        assert_eq!(affected_table(&op), Some("orders".into()));
    }

    #[test]
    fn test_affected_table_create_view_is_none() {
        let op = DdlOperation::CreateView {
            name: "v_stats".into(),
            is_materialized: false,
        };
        assert_eq!(affected_table(&op), None);
    }

    #[test]
    fn test_affected_table_create_function_is_none() {
        let op = DdlOperation::CreateFunction {
            name: "my_func".into(),
        };
        assert_eq!(affected_table(&op), None);
    }

    #[test]
    fn test_affected_table_other_is_none() {
        let op = DdlOperation::Other {
            statement_preview: "GRANT SELECT ON ...".into(),
        };
        assert_eq!(affected_table(&op), None);
    }

    // ── Display impls ─────────────────────────────────────────────────

    #[test]
    fn test_lock_level_display() {
        assert_eq!(LockLevel::None.to_string(), "None");
        assert_eq!(LockLevel::AccessShareLock.to_string(), "ACCESS SHARE");
        assert_eq!(LockLevel::RowShareLock.to_string(), "ROW SHARE");
        assert_eq!(LockLevel::RowExclusiveLock.to_string(), "ROW EXCLUSIVE");
        assert_eq!(
            LockLevel::ShareUpdateExclusiveLock.to_string(),
            "SHARE UPDATE EXCLUSIVE"
        );
        assert_eq!(LockLevel::ShareLock.to_string(), "SHARE");
        assert_eq!(
            LockLevel::ShareRowExclusiveLock.to_string(),
            "SHARE ROW EXCLUSIVE"
        );
        assert_eq!(LockLevel::ExclusiveLock.to_string(), "EXCLUSIVE");
        assert_eq!(
            LockLevel::AccessExclusiveLock.to_string(),
            "ACCESS EXCLUSIVE"
        );
    }

    #[test]
    fn test_safety_verdict_display() {
        assert_eq!(SafetyVerdict::Safe.to_string(), "SAFE");
        assert_eq!(SafetyVerdict::Caution.to_string(), "CAUTION");
        assert_eq!(SafetyVerdict::Danger.to_string(), "DANGER");
    }

    #[test]
    fn test_table_size_display() {
        assert_eq!(TableSize::Small.to_string(), "Small (<10k rows)");
        assert_eq!(TableSize::Medium.to_string(), "Medium (10k-1M rows)");
        assert_eq!(TableSize::Large.to_string(), "Large (1M-100M rows)");
        assert_eq!(TableSize::Huge.to_string(), "Huge (>100M rows)");
    }

    // ── Row count classification ──────────────────────────────────────

    #[test]
    fn test_classify_row_count_small() {
        assert_eq!(
            classify_row_count(0, 1_000_000, 100_000_000),
            TableSize::Small
        );
        assert_eq!(
            classify_row_count(9_999, 1_000_000, 100_000_000),
            TableSize::Small
        );
    }

    #[test]
    fn test_classify_row_count_medium() {
        assert_eq!(
            classify_row_count(10_000, 1_000_000, 100_000_000),
            TableSize::Medium
        );
        assert_eq!(
            classify_row_count(500_000, 1_000_000, 100_000_000),
            TableSize::Medium
        );
        assert_eq!(
            classify_row_count(1_000_000, 1_000_000, 100_000_000),
            TableSize::Medium
        );
    }

    #[test]
    fn test_classify_row_count_large() {
        assert_eq!(
            classify_row_count(1_000_001, 1_000_000, 100_000_000),
            TableSize::Large
        );
        assert_eq!(
            classify_row_count(50_000_000, 1_000_000, 100_000_000),
            TableSize::Large
        );
        assert_eq!(
            classify_row_count(100_000_000, 1_000_000, 100_000_000),
            TableSize::Large
        );
    }

    #[test]
    fn test_classify_row_count_huge() {
        assert_eq!(
            classify_row_count(100_000_001, 1_000_000, 100_000_000),
            TableSize::Huge
        );
        assert_eq!(
            classify_row_count(1_000_000_000, 1_000_000, 100_000_000),
            TableSize::Huge
        );
    }

    #[test]
    fn test_classify_custom_thresholds() {
        assert_eq!(classify_row_count(500, 1_000, 10_000), TableSize::Small);
        assert_eq!(classify_row_count(1_001, 1_000, 10_000), TableSize::Large);
        assert_eq!(classify_row_count(10_000, 1_000, 10_000), TableSize::Large);
        assert_eq!(classify_row_count(10_001, 1_000, 10_000), TableSize::Huge);
    }

    // ── SafetyConfig defaults ─────────────────────────────────────────

    #[test]
    fn test_safety_config_defaults() {
        let config = SafetyConfig::default();
        assert!(config.enabled);
        assert!(!config.block_on_danger);
        assert_eq!(config.large_table_threshold, 1_000_000);
        assert_eq!(config.huge_table_threshold, 100_000_000);
    }
}
