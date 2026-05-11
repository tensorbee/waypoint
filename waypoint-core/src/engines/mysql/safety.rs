//! MySQL safety analysis. Uses worst-case lock mapping (ALGORITHM=COPY
//! semantics for ALTER TABLE) by default, but downgrades to `LockLevel::None`
//! when the running MySQL version supports `ALGORITHM=INSTANT` for the
//! specific operation. Table size comes from
//! `information_schema.tables.table_rows`, optionally refreshed via
//! `ANALYZE TABLE` first when `[safety] refresh_stats_mysql = true`. Shared
//! types and dispatcher live in [`crate::safety`].

use mysql_async::prelude::*;

use crate::db::DbClient;
use crate::error::Result;
use crate::safety::{
    affected_table, classify_row_count, compute_verdict, is_data_loss, LockLevel, SafetyConfig,
    SafetyReport, SafetyVerdict, StatementAnalysis, TableSize,
};
use crate::sql_parser::DdlOperation;

/// Parsed MySQL server version (major, minor, patch). Compared
/// lexicographically (the tuple ordering matches semver intent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MysqlVersion(pub u32, pub u32, pub u32);

impl MysqlVersion {
    /// Parse the `@@version` system variable. MySQL emits strings like
    /// `8.0.36`, `8.0.36-debug`, `8.4.0-cluster`, etc. We accept anything
    /// that starts with three dot-separated integers and ignore the suffix.
    /// Returns `None` if the string is malformed.
    pub(crate) fn parse(s: &str) -> Option<Self> {
        let head = s.split(|c: char| !c.is_ascii_digit() && c != '.').next()?;
        let mut parts = head.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;
        Some(MysqlVersion(major, minor, patch))
    }
}

/// Query the connected MySQL server's version. Falls back to (0, 0, 0) so
/// the rest of the safety pass stays conservative if the version query
/// fails (in which case `supports_instant_*` returns false and we keep the
/// worst-case lock mapping).
async fn detect_mysql_version(client: &DbClient) -> MysqlVersion {
    let pool = match client.as_mysql() {
        Ok(p) => p,
        Err(_) => return MysqlVersion(0, 0, 0),
    };
    let Ok(mut conn) = pool.get_conn().await else {
        return MysqlVersion(0, 0, 0);
    };
    let row: Option<String> = conn.query_first("SELECT @@version").await.unwrap_or(None);
    row.as_deref()
        .and_then(MysqlVersion::parse)
        .unwrap_or(MysqlVersion(0, 0, 0))
}

/// Whether MySQL `version` supports `ALGORITHM=INSTANT` for ADD COLUMN.
///
/// 8.0.12 introduced INSTANT ADD COLUMN, but only at the end of the table
/// definition. We can't tell statically whether a given ALTER places the
/// column at the end (we don't parse FIRST/AFTER), so for 8.0.12–8.0.28 we
/// stay conservative and only flag INSTANT-eligibility from 8.0.29+, which
/// lifted the position restriction.
fn supports_instant_add_column(version: MysqlVersion) -> bool {
    version >= MysqlVersion(8, 0, 29)
}

/// Whether MySQL `version` supports `ALGORITHM=INSTANT` for DROP COLUMN.
/// Introduced in 8.0.29.
fn supports_instant_drop_column(version: MysqlVersion) -> bool {
    version >= MysqlVersion(8, 0, 29)
}

/// Determine the approximate MySQL lock level required by a DDL operation,
/// given the connected server's version.
///
/// Conservative by default — ALTER TABLE maps to `AccessExclusiveLock` to
/// reflect the worst-case `ALGORITHM=COPY` fallback. When the server
/// version supports `ALGORITHM=INSTANT` for a given operation and the
/// operation's parsed shape is compatible (e.g. `ADD COLUMN` without a
/// `NOT NULL` constraint that lacks a default), we downgrade to
/// `LockLevel::None`.
pub(crate) fn lock_level_for_ddl_versioned(op: &DdlOperation, version: MysqlVersion) -> LockLevel {
    match op {
        DdlOperation::CreateTable { .. } => LockLevel::None,
        DdlOperation::CreateView { .. } => LockLevel::None,
        DdlOperation::AlterTableAddColumn {
            is_not_null,
            has_default,
            ..
        } => {
            // INSTANT eligibility (8.0.29+): adding a column doesn't rewrite
            // existing rows even when not nullable, *as long as* the column
            // has a default value MySQL can stamp in metadata. A NOT NULL
            // column with no default has no value to populate existing rows
            // and forces ALGORITHM=COPY (or just fails outright on a
            // non-empty table).
            let instant_compatible = !*is_not_null || *has_default;
            if supports_instant_add_column(version) && instant_compatible {
                LockLevel::None
            } else {
                LockLevel::AccessExclusiveLock
            }
        }
        DdlOperation::AlterTableDropColumn { .. } => {
            if supports_instant_drop_column(version) {
                LockLevel::None
            } else {
                LockLevel::AccessExclusiveLock
            }
        }
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

/// Back-compat thin wrapper for callers that don't know the server version.
/// Uses the conservative (no INSTANT) mapping.
pub fn lock_level_for_ddl(op: &DdlOperation) -> LockLevel {
    lock_level_for_ddl_versioned(op, MysqlVersion(0, 0, 0))
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
    classify_table_size_with_refresh(
        client,
        schema,
        table,
        large_threshold,
        huge_threshold,
        false,
    )
    .await
}

/// Same as [`classify_table_size`] but optionally runs `ANALYZE TABLE` first
/// to refresh the engine's row-count estimator. `ANALYZE TABLE` is
/// non-blocking on InnoDB (acquires a short metadata lock) but still has a
/// cost, so the caller decides per `SafetyConfig::refresh_stats_mysql`.
pub async fn classify_table_size_with_refresh(
    client: &DbClient,
    schema: &str,
    table: &str,
    large_threshold: i64,
    huge_threshold: i64,
    refresh_stats: bool,
) -> Result<(TableSize, i64)> {
    let pool = client.as_mysql()?;
    let mut conn = pool.get_conn().await?;
    if refresh_stats {
        // ANALYZE TABLE on a missing table is a hard error; tolerate it so
        // a freshly-created table (CREATE in this migration, ALTER in the
        // next statement) doesn't blow up the whole safety pass.
        let _ = conn
            .query_drop(format!("ANALYZE TABLE `{}`.`{}`", schema, table))
            .await;
    }
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
fn generate_suggestions(op: &DdlOperation, size: TableSize, version: MysqlVersion) -> Vec<String> {
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
        DdlOperation::AlterTableDropColumn { .. } if !supports_instant_drop_column(version) => {
            suggestions.push(
                "DROP COLUMN on MySQL <8.0.29 falls back to ALGORITHM=COPY. Upgrade or use \
                 pt-online-schema-change for large tables. Also consider soft-delete pattern \
                 for reversibility."
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
    // Detect server version once per analysis so we can downgrade ALTER TABLE
    // verdicts when MySQL would actually use ALGORITHM=INSTANT.
    let version = detect_mysql_version(client).await;
    let mut statements = Vec::new();
    let mut all_suggestions = Vec::new();
    let mut worst_verdict = SafetyVerdict::Safe;

    for op in &ops {
        let lock = lock_level_for_ddl_versioned(op, version);
        let table = affected_table(op);
        let data_loss = is_data_loss(op);

        let (table_size, estimated_rows) = if let Some(ref t) = table {
            match classify_table_size_with_refresh(
                client,
                schema,
                t,
                config.large_table_threshold,
                config.huge_table_threshold,
                config.refresh_stats_mysql,
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
        let suggestions = generate_suggestions(op, size_for_verdict, version);
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

    #[test]
    fn parse_version_basic() {
        assert_eq!(MysqlVersion::parse("8.0.36"), Some(MysqlVersion(8, 0, 36)));
        assert_eq!(MysqlVersion::parse("8.4.0"), Some(MysqlVersion(8, 4, 0)));
    }

    #[test]
    fn parse_version_with_suffix() {
        // MySQL emits `8.0.36-debug`, `8.4.0-cluster`, etc.
        assert_eq!(
            MysqlVersion::parse("8.0.36-debug"),
            Some(MysqlVersion(8, 0, 36))
        );
        assert_eq!(
            MysqlVersion::parse("8.4.0-cluster"),
            Some(MysqlVersion(8, 4, 0))
        );
    }

    #[test]
    fn parse_version_rejects_malformed() {
        assert_eq!(MysqlVersion::parse(""), None);
        assert_eq!(MysqlVersion::parse("8.0"), None);
        assert_eq!(MysqlVersion::parse("not-a-version"), None);
    }

    #[test]
    fn version_ordering_is_semver_like() {
        assert!(MysqlVersion(8, 0, 28) < MysqlVersion(8, 0, 29));
        assert!(MysqlVersion(8, 0, 99) < MysqlVersion(8, 1, 0));
        assert!(MysqlVersion(8, 4, 0) > MysqlVersion(8, 0, 36));
    }

    #[test]
    fn instant_add_column_gated_by_version() {
        let op = DdlOperation::AlterTableAddColumn {
            table: "t".into(),
            column: "c".into(),
            data_type: "INT".into(),
            has_default: false,
            is_not_null: false,
        };
        // 8.0.28 — INSTANT ADD COLUMN restricted to end-of-table only, which
        // we can't detect statically, so we stay conservative.
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 0, 28)),
            LockLevel::AccessExclusiveLock
        );
        // 8.0.29+ — position restriction lifted; nullable column = INSTANT.
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 0, 29)),
            LockLevel::None
        );
    }

    #[test]
    fn instant_add_column_not_null_without_default_blocked() {
        // NOT NULL without a default forces ALGORITHM=COPY even on 8.0.29+
        // because there's no value to populate existing rows with.
        let op = DdlOperation::AlterTableAddColumn {
            table: "t".into(),
            column: "c".into(),
            data_type: "INT".into(),
            has_default: false,
            is_not_null: true,
        };
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 0, 36)),
            LockLevel::AccessExclusiveLock
        );
    }

    #[test]
    fn instant_add_column_not_null_with_default_allowed() {
        // NOT NULL + DEFAULT is INSTANT-eligible on 8.0.29+ (the default
        // value can be stored in metadata).
        let op = DdlOperation::AlterTableAddColumn {
            table: "t".into(),
            column: "c".into(),
            data_type: "INT".into(),
            has_default: true,
            is_not_null: true,
        };
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 0, 36)),
            LockLevel::None
        );
    }

    #[test]
    fn instant_drop_column_gated_by_version() {
        let op = DdlOperation::AlterTableDropColumn {
            table: "t".into(),
            column: "c".into(),
        };
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 0, 28)),
            LockLevel::AccessExclusiveLock
        );
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 0, 29)),
            LockLevel::None
        );
    }

    #[test]
    fn alter_column_type_always_conservative() {
        // Type changes always rewrite data on MySQL — no INSTANT path.
        let op = DdlOperation::AlterTableAlterColumn {
            table: "t".into(),
            column: "c".into(),
        };
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(8, 4, 0)),
            LockLevel::AccessExclusiveLock
        );
    }

    #[test]
    fn unknown_version_stays_conservative() {
        // detect_mysql_version returns (0, 0, 0) on failure — verify we
        // don't accidentally downgrade locks in that case.
        let op = DdlOperation::AlterTableAddColumn {
            table: "t".into(),
            column: "c".into(),
            data_type: "INT".into(),
            has_default: true,
            is_not_null: false,
        };
        assert_eq!(
            lock_level_for_ddl_versioned(&op, MysqlVersion(0, 0, 0)),
            LockLevel::AccessExclusiveLock
        );
    }
}
