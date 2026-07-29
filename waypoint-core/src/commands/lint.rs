//! Static analysis of migration SQL files.
//!
//! Checks for common anti-patterns and dangerous operations
//! without requiring a database connection.

use std::path::PathBuf;

use serde::Serialize;

use crate::directive::{LintIgnoreScope, parse_lint_ignores};
use crate::error::Result;
use crate::migration::scan_migrations;
use crate::sql_parser::{
    DdlOperation, LocatedDdl, extract_ddl_operations_located, line_number_at, strip_comments,
};

/// Severity level for a lint issue.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum LintSeverity {
    /// A critical issue that will likely cause migration failure.
    Error,
    /// A potential problem or anti-pattern that deserves attention.
    Warning,
    /// An informational observation about the migration.
    Info,
}

impl std::fmt::Display for LintSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LintSeverity::Error => write!(f, "error"),
            LintSeverity::Warning => write!(f, "warning"),
            LintSeverity::Info => write!(f, "info"),
        }
    }
}

/// A single lint finding.
#[derive(Debug, Clone, Serialize)]
pub struct LintIssue {
    /// Unique identifier of the lint rule (e.g. "W001", "E001").
    pub rule_id: String,
    /// Severity level of this issue.
    pub severity: LintSeverity,
    /// Human-readable description of the issue.
    pub message: String,
    /// Filename of the migration script where the issue was found.
    pub script: String,
    /// Approximate line number of the issue, if determinable.
    pub line: Option<usize>,
    /// Suggested fix or remediation for the issue.
    pub suggestion: Option<String>,
}

/// An inline suppression that took effect during a lint run.
#[derive(Debug, Clone, Serialize)]
pub struct LintSuppression {
    /// Rule IDs the directive suppresses.
    pub rules: Vec<String>,
    /// The mandatory justification supplied by the directive.
    pub reason: String,
    /// Migration file the directive appears in.
    pub script: String,
    /// 1-based line number of the directive.
    pub line: usize,
    /// Whether the directive covers one statement or the whole file.
    pub scope: LintIgnoreScope,
    /// How many issues this directive suppressed.
    pub suppressed_count: usize,
}

/// Aggregate lint report.
#[derive(Debug, Serialize)]
pub struct LintReport {
    /// All lint issues found across checked files.
    pub issues: Vec<LintIssue>,
    /// Total number of migration files that were checked.
    pub files_checked: usize,
    /// Number of issues with Error severity.
    pub error_count: usize,
    /// Number of issues with Warning severity.
    pub warning_count: usize,
    /// Number of issues with Info severity.
    pub info_count: usize,
    /// Inline suppressions that were applied, with their justifications.
    pub suppressions: Vec<LintSuppression>,
    /// Total number of issues removed by inline suppressions.
    pub suppressed_count: usize,
}

/// An issue paired with the source offset it was raised at, so inline
/// suppressions can be scoped to a single statement.
struct PendingIssue {
    issue: LintIssue,
    offset: Option<usize>,
}

/// A validated suppression directive resolved against the file's statements.
struct ActiveSuppression {
    rules: Vec<String>,
    reason: String,
    line: usize,
    scope: LintIgnoreScope,
    /// Byte range of the statement this directive covers (`None` for file scope).
    span: Option<(usize, usize)>,
    hits: usize,
}

impl ActiveSuppression {
    fn covers(&self, issue: &LintIssue, offset: Option<usize>) -> bool {
        if !self.rules.contains(&issue.rule_id) {
            return false;
        }
        match self.span {
            None => true,
            Some((start, end)) => offset.is_some_and(|o| o >= start && o < end),
        }
    }
}

/// Execute the lint command.
pub fn execute(locations: &[PathBuf], disabled_rules: &[String]) -> Result<LintReport> {
    let migrations = scan_migrations(locations)?;
    let mut issues = Vec::new();
    let mut suppressions: Vec<LintSuppression> = Vec::new();
    let mut suppressed_count = 0usize;
    let disabled: std::collections::HashSet<&str> =
        disabled_rules.iter().map(|s| s.as_str()).collect();

    let files_checked = migrations.len();

    for migration in &migrations {
        // Skip undo migrations for linting
        if migration.is_undo() {
            continue;
        }

        let sql = &migration.sql;
        let script = &migration.script;

        // Semantic analysis runs against a comment-blanked copy of the file so
        // that keywords inside comments can never influence a rule. Blanking
        // preserves byte offsets, so diagnostics still resolve against `sql`.
        let code = strip_comments(sql);

        // I001: File contains only comments or whitespace
        if !disabled.contains("I001") && code.trim().is_empty() {
            issues.push(LintIssue {
                rule_id: "I001".to_string(),
                severity: LintSeverity::Info,
                message: "File contains only comments or whitespace".to_string(),
                script: script.clone(),
                line: None,
                suggestion: None,
            });
            continue;
        }

        let located = extract_ddl_operations_located(sql);
        let mut pending: Vec<PendingIssue> = Vec::new();

        for ddl in &located {
            let LocatedDdl { op, focus, .. } = ddl;
            let line = Some(line_number_at(sql, *focus));
            // Per-statement text, comments already removed.
            let upper = code[ddl.start..ddl.end].to_uppercase();

            let mut raise = |rule_id: &str, severity: LintSeverity, message: String, hint: &str| {
                pending.push(PendingIssue {
                    issue: LintIssue {
                        rule_id: rule_id.to_string(),
                        severity,
                        message,
                        script: script.clone(),
                        line,
                        suggestion: Some(hint.to_string()),
                    },
                    offset: Some(*focus),
                });
            };

            match op {
                // W001: CREATE TABLE without IF NOT EXISTS
                DdlOperation::CreateTable {
                    table,
                    if_not_exists,
                } if !if_not_exists && !disabled.contains("W001") => {
                    raise(
                        "W001",
                        LintSeverity::Warning,
                        format!("CREATE TABLE {} without IF NOT EXISTS", table),
                        "Use CREATE TABLE IF NOT EXISTS to make migration re-runnable",
                    );
                }

                // W002: CREATE INDEX without CONCURRENTLY
                DdlOperation::CreateIndex {
                    name,
                    is_concurrent,
                    ..
                } if !is_concurrent && !disabled.contains("W002") => {
                    raise(
                        "W002",
                        LintSeverity::Warning,
                        format!(
                            "CREATE INDEX {} without CONCURRENTLY (blocks writes during creation)",
                            name
                        ),
                        "Use CREATE INDEX CONCURRENTLY to avoid blocking writes",
                    );
                }

                // E001: ADD COLUMN NOT NULL without DEFAULT.
                // `is_not_null` / `has_default` come from the parsed column
                // definition, so a NOT NULL inside a comment, a string
                // literal, or a CHECK expression does not trigger this.
                DdlOperation::AlterTableAddColumn {
                    table,
                    column,
                    is_not_null,
                    has_default,
                    ..
                } if *is_not_null && !has_default && !disabled.contains("E001") => {
                    raise(
                        "E001",
                        LintSeverity::Error,
                        format!(
                            "ADD COLUMN {}.{} is NOT NULL without DEFAULT (will fail if table has rows)",
                            table, column
                        ),
                        "Add a DEFAULT value or make the column nullable",
                    );
                }

                // W003: ALTER COLUMN TYPE (full table rewrite + lock).
                // The `upper.contains("TYPE")` guard distinguishes a TYPE
                // change from other ALTER COLUMN forms (SET DEFAULT, SET NOT
                // NULL, etc.) which don't trigger a rewrite.
                DdlOperation::AlterTableAlterColumn { table, column }
                    if !disabled.contains("W003") && upper.contains("TYPE") =>
                {
                    raise(
                        "W003",
                        LintSeverity::Warning,
                        format!(
                            "ALTER COLUMN {}.{} TYPE causes full table rewrite and exclusive lock",
                            table, column
                        ),
                        "Consider a multi-step approach: add new column, backfill, swap",
                    );
                }

                // W004: DROP TABLE / DROP COLUMN (destructive)
                DdlOperation::DropTable { table } if !disabled.contains("W004") => {
                    raise(
                        "W004",
                        LintSeverity::Warning,
                        format!("DROP TABLE {} is destructive and irreversible", table),
                        "Ensure you have a backup or undo migration",
                    );
                }
                DdlOperation::AlterTableDropColumn { table, column }
                    if !disabled.contains("W004") =>
                {
                    raise(
                        "W004",
                        LintSeverity::Warning,
                        format!(
                            "DROP COLUMN {}.{} is destructive and irreversible",
                            table, column
                        ),
                        "Ensure you have a backup or undo migration",
                    );
                }

                // W006: Volatile DEFAULT expression on ADD COLUMN. Evaluated
                // against this column's own DEFAULT expression, so a sibling
                // clause in the same ALTER TABLE cannot trigger it. Pre-PG11 a
                // volatile default forces a full table rewrite.
                DdlOperation::AlterTableAddColumn {
                    table,
                    column,
                    default_expr: Some(expr),
                    ..
                } if !disabled.contains("W006") && is_volatile_default(expr) => {
                    raise(
                        "W006",
                        LintSeverity::Warning,
                        format!(
                            "ADD COLUMN {}.{} with volatile DEFAULT expression (pre-PG11: table rewrite)",
                            table, column
                        ),
                        "On PostgreSQL < 11, volatile defaults cause a full table rewrite",
                    );
                }

                // W007: TRUNCATE TABLE
                DdlOperation::TruncateTable { table } if !disabled.contains("W007") => {
                    raise(
                        "W007",
                        LintSeverity::Warning,
                        format!(
                            "TRUNCATE TABLE {} is destructive and acquires ACCESS EXCLUSIVE lock",
                            table
                        ),
                        "Ensure this is intentional and the table can be locked exclusively",
                    );
                }

                _ => {}
            }
        }

        // Distinct statement spans, in source order. Every non-empty statement
        // yields at least one operation, so this covers the whole file.
        let mut spans: Vec<(usize, usize)> = Vec::new();
        for ddl in &located {
            if spans.last() != Some(&(ddl.start, ddl.end)) {
                spans.push((ddl.start, ddl.end));
            }
        }

        // E002: Multiple DDL statements without explicit transaction control
        if !disabled.contains("E002") {
            let ddl_count = spans
                .iter()
                .filter(|(s, e)| {
                    located.iter().any(|d| {
                        d.start == *s && d.end == *e && !matches!(d.op, DdlOperation::Other { .. })
                    })
                })
                .count();
            let has_begin = spans.iter().any(|(s, e)| {
                code[*s..*e]
                    .get(..5)
                    .is_some_and(|w| w.eq_ignore_ascii_case("BEGIN"))
            });
            if ddl_count > 1 && !has_begin {
                // Reported as an Error: waypoint does wrap each migration in a
                // transaction, but relying on that means the file is not
                // safely runnable by hand or by another tool.
                pending.push(PendingIssue {
                    issue: LintIssue {
                        rule_id: "E002".to_string(),
                        severity: LintSeverity::Error,
                        message: format!(
                            "{} DDL statements without explicit BEGIN/COMMIT (relies on tool-level transaction)",
                            ddl_count
                        ),
                        script: script.clone(),
                        line: None,
                        suggestion: Some("Consider adding explicit BEGIN/COMMIT for clarity, or split into separate migrations".to_string()),
                    },
                    offset: None,
                });
            }
        }

        // Inline suppressions: `-- waypoint:lint-ignore[-file] <RULES> reason=<why>`
        let mut active = resolve_suppressions(sql, script, &spans, &disabled, &mut pending);

        for p in pending {
            if let Some(s) = active.iter_mut().find(|s| s.covers(&p.issue, p.offset)) {
                s.hits += 1;
                suppressed_count += 1;
                continue;
            }
            issues.push(p.issue);
        }

        for s in active {
            if s.hits == 0 && !disabled.contains("I002") {
                issues.push(LintIssue {
                    rule_id: "I002".to_string(),
                    severity: LintSeverity::Info,
                    message: format!(
                        "Lint suppression for {} matched no issues",
                        s.rules.join(", ")
                    ),
                    script: script.clone(),
                    line: Some(s.line),
                    suggestion: Some("Remove the stale waypoint:lint-ignore directive".to_string()),
                });
            }
            suppressions.push(LintSuppression {
                rules: s.rules,
                reason: s.reason,
                script: script.clone(),
                line: s.line,
                scope: s.scope,
                suppressed_count: s.hits,
            });
        }
    }

    let error_count = issues
        .iter()
        .filter(|i| i.severity == LintSeverity::Error)
        .count();
    let warning_count = issues
        .iter()
        .filter(|i| i.severity == LintSeverity::Warning)
        .count();
    let info_count = issues
        .iter()
        .filter(|i| i.severity == LintSeverity::Info)
        .count();

    Ok(LintReport {
        issues,
        files_checked,
        error_count,
        warning_count,
        info_count,
        suppressions,
        suppressed_count,
    })
}

/// Whether a `DEFAULT` expression calls a volatile function, which forces a
/// full table rewrite on PostgreSQL before 11.
fn is_volatile_default(expr: &str) -> bool {
    let compact: String = expr
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_uppercase)
        .collect();
    ["RANDOM()", "GEN_RANDOM_UUID()", "NOW()"]
        .iter()
        .any(|f| compact.contains(f))
}

/// Resolve the inline suppression directives in one migration file.
///
/// Directives missing a rule list or a `reason=` are rejected with an E003
/// issue and take no effect — a suppression is only honoured when it says
/// what it silences and why.
fn resolve_suppressions(
    sql: &str,
    script: &str,
    spans: &[(usize, usize)],
    disabled: &std::collections::HashSet<&str>,
    pending: &mut Vec<PendingIssue>,
) -> Vec<ActiveSuppression> {
    let mut active = Vec::new();

    for d in parse_lint_ignores(sql) {
        let problem = if d.rules.is_empty() {
            Some("names no rule IDs".to_string())
        } else if d.reason.is_none() {
            Some(format!(
                "suppresses {} without a reason",
                d.rules.join(", ")
            ))
        } else {
            None
        };

        if let Some(problem) = problem {
            if !disabled.contains("E003") {
                pending.push(PendingIssue {
                    issue: LintIssue {
                        rule_id: "E003".to_string(),
                        severity: LintSeverity::Error,
                        message: format!("Lint suppression directive {} (ignored)", problem),
                        script: script.to_string(),
                        line: Some(d.line),
                        suggestion: Some(
                            "Write `-- waypoint:lint-ignore <RULE[,RULE]> reason=<why>`"
                                .to_string(),
                        ),
                    },
                    offset: None,
                });
            }
            continue;
        }

        // A statement-scoped directive covers the first statement that has not
        // yet ended at the directive's line — either the statement it sits
        // inside, or the next one to start.
        let span = match d.scope {
            LintIgnoreScope::File => None,
            LintIgnoreScope::NextStatement => Some(
                spans
                    .iter()
                    .copied()
                    .find(|(_, end)| *end > d.offset)
                    // No statement follows: an empty range that covers nothing,
                    // so the directive is reported as stale.
                    .unwrap_or((d.offset, d.offset)),
            ),
        };

        active.push(ActiveSuppression {
            rules: d.rules,
            reason: d.reason.unwrap_or_default(),
            line: d.line,
            scope: d.scope,
            span,
            hits: 0,
        });
    }

    active
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_migration(dir: &std::path::Path, name: &str, sql: &str) {
        fs::write(dir.join(name), sql).unwrap();
    }

    #[test]
    fn test_lint_create_table_without_if_not_exists() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Create_users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W001"));
    }

    #[test]
    fn test_lint_create_table_with_if_not_exists_passes() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Create_users.sql",
            "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY);",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(!report.issues.iter().any(|i| i.rule_id == "W001"));
    }

    #[test]
    fn test_lint_add_column_not_null_without_default() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Add_email.sql",
            "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL;",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "E001"));
        assert!(report.error_count > 0);
    }

    #[test]
    fn test_lint_index_without_concurrently() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Add_index.sql",
            "CREATE INDEX idx_users_email ON users (email);",
        );

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W002"));
    }

    #[test]
    fn test_lint_disabled_rules() {
        let dir = TempDir::new().unwrap();
        setup_migration(
            dir.path(),
            "V1__Create_users.sql",
            "CREATE TABLE users (id SERIAL PRIMARY KEY);",
        );

        let report = execute(&[dir.path().to_path_buf()], &["W001".to_string()]).unwrap();
        assert!(!report.issues.iter().any(|i| i.rule_id == "W001"));
    }

    #[test]
    fn test_lint_drop_table() {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Drop_old.sql", "DROP TABLE old_table;");

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W004"));
    }

    #[test]
    fn test_lint_empty_file() {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Empty.sql", "-- Just a comment\n");

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "I001"));
    }

    #[test]
    fn test_lint_truncate() {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Truncate.sql", "TRUNCATE TABLE users;");

        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        assert!(report.issues.iter().any(|i| i.rule_id == "W007"));
    }

    /// Lint a single migration body and return the report.
    fn lint_one(sql: &str) -> (TempDir, LintReport) {
        let dir = TempDir::new().unwrap();
        setup_migration(dir.path(), "V1__Test.sql", sql);
        let report = execute(&[dir.path().to_path_buf()], &[]).unwrap();
        (dir, report)
    }

    fn e001(report: &LintReport) -> Vec<&LintIssue> {
        report
            .issues
            .iter()
            .filter(|i| i.rule_id == "E001")
            .collect()
    }

    // --- E001 false positives: IF NOT EXISTS + comments ---------------------

    #[test]
    fn test_add_column_if_not_exists_nullable_with_not_null_in_comment() {
        // The reported regression: `IF NOT EXISTS` was parsed as the column
        // name and `NOT NULL` was read out of the preceding comment.
        let (_d, report) = lint_one(
            "-- Every ceremony writes the threshold NOT NULL.\n\
             ALTER TABLE dicom.reid_shares\n  \
               ADD COLUMN IF NOT EXISTS threshold smallint\n    \
                 CHECK (threshold IS NULL OR threshold BETWEEN 1 AND 255);\n",
        );
        assert!(
            e001(&report).is_empty(),
            "nullable column must not raise E001: {:?}",
            report.issues
        );
        assert_eq!(report.error_count, 0);
    }

    #[test]
    fn test_add_column_if_not_exists_not_null_reports_real_column_name() {
        let (_d, report) = lint_one(
            "-- waypoint header\n\
             ALTER TABLE dicom.reid_shares\n  \
               ADD COLUMN IF NOT EXISTS threshold smallint NOT NULL;\n",
        );
        let issues = e001(&report);
        assert_eq!(issues.len(), 1);
        assert!(
            issues[0].message.contains("reid_shares.threshold"),
            "expected the parsed column name, got: {}",
            issues[0].message
        );
        for bad in ["IF", "NOT", "EXISTS"] {
            assert!(
                !issues[0].message.contains(&format!(".{} ", bad)),
                "column name leaked keyword {}: {}",
                bad,
                issues[0].message
            );
        }
        // Points at the ADD COLUMN line, not the comment on line 1.
        assert_eq!(issues[0].line, Some(3));
    }

    #[test]
    fn test_add_column_if_not_exists_with_default_passes() {
        let (_d, report) = lint_one(
            "ALTER TABLE dicom.reid_shares\n  \
               ADD COLUMN IF NOT EXISTS threshold smallint NOT NULL DEFAULT 1;\n",
        );
        assert!(e001(&report).is_empty(), "{:?}", report.issues);
    }

    #[test]
    fn test_add_column_without_if_not_exists_keeps_existing_behavior() {
        let (_d, report) = lint_one("ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL;");
        let issues = e001(&report);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("users.email"));

        let (_d, ok) = lint_one("ALTER TABLE users ADD COLUMN email VARCHAR(255);");
        assert!(e001(&ok).is_empty());
    }

    #[test]
    fn test_not_null_in_line_comment_is_ignored() {
        let (_d, report) = lint_one(
            "-- NOT NULL\n\
             ALTER TABLE users ADD COLUMN email text; -- NOT NULL enforced elsewhere\n",
        );
        assert!(e001(&report).is_empty(), "{:?}", report.issues);
    }

    #[test]
    fn test_not_null_in_block_comment_is_ignored() {
        let (_d, report) = lint_one(
            "/* the app treats this as NOT NULL\n   and DEFAULT 0 */\n\
             ALTER TABLE users ADD COLUMN score int;\n",
        );
        assert!(e001(&report).is_empty(), "{:?}", report.issues);
    }

    #[test]
    fn test_not_null_inside_check_expression_is_ignored() {
        let (_d, report) =
            lint_one("ALTER TABLE users ADD COLUMN email text CHECK (email IS NOT NULL);");
        assert!(e001(&report).is_empty(), "{:?}", report.issues);
    }

    #[test]
    fn test_diagnostic_line_points_at_statement_not_comment() {
        let sql = "-- line one\n-- line two\n-- line three\nALTER TABLE users ADD COLUMN a int NOT NULL;\n";
        let (_d, report) = lint_one(sql);
        assert_eq!(e001(&report)[0].line, Some(4));
    }

    #[test]
    fn test_multiple_add_columns_in_one_statement() {
        let (_d, report) = lint_one(
            "ALTER TABLE t ADD COLUMN a int DEFAULT 0, ADD COLUMN IF NOT EXISTS b int NOT NULL;",
        );
        let issues = e001(&report);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("t.b"));
        // One statement — E002 must not fire for a single ALTER TABLE.
        assert!(!report.issues.iter().any(|i| i.rule_id == "E002"));
    }

    #[test]
    fn test_w006_only_fires_for_the_column_with_the_volatile_default() {
        let (_d, report) = lint_one(
            "ALTER TABLE t\n  \
               ADD COLUMN IF NOT EXISTS tags text[] DEFAULT '{}',\n  \
               ADD COLUMN IF NOT EXISTS updated_utc timestamptz NOT NULL DEFAULT now();\n",
        );
        let w006: Vec<_> = report
            .issues
            .iter()
            .filter(|i| i.rule_id == "W006")
            .collect();
        assert_eq!(w006.len(), 1, "{:?}", report.issues);
        assert!(w006[0].message.contains("t.updated_utc"));
    }

    #[test]
    fn test_w006_ignores_volatile_call_elsewhere_in_the_file() {
        let (_d, report) = lint_one(
            "ALTER TABLE t ADD COLUMN a int DEFAULT 0;\n\
             UPDATE t SET a = extract(epoch from now());\n",
        );
        assert!(!report.issues.iter().any(|i| i.rule_id == "W006"));
    }

    // --- Inline suppressions -----------------------------------------------

    #[test]
    fn test_lint_ignore_scoped_to_next_statement() {
        let (_d, report) = lint_one(
            "-- waypoint:lint-ignore E001 reason=\"backfilled by the writer\"\n\
             ALTER TABLE t ADD COLUMN a int NOT NULL;\n\
             ALTER TABLE t ADD COLUMN b int NOT NULL;\n",
        );
        let issues = e001(&report);
        assert_eq!(issues.len(), 1, "only the first statement is suppressed");
        assert!(issues[0].message.contains("t.b"));
        assert_eq!(report.suppressed_count, 1);
        assert_eq!(report.suppressions[0].reason, "backfilled by the writer");
        assert_eq!(report.suppressions[0].scope, LintIgnoreScope::NextStatement);
    }

    #[test]
    fn test_lint_ignore_file_scope() {
        let (_d, report) = lint_one(
            "-- waypoint:lint-ignore-file E001 reason=migration runs on an empty table\n\
             ALTER TABLE t ADD COLUMN a int NOT NULL;\n\
             ALTER TABLE t ADD COLUMN b int NOT NULL;\n",
        );
        assert!(e001(&report).is_empty());
        assert_eq!(report.suppressed_count, 2);
        assert_eq!(
            report.suppressions[0].reason,
            "migration runs on an empty table"
        );
    }

    #[test]
    fn test_lint_ignore_requires_a_reason() {
        let (_d, report) =
            lint_one("-- waypoint:lint-ignore E001\nALTER TABLE t ADD COLUMN a int NOT NULL;\n");
        assert_eq!(e001(&report).len(), 1, "suppression must not take effect");
        assert!(report.issues.iter().any(|i| i.rule_id == "E003"));
        assert_eq!(report.suppressed_count, 0);
    }

    #[test]
    fn test_lint_ignore_requires_rule_ids() {
        let (_d, report) = lint_one(
            "-- waypoint:lint-ignore reason=just because\nALTER TABLE t ADD COLUMN a int NOT NULL;\n",
        );
        assert_eq!(e001(&report).len(), 1);
        assert!(report.issues.iter().any(|i| i.rule_id == "E003"));
    }

    #[test]
    fn test_lint_ignore_only_suppresses_named_rules() {
        let (_d, report) = lint_one(
            "-- waypoint:lint-ignore W004 reason=intentional teardown\n\
             ALTER TABLE t ADD COLUMN a int NOT NULL;\n",
        );
        assert_eq!(e001(&report).len(), 1);
    }

    #[test]
    fn test_stale_lint_ignore_reported_as_info() {
        let (_d, report) = lint_one(
            "-- waypoint:lint-ignore E001 reason=no longer needed\n\
             ALTER TABLE t ADD COLUMN a int;\n",
        );
        assert!(report.issues.iter().any(|i| i.rule_id == "I002"));
        assert_eq!(report.suppressed_count, 0);
    }

    #[test]
    fn test_lint_ignore_inside_statement_covers_that_statement() {
        let (_d, report) = lint_one(
            "ALTER TABLE t\n\
             -- waypoint:lint-ignore E001 reason=empty table\n  \
               ADD COLUMN a int NOT NULL;\n",
        );
        assert!(e001(&report).is_empty(), "{:?}", report.issues);
    }
}
