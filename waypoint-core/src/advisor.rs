//! Schema advisor: proactive suggestions for schema improvements.
//!
//! This module owns the engine-agnostic types and the dialect-aware
//! dispatcher. The actual rule implementations live in
//! [`crate::engines::postgres::advisor`] (rules A001-A010) and
//! [`crate::engines::mysql::advisor`] (rules M001-M005).
//!
//! Rule IDs are namespaced per engine so JSON consumers can ignore the
//! dialect — they share the same [`Advisory`] / [`AdvisorReport`] types.

use serde::Serialize;

use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::Result;

// ── Re-exports of the engine-specific entry points ──────────────────────────

#[cfg(feature = "mysql")]
pub use crate::engines::mysql::advisor::analyze as analyze_mysql;
#[cfg(feature = "postgres")]
pub use crate::engines::postgres::advisor::analyze;

// ── Shared types ────────────────────────────────────────────────────────────

/// Configuration for the schema advisor.
#[derive(Debug, Clone, Default)]
pub struct AdvisorConfig {
    /// Whether to run the advisor after migrations.
    pub run_after_migrate: bool,
    /// List of rule IDs to disable (e.g., ["A003", "A006"]).
    pub disabled_rules: Vec<String>,
}

/// Severity of an advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AdvisorySeverity {
    Info,
    Suggestion,
    Warning,
}

impl std::fmt::Display for AdvisorySeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Suggestion => write!(f, "suggestion"),
            Self::Warning => write!(f, "warning"),
        }
    }
}

/// A single advisory finding.
#[derive(Debug, Clone, Serialize)]
pub struct Advisory {
    /// Rule ID (e.g., "A001").
    pub rule_id: String,
    /// Category of the advisory.
    pub category: String,
    /// Severity level.
    pub severity: AdvisorySeverity,
    /// Affected database object (e.g., "users.email", "idx_name").
    pub object: String,
    /// Human-readable explanation of the issue.
    pub explanation: String,
    /// Generated SQL to fix the issue.
    pub fix_sql: Option<String>,
}

/// Report from the schema advisor.
#[derive(Debug, Clone, Serialize)]
pub struct AdvisorReport {
    /// Schema that was analyzed.
    pub schema: String,
    /// All advisory findings.
    pub advisories: Vec<Advisory>,
    /// Count of warnings.
    pub warning_count: usize,
    /// Count of suggestions.
    pub suggestion_count: usize,
    /// Count of info items.
    pub info_count: usize,
}

/// Run all advisory rules against the database schema (dialect-aware entry).
pub async fn analyze_db(
    client: &DbClient,
    schema: &str,
    config: &AdvisorConfig,
) -> Result<AdvisorReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => analyze(client.as_postgres()?, schema, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(crate::error::WaypointError::ConfigError(
            "PostgreSQL support is not compiled in".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => analyze_mysql(client, schema, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(crate::error::WaypointError::ConfigError(
            "MySQL support is not compiled in".into(),
        )),
    }
}

/// Generate combined fix SQL from all advisories.
pub fn generate_fix_sql(report: &AdvisorReport) -> String {
    let fixes: Vec<String> = report
        .advisories
        .iter()
        .filter_map(|a| {
            a.fix_sql.as_ref().map(|sql| {
                format!(
                    "-- {} [{}]: {}\n{}",
                    a.rule_id, a.severity, a.explanation, sql
                )
            })
        })
        .collect();
    fixes.join("\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_advisor_config_default() {
        let config = AdvisorConfig::default();
        assert!(!config.run_after_migrate);
        assert!(config.disabled_rules.is_empty());
    }

    #[test]
    fn test_generate_fix_sql_empty() {
        let report = AdvisorReport {
            schema: "public".to_string(),
            advisories: vec![],
            warning_count: 0,
            suggestion_count: 0,
            info_count: 0,
        };
        assert!(generate_fix_sql(&report).is_empty());
    }

    #[test]
    fn test_generate_fix_sql_with_advisories() {
        let report = AdvisorReport {
            schema: "public".to_string(),
            advisories: vec![
                Advisory {
                    rule_id: "A001".to_string(),
                    category: "Performance".to_string(),
                    severity: AdvisorySeverity::Warning,
                    object: "orders.user_id".to_string(),
                    explanation: "FK without index".to_string(),
                    fix_sql: Some(
                        "CREATE INDEX idx_orders_user_id ON \"orders\" (\"user_id\");".to_string(),
                    ),
                },
                Advisory {
                    rule_id: "A004".to_string(),
                    category: "Correctness".to_string(),
                    severity: AdvisorySeverity::Warning,
                    object: "logs".to_string(),
                    explanation: "No primary key".to_string(),
                    fix_sql: None,
                },
            ],
            warning_count: 2,
            suggestion_count: 0,
            info_count: 0,
        };
        let sql = generate_fix_sql(&report);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("A001"));
        assert!(!sql.contains("A004"));
    }

    #[test]
    fn test_advisory_severity_display() {
        assert_eq!(AdvisorySeverity::Info.to_string(), "info");
        assert_eq!(AdvisorySeverity::Suggestion.to_string(), "suggestion");
        assert_eq!(AdvisorySeverity::Warning.to_string(), "warning");
    }
}
