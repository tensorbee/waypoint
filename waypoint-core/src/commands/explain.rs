//! Enhanced dry-run with EXPLAIN for pending migrations.
//!
//! Runs EXPLAIN on each DML statement within a rolled-back transaction
//! to show execution plans and identify potential issues.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::commands::info::{self, MigrationState};
use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::Result;
#[cfg(any(not(feature = "postgres"), not(feature = "mysql")))]
use crate::error::WaypointError;
use crate::placeholder::{build_placeholders, replace_placeholders};
use crate::sql_parser::split_statements;

/// EXPLAIN report for all pending migrations.
#[derive(Debug, Serialize)]
pub struct ExplainReport {
    /// Per-migration EXPLAIN analysis results.
    pub migrations: Vec<MigrationExplain>,
}

/// EXPLAIN analysis for a single migration.
#[derive(Debug, Serialize)]
pub struct MigrationExplain {
    /// Filename of the migration script.
    pub script: String,
    /// Version string, or None for repeatable migrations.
    pub version: Option<String>,
    /// EXPLAIN results for each statement in the migration.
    pub statements: Vec<StatementExplain>,
}

/// EXPLAIN analysis for a single statement.
#[derive(Debug, Serialize)]
pub struct StatementExplain {
    /// Truncated preview of the SQL statement (up to 80 characters).
    pub statement_preview: String,
    /// Full EXPLAIN output or a status message for DDL statements.
    pub plan: String,
    /// Estimated number of rows from the query plan, if available.
    pub estimated_rows: Option<f64>,
    /// Estimated total cost from the query plan, if available.
    pub estimated_cost: Option<f64>,
    /// Performance warnings derived from the execution plan.
    pub warnings: Vec<String>,
    /// Whether this statement is a DDL operation (not explainable).
    pub is_ddl: bool,
}

/// Execute explain analysis for pending migrations (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute(client: &Client, config: &WaypointConfig) -> Result<ExplainReport> {
    let infos = info::execute(client, config).await?;

    let pending: Vec<_> = infos
        .iter()
        .filter(|i| matches!(i.state, MigrationState::Pending | MigrationState::Outdated))
        .collect();

    let schema = &config.migrations.schema;
    let db_user = crate::db::get_current_user(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let db_name = crate::db::get_current_database(client)
        .await
        .unwrap_or_else(|_| "unknown".to_string());

    // Scan migration files to get SQL content
    let resolved = crate::migration::scan_migrations(&config.migrations.locations)?;

    let mut migrations = Vec::new();

    for info in &pending {
        // Find the resolved migration matching this info
        let migration = resolved.iter().find(|m| m.script == info.script);
        let sql = match migration {
            Some(m) => {
                let placeholders =
                    build_placeholders(&config.placeholders, schema, &db_user, &db_name, &m.script);
                replace_placeholders(&m.sql, &placeholders)?
            }
            None => continue,
        };

        let statements_raw = split_statements(&sql);
        let mut statements = Vec::new();

        // Begin a transaction for EXPLAIN
        client.batch_execute("BEGIN").await?;

        for stmt_str in &statements_raw {
            let trimmed = stmt_str.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            let preview: String = trimmed.chars().take(80).collect();
            let preview = if trimmed.len() > 80 {
                format!("{}...", preview)
            } else {
                preview
            };

            let upper = trimmed.to_uppercase();
            let is_ddl = upper.starts_with("CREATE")
                || upper.starts_with("ALTER")
                || upper.starts_with("DROP")
                || upper.starts_with("TRUNCATE");

            if is_ddl {
                // DDL can't be meaningfully EXPLAINed; execute it to build schema state
                match client.batch_execute(trimmed).await {
                    Ok(()) => {}
                    Err(e) => {
                        log::debug!("DDL statement failed during explain: {}", e);
                    }
                }
                statements.push(StatementExplain {
                    statement_preview: preview,
                    plan: "DDL statement — not explainable".to_string(),
                    estimated_rows: None,
                    estimated_cost: None,
                    warnings: vec![],
                    is_ddl: true,
                });
            } else {
                // Try EXPLAIN on DML
                let explain_sql = format!("EXPLAIN (FORMAT TEXT) {}", trimmed);
                match client.query(&explain_sql, &[]).await {
                    Ok(rows_result) => {
                        let plan_lines: Vec<String> =
                            rows_result.iter().map(|r| r.get::<_, String>(0)).collect();
                        let plan_str = plan_lines.join("\n");

                        let (rows, cost, warnings) = extract_plan_info_text(&plan_str);

                        statements.push(StatementExplain {
                            statement_preview: preview,
                            plan: plan_str,
                            estimated_rows: rows,
                            estimated_cost: cost,
                            warnings,
                            is_ddl: false,
                        });
                    }
                    Err(e) => {
                        statements.push(StatementExplain {
                            statement_preview: preview,
                            plan: format!("EXPLAIN failed: {}", e),
                            estimated_rows: None,
                            estimated_cost: None,
                            warnings: vec![],
                            is_ddl: false,
                        });
                    }
                }
            }
        }

        // Rollback the transaction
        let _ = client.batch_execute("ROLLBACK").await;

        migrations.push(MigrationExplain {
            script: info.script.clone(),
            version: info.version.clone(),
            statements,
        });
    }

    Ok(ExplainReport { migrations })
}

/// Execute explain analysis for pending migrations (dialect-aware entry).
pub async fn execute_db(client: &DbClient, config: &WaypointConfig) -> Result<ExplainReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => execute(client.as_postgres()?, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => execute_mysql(client, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

/// MySQL EXPLAIN path.
///
/// Unlike PG we don't wrap in a transaction (MySQL DDL auto-commits anyway).
/// DDL is reported as "not explainable" — DML gets EXPLAIN FORMAT=JSON. Since
/// we don't execute the migration's DDL, EXPLAIN on a DML statement that
/// references an as-yet-uncreated table will fail with a clear "table doesn't
/// exist" message; that's the right UX since we can't reasonably create then
/// drop tables for an analysis-only command.
#[cfg(feature = "mysql")]
async fn execute_mysql(client: &DbClient, config: &WaypointConfig) -> Result<ExplainReport> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let infos = info::execute_db(client, config).await?;

    let pending: Vec<_> = infos
        .iter()
        .filter(|i| matches!(i.state, MigrationState::Pending | MigrationState::Outdated))
        .collect();

    let schema = client.resolve_schema(&config.migrations.schema).await?;
    let db_user = client
        .current_user()
        .await
        .unwrap_or_else(|_| "unknown".into());
    let db_name = client
        .current_database()
        .await
        .unwrap_or_else(|_| "unknown".into());

    let resolved = crate::migration::scan_migrations(&config.migrations.locations)?;
    let mut migrations = Vec::new();

    for info in &pending {
        let migration = resolved.iter().find(|m| m.script == info.script);
        let sql = match migration {
            Some(m) => {
                let placeholders = build_placeholders(
                    &config.placeholders,
                    &schema,
                    &db_user,
                    &db_name,
                    &m.script,
                );
                replace_placeholders(&m.sql, &placeholders)?
            }
            None => continue,
        };

        let mut statements = Vec::new();
        let mut conn = pool.get_conn().await?;

        for stmt_str in crate::sql_parser::split_mysql_statements(&sql) {
            let trimmed = stmt_str.trim();
            if trimmed.is_empty() {
                continue;
            }

            let preview: String = trimmed.chars().take(80).collect();
            let preview = if trimmed.len() > 80 {
                format!("{}...", preview)
            } else {
                preview
            };

            let upper = trimmed.to_uppercase();
            let is_ddl = upper.starts_with("CREATE")
                || upper.starts_with("ALTER")
                || upper.starts_with("DROP")
                || upper.starts_with("TRUNCATE")
                || upper.starts_with("RENAME");

            if is_ddl {
                statements.push(StatementExplain {
                    statement_preview: preview,
                    plan: "DDL statement — not explainable".to_string(),
                    estimated_rows: None,
                    estimated_cost: None,
                    warnings: vec![],
                    is_ddl: true,
                });
            } else {
                let explain_sql = format!("EXPLAIN FORMAT=JSON {}", trimmed);
                match conn.query_first::<String, _>(&explain_sql).await {
                    Ok(Some(plan_json)) => {
                        let (rows, warnings) = extract_plan_info_mysql(&plan_json);
                        statements.push(StatementExplain {
                            statement_preview: preview,
                            plan: plan_json,
                            estimated_rows: rows,
                            estimated_cost: None, // MySQL EXPLAIN doesn't expose unified cost
                            warnings,
                            is_ddl: false,
                        });
                    }
                    Ok(None) => statements.push(StatementExplain {
                        statement_preview: preview,
                        plan: "EXPLAIN produced no rows".to_string(),
                        estimated_rows: None,
                        estimated_cost: None,
                        warnings: vec![],
                        is_ddl: false,
                    }),
                    Err(e) => statements.push(StatementExplain {
                        statement_preview: preview,
                        plan: format!("EXPLAIN failed: {}", e),
                        estimated_rows: None,
                        estimated_cost: None,
                        warnings: vec![],
                        is_ddl: false,
                    }),
                }
            }
        }

        migrations.push(MigrationExplain {
            script: info.script.clone(),
            version: info.version.clone(),
            statements,
        });
    }

    Ok(ExplainReport { migrations })
}

/// Extract row estimates and access-type warnings from a MySQL EXPLAIN
/// FORMAT=JSON plan. We do a coarse JSON-string search rather than parsing
/// into serde_json::Value because the plan structure varies across MySQL
/// versions and we only need a couple of signals.
#[cfg(feature = "mysql")]
fn extract_plan_info_mysql(plan: &str) -> (Option<f64>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut rows = None;

    // "rows_examined_per_scan": N  (MySQL 8.0 query plan)
    if let Some(idx) = plan.find("\"rows_examined_per_scan\":") {
        let after = &plan[idx + "\"rows_examined_per_scan\":".len()..];
        let after = after.trim_start();
        let end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        if let Ok(r) = after[..end].parse::<f64>() {
            rows = Some(r);
        }
    }

    // access_type = ALL means full table scan
    if plan.contains("\"access_type\": \"ALL\"") || plan.contains("\"access_type\":\"ALL\"") {
        let big_table = rows.map(|r| r > 10_000.0).unwrap_or(false);
        if big_table {
            warnings.push(format!(
                "Full table scan (~{:.0} rows) — consider adding an index",
                rows.unwrap_or(0.0)
            ));
        } else {
            warnings.push("Full table scan detected — consider adding an index".to_string());
        }
    }

    (rows, warnings)
}

fn extract_plan_info_text(plan_text: &str) -> (Option<f64>, Option<f64>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut total_rows = None;
    let mut total_cost = None;

    // Parse cost and rows from the first line: "Seq Scan on ... (cost=0.00..35.50 rows=2550 width=36)"
    for line in plan_text.lines() {
        let trimmed = line.trim();
        if let Some(cost_start) = trimmed.find("cost=") {
            let rest = &trimmed[cost_start + 5..];
            if let Some(dot_dot) = rest.find("..") {
                let after_dots = &rest[dot_dot + 2..];
                if let Some(space_pos) = after_dots.find(' ') {
                    if let Ok(cost) = after_dots[..space_pos].parse::<f64>() {
                        if total_cost.is_none() {
                            total_cost = Some(cost);
                        }
                    }
                }
            }
        }
        if let Some(rows_start) = trimmed.find("rows=") {
            let rest = &trimmed[rows_start + 5..];
            let end = rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len());
            if let Ok(rows) = rest[..end].parse::<f64>() {
                if total_rows.is_none() {
                    total_rows = Some(rows);
                }
            }
        }

        // Detect sequential scans
        if trimmed.contains("Seq Scan") {
            if let Some(rows) = total_rows {
                if rows > 10000.0 {
                    // Try to extract table name
                    let table = trimmed
                        .find("on ")
                        .map(|i| {
                            let after = &trimmed[i + 3..];
                            after.split_whitespace().next().unwrap_or("unknown")
                        })
                        .unwrap_or("unknown");
                    warnings.push(format!(
                        "Sequential Scan on '{}' (~{:.0} rows) — consider adding an index",
                        table, rows
                    ));
                }
            }
        }
    }

    (total_rows, total_cost, warnings)
}
