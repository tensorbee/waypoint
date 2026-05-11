//! Pre-flight health checks run before migrations.
//!
//! Checks database health metrics like recovery mode, active connections,
//! long-running queries, replication lag, and lock contention.

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::Result;
#[cfg(any(not(feature = "postgres"), not(feature = "mysql")))]
use crate::error::WaypointError;

/// Result of a single pre-flight check.
#[derive(Debug, Clone, Serialize)]
pub struct PreflightCheck {
    /// Human-readable name of the check (e.g. "Recovery Mode").
    pub name: String,
    /// Whether the check passed, warned, or failed.
    pub status: CheckStatus,
    /// Descriptive detail about the check result.
    pub detail: String,
}

/// Status of a pre-flight check.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum CheckStatus {
    /// The check passed successfully.
    Pass,
    /// The check produced a non-blocking warning.
    Warn,
    /// The check failed and should block migration.
    Fail,
}

impl std::fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckStatus::Pass => write!(f, "PASS"),
            CheckStatus::Warn => write!(f, "WARN"),
            CheckStatus::Fail => write!(f, "FAIL"),
        }
    }
}

/// Aggregate report of all pre-flight checks.
#[derive(Debug, Serialize)]
pub struct PreflightReport {
    /// Individual check results.
    pub checks: Vec<PreflightCheck>,
    /// Whether all checks passed (no failures).
    pub passed: bool,
}

/// Configuration for pre-flight checks.
#[derive(Debug, Clone)]
pub struct PreflightConfig {
    /// Whether pre-flight checks are enabled before migrations.
    pub enabled: bool,
    /// Maximum acceptable replication lag in megabytes before warning.
    pub max_replication_lag_mb: i64,
    /// Threshold in seconds for detecting long-running queries.
    pub long_query_threshold_secs: i64,
}

impl Default for PreflightConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_replication_lag_mb: 100,
            long_query_threshold_secs: 300,
        }
    }
}

/// Run all pre-flight checks against the database (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn run_preflight(client: &Client, config: &PreflightConfig) -> Result<PreflightReport> {
    let mut checks = Vec::new();

    checks.push(check_recovery_mode(client).await);
    checks.push(check_active_connections(client).await);
    checks.push(check_long_running_queries(client, config.long_query_threshold_secs).await);
    checks.push(check_replication_lag(client, config.max_replication_lag_mb).await);
    checks.push(check_database_size(client).await);
    checks.push(check_lock_contention(client).await);

    let passed = !checks.iter().any(|c| c.status == CheckStatus::Fail);

    Ok(PreflightReport { checks, passed })
}

/// Run all pre-flight checks against the database (dialect-aware entry).
pub async fn run_preflight_db(
    client: &DbClient,
    config: &PreflightConfig,
) -> Result<PreflightReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => run_preflight(client.as_postgres()?, config).await,
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => run_preflight_mysql(client, config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

#[cfg(feature = "postgres")]
async fn check_recovery_mode(client: &Client) -> PreflightCheck {
    match client.query_one("SELECT pg_is_in_recovery()", &[]).await {
        Ok(row) => {
            let in_recovery: bool = row.get(0);
            if in_recovery {
                PreflightCheck {
                    name: "Recovery Mode".to_string(),
                    status: CheckStatus::Fail,
                    detail: "Database is in recovery mode (read-only replica)".to_string(),
                }
            } else {
                PreflightCheck {
                    name: "Recovery Mode".to_string(),
                    status: CheckStatus::Pass,
                    detail: "Not in recovery mode".to_string(),
                }
            }
        }
        Err(e) => PreflightCheck {
            name: "Recovery Mode".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

#[cfg(feature = "postgres")]
async fn check_active_connections(client: &Client) -> PreflightCheck {
    let query = "SELECT count(*)::int as active,
                        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_conn
                 FROM pg_stat_activity";
    match client.query_one(query, &[]).await {
        Ok(row) => {
            let active: i32 = row.get(0);
            let max_conn: i32 = row.get(1);
            let pct = (active as f64 / max_conn as f64) * 100.0;
            let status = if pct >= 80.0 {
                CheckStatus::Warn
            } else {
                CheckStatus::Pass
            };
            PreflightCheck {
                name: "Active Connections".to_string(),
                status,
                detail: format!("{}/{} ({:.0}%)", active, max_conn, pct),
            }
        }
        Err(e) => PreflightCheck {
            name: "Active Connections".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

#[cfg(feature = "postgres")]
async fn check_long_running_queries(client: &Client, threshold_secs: i64) -> PreflightCheck {
    let query = format!(
        "SELECT count(*)::int FROM pg_stat_activity
         WHERE state = 'active' AND now() - query_start > interval '{} seconds'",
        threshold_secs
    );
    match client.query_one(&query, &[]).await {
        Ok(row) => {
            let count: i32 = row.get(0);
            if count > 0 {
                PreflightCheck {
                    name: "Long-Running Queries".to_string(),
                    status: CheckStatus::Warn,
                    detail: format!(
                        "{} query(ies) running longer than {}s",
                        count, threshold_secs
                    ),
                }
            } else {
                PreflightCheck {
                    name: "Long-Running Queries".to_string(),
                    status: CheckStatus::Pass,
                    detail: format!("No queries running longer than {}s", threshold_secs),
                }
            }
        }
        Err(e) => PreflightCheck {
            name: "Long-Running Queries".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

#[cfg(feature = "postgres")]
async fn check_replication_lag(client: &Client, max_lag_mb: i64) -> PreflightCheck {
    let query = "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)
                 FROM pg_stat_replication
                 ORDER BY replay_lsn ASC LIMIT 1";
    match client.query_opt(query, &[]).await {
        Ok(Some(row)) => {
            let lag_bytes: Option<i64> = row.get(0);
            let lag_mb = lag_bytes.unwrap_or(0) / (1024 * 1024);
            let status = if lag_mb > max_lag_mb {
                CheckStatus::Warn
            } else {
                CheckStatus::Pass
            };
            PreflightCheck {
                name: "Replication Lag".to_string(),
                status,
                detail: format!("{}MB (threshold: {}MB)", lag_mb, max_lag_mb),
            }
        }
        Ok(None) => PreflightCheck {
            name: "Replication Lag".to_string(),
            status: CheckStatus::Pass,
            detail: "No replicas connected".to_string(),
        },
        Err(_) => PreflightCheck {
            name: "Replication Lag".to_string(),
            status: CheckStatus::Pass,
            detail: "Not a primary or no replication configured".to_string(),
        },
    }
}

#[cfg(feature = "postgres")]
async fn check_database_size(client: &Client) -> PreflightCheck {
    match client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await
    {
        Ok(row) => {
            let size_bytes: i64 = row.get(0);
            let size_mb = size_bytes / (1024 * 1024);
            let detail = if size_mb > 1024 {
                format!("{:.1}GB", size_mb as f64 / 1024.0)
            } else {
                format!("{}MB", size_mb)
            };
            PreflightCheck {
                name: "Database Size".to_string(),
                status: CheckStatus::Pass,
                detail,
            }
        }
        Err(e) => PreflightCheck {
            name: "Database Size".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

#[cfg(feature = "postgres")]
async fn check_lock_contention(client: &Client) -> PreflightCheck {
    match client
        .query_one("SELECT count(*)::int FROM pg_locks WHERE NOT granted", &[])
        .await
    {
        Ok(row) => {
            let blocked: i32 = row.get(0);
            if blocked > 0 {
                PreflightCheck {
                    name: "Lock Contention".to_string(),
                    status: CheckStatus::Warn,
                    detail: format!("{} blocked lock request(s)", blocked),
                }
            } else {
                PreflightCheck {
                    name: "Lock Contention".to_string(),
                    status: CheckStatus::Pass,
                    detail: "No blocked locks".to_string(),
                }
            }
        }
        Err(e) => PreflightCheck {
            name: "Lock Contention".to_string(),
            status: CheckStatus::Warn,
            detail: format!("Could not check: {}", e),
        },
    }
}

// ── MySQL pre-flight checks ───────────────────────────────────────────────────

#[cfg(feature = "mysql")]
async fn run_preflight_mysql(
    client: &DbClient,
    config: &PreflightConfig,
) -> Result<PreflightReport> {
    let mut checks = Vec::new();
    checks.push(check_read_only_mysql(client).await);
    checks.push(check_active_connections_mysql(client).await);
    checks.push(check_long_running_queries_mysql(client, config.long_query_threshold_secs).await);
    checks.push(check_replication_lag_mysql(client, config.max_replication_lag_mb).await);
    checks.push(check_database_size_mysql(client).await);
    checks.push(check_lock_contention_mysql(client).await);

    let passed = !checks.iter().any(|c| c.status == CheckStatus::Fail);
    Ok(PreflightReport { checks, passed })
}

#[cfg(feature = "mysql")]
async fn check_read_only_mysql(client: &DbClient) -> PreflightCheck {
    use mysql_async::prelude::*;
    let pool = match client.as_mysql() {
        Ok(p) => p,
        Err(e) => {
            return PreflightCheck {
                name: "Read-only".into(),
                status: CheckStatus::Warn,
                detail: format!("Could not check: {}", e),
            };
        }
    };
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            return PreflightCheck {
                name: "Read-only".into(),
                status: CheckStatus::Warn,
                detail: format!("Could not check: {}", e),
            };
        }
    };
    // Treat @@read_only as the canonical signal that this is a replica or
    // intentionally locked down. super_read_only is even stricter (8.0+).
    match conn
        .query_first::<(i64, i64), _>("SELECT @@read_only, @@super_read_only")
        .await
    {
        Ok(Some((read_only, super_read_only))) => {
            if read_only != 0 || super_read_only != 0 {
                PreflightCheck {
                    name: "Read-only".into(),
                    status: CheckStatus::Fail,
                    detail: format!(
                        "Server is read-only (read_only={}, super_read_only={})",
                        read_only, super_read_only
                    ),
                }
            } else {
                PreflightCheck {
                    name: "Read-only".into(),
                    status: CheckStatus::Pass,
                    detail: "Server accepts writes".into(),
                }
            }
        }
        Ok(None) | Err(_) => PreflightCheck {
            name: "Read-only".into(),
            status: CheckStatus::Warn,
            detail: "Could not determine read-only state".into(),
        },
    }
}

#[cfg(feature = "mysql")]
async fn check_active_connections_mysql(client: &DbClient) -> PreflightCheck {
    use mysql_async::prelude::*;
    let pool = client.as_mysql().expect("mysql pool");
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            return PreflightCheck {
                name: "Active Connections".into(),
                status: CheckStatus::Warn,
                detail: format!("Could not check: {}", e),
            };
        }
    };
    // performance_schema.global_status / global_variables expose these without
    // SUPER privilege on most installs; fall back to SHOW STATUS if needed.
    let active: Option<i64> = conn
        .query_first(
            "SELECT VARIABLE_VALUE + 0 FROM performance_schema.global_status \
             WHERE VARIABLE_NAME = 'Threads_connected'",
        )
        .await
        .unwrap_or(None);
    let max_conn: Option<i64> = conn
        .query_first("SELECT @@max_connections")
        .await
        .unwrap_or(None);
    match (active, max_conn) {
        (Some(a), Some(m)) if m > 0 => {
            let pct = (a as f64 / m as f64) * 100.0;
            let status = if pct >= 80.0 {
                CheckStatus::Warn
            } else {
                CheckStatus::Pass
            };
            PreflightCheck {
                name: "Active Connections".into(),
                status,
                detail: format!("{}/{} ({:.0}%)", a, m, pct),
            }
        }
        _ => PreflightCheck {
            name: "Active Connections".into(),
            status: CheckStatus::Warn,
            detail: "Could not read connection stats".into(),
        },
    }
}

#[cfg(feature = "mysql")]
async fn check_long_running_queries_mysql(
    client: &DbClient,
    threshold_secs: i64,
) -> PreflightCheck {
    use mysql_async::prelude::*;
    let pool = client.as_mysql().expect("mysql pool");
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            return PreflightCheck {
                name: "Long-Running Queries".into(),
                status: CheckStatus::Warn,
                detail: format!("Could not check: {}", e),
            };
        }
    };
    // information_schema.PROCESSLIST.TIME is "seconds since the thread entered
    // its current state". Sleeping threads aren't running queries.
    let count: Option<i64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST \
             WHERE COMMAND <> 'Sleep' AND TIME > ?",
            (threshold_secs,),
        )
        .await
        .unwrap_or(None);
    match count {
        Some(c) if c > 0 => PreflightCheck {
            name: "Long-Running Queries".into(),
            status: CheckStatus::Warn,
            detail: format!("{} query(ies) running longer than {}s", c, threshold_secs),
        },
        Some(_) => PreflightCheck {
            name: "Long-Running Queries".into(),
            status: CheckStatus::Pass,
            detail: format!("No queries running longer than {}s", threshold_secs),
        },
        None => PreflightCheck {
            name: "Long-Running Queries".into(),
            status: CheckStatus::Warn,
            detail: "Could not read PROCESSLIST".into(),
        },
    }
}

#[cfg(feature = "mysql")]
async fn check_replication_lag_mysql(client: &DbClient, max_lag_mb: i64) -> PreflightCheck {
    use mysql_async::prelude::*;
    // We measure lag in seconds (MySQL's natural unit) and apply the configured
    // limit by treating MB as an approximate seconds value — caller can tune
    // max_replication_lag_mb to taste. We don't pretend the units match PG.
    let max_lag_secs = max_lag_mb;
    let pool = client.as_mysql().expect("mysql pool");
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(_) => {
            return PreflightCheck {
                name: "Replication Lag".into(),
                status: CheckStatus::Pass,
                detail: "Could not check (treating as primary)".into(),
            };
        }
    };
    // SHOW REPLICA STATUS requires REPLICATION CLIENT. We try, and on error
    // assume this is a primary or non-replica.
    let row: Option<mysql_async::Row> = conn
        .query_first("SHOW REPLICA STATUS")
        .await
        .unwrap_or(None);
    match row {
        None => PreflightCheck {
            name: "Replication Lag".into(),
            status: CheckStatus::Pass,
            detail: "Not a replica".into(),
        },
        Some(mut r) => {
            // Seconds_Behind_Source is NULL when replication isn't running.
            let lag: Option<i64> = r.take("Seconds_Behind_Source").unwrap_or(None);
            match lag {
                Some(secs) => {
                    let status = if secs > max_lag_secs {
                        CheckStatus::Warn
                    } else {
                        CheckStatus::Pass
                    };
                    PreflightCheck {
                        name: "Replication Lag".into(),
                        status,
                        detail: format!("{}s (threshold: {}s)", secs, max_lag_secs),
                    }
                }
                None => PreflightCheck {
                    name: "Replication Lag".into(),
                    status: CheckStatus::Warn,
                    detail: "Replication thread not running".into(),
                },
            }
        }
    }
}

#[cfg(feature = "mysql")]
async fn check_database_size_mysql(client: &DbClient) -> PreflightCheck {
    use mysql_async::prelude::*;
    let pool = client.as_mysql().expect("mysql pool");
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            return PreflightCheck {
                name: "Database Size".into(),
                status: CheckStatus::Warn,
                detail: format!("Could not check: {}", e),
            };
        }
    };
    let db: Option<Option<String>> = conn.query_first("SELECT DATABASE()").await.unwrap_or(None);
    let db = match db.flatten() {
        Some(d) => d,
        None => {
            return PreflightCheck {
                name: "Database Size".into(),
                status: CheckStatus::Warn,
                detail: "No current database selected".into(),
            };
        }
    };
    let size: Option<i64> = conn
        .exec_first(
            "SELECT IFNULL(SUM(data_length + index_length), 0) \
             FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (db.as_str(),),
        )
        .await
        .unwrap_or(None);
    match size {
        Some(bytes) => {
            let mb = bytes / (1024 * 1024);
            let detail = if mb > 1024 {
                format!("{:.1}GB", mb as f64 / 1024.0)
            } else {
                format!("{}MB", mb)
            };
            PreflightCheck {
                name: "Database Size".into(),
                status: CheckStatus::Pass,
                detail,
            }
        }
        None => PreflightCheck {
            name: "Database Size".into(),
            status: CheckStatus::Warn,
            detail: "Could not compute size".into(),
        },
    }
}

#[cfg(feature = "mysql")]
async fn check_lock_contention_mysql(client: &DbClient) -> PreflightCheck {
    use mysql_async::prelude::*;
    let pool = client.as_mysql().expect("mysql pool");
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            return PreflightCheck {
                name: "Lock Contention".into(),
                status: CheckStatus::Warn,
                detail: format!("Could not check: {}", e),
            };
        }
    };
    // performance_schema.metadata_locks needs performance_schema enabled (on
    // by default in MySQL 8.0). PENDING rows indicate waiters.
    let pending: Option<i64> = conn
        .query_first(
            "SELECT COUNT(*) FROM performance_schema.metadata_locks \
             WHERE LOCK_STATUS = 'PENDING'",
        )
        .await
        .unwrap_or(None);
    match pending {
        Some(p) if p > 0 => PreflightCheck {
            name: "Lock Contention".into(),
            status: CheckStatus::Warn,
            detail: format!("{} pending metadata lock(s)", p),
        },
        Some(_) => PreflightCheck {
            name: "Lock Contention".into(),
            status: CheckStatus::Pass,
            detail: "No pending locks".into(),
        },
        None => PreflightCheck {
            name: "Lock Contention".into(),
            status: CheckStatus::Warn,
            detail: "Could not query metadata_locks".into(),
        },
    }
}
