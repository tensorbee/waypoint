//! Multi-database orchestration.
//!
//! Allows managing migrations across multiple named databases with dependency
//! ordering between them. Supports mixed-engine deployments — one config can
//! mix `postgres://` and `mysql://` databases; the engine is auto-detected per
//! database from the URL scheme.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::Serialize;

use crate::config::{DatabaseConfig, HooksConfig, MigrationSettings, WaypointConfig};
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};

/// Configuration for a single named database within a multi-db setup.
#[derive(Debug, Clone)]
pub struct NamedDatabaseConfig {
    /// Unique logical name identifying this database.
    pub name: String,
    /// Database connection configuration.
    pub database: DatabaseConfig,
    /// Migration settings for this database.
    pub migrations: MigrationSettings,
    /// Hook configuration for this database.
    pub hooks: HooksConfig,
    /// Placeholder key-value pairs for SQL template substitution.
    pub placeholders: HashMap<String, String>,
    /// Names of other databases that must be migrated before this one.
    pub depends_on: Vec<String>,
}

impl NamedDatabaseConfig {
    /// Convert to a standalone WaypointConfig for running commands.
    pub fn to_waypoint_config(&self) -> WaypointConfig {
        WaypointConfig {
            database: self.database.clone(),
            migrations: self.migrations.clone(),
            hooks: self.hooks.clone(),
            placeholders: self.placeholders.clone(),
            ..WaypointConfig::default()
        }
    }
}

/// Multi-database orchestration entry point.
pub struct MultiWaypoint {
    /// List of all database configurations to orchestrate.
    pub databases: Vec<NamedDatabaseConfig>,
}

/// Result from a multi-db operation on a single database.
#[derive(Debug, Serialize)]
pub struct DatabaseResult {
    /// Logical name of the database.
    pub name: String,
    /// Whether the operation succeeded on this database.
    pub success: bool,
    /// Human-readable summary of the operation result.
    pub message: String,
}

/// Aggregate result from a multi-db operation.
#[derive(Debug, Serialize)]
pub struct MultiResult {
    /// Per-database operation results.
    pub results: Vec<DatabaseResult>,
    /// Whether every database operation succeeded.
    pub all_succeeded: bool,
}

impl MultiWaypoint {
    /// Determine execution order based on depends_on relationships (Kahn's algorithm).
    ///
    /// Uses borrowed `&str` references internally to avoid cloning database names
    /// during the topological sort; only clones into owned `String`s for the output.
    pub fn execution_order(databases: &[NamedDatabaseConfig]) -> Result<Vec<String>> {
        let all_names: HashSet<&str> = databases.iter().map(|d| d.name.as_str()).collect();

        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut reverse_edges: HashMap<&str, Vec<&str>> = HashMap::new();

        for db in databases {
            in_degree.entry(db.name.as_str()).or_insert(0);
            for dep in &db.depends_on {
                if !all_names.contains(dep.as_str()) {
                    return Err(WaypointError::DatabaseNotFound {
                        name: dep.clone(),
                        available: all_names.iter().copied().collect::<Vec<_>>().join(", "),
                    });
                }
                *in_degree.entry(db.name.as_str()).or_insert(0) += 1;
                reverse_edges
                    .entry(dep.as_str())
                    .or_default()
                    .push(db.name.as_str());
            }
        }

        let mut queue: VecDeque<&str> = VecDeque::new();
        for (&name, &deg) in &in_degree {
            if deg == 0 {
                queue.push_back(name);
            }
        }

        let mut sorted = Vec::new();
        while let Some(name) = queue.pop_front() {
            sorted.push(name.to_string());
            if let Some(dependents) = reverse_edges.get(name) {
                for &dep in dependents {
                    let deg = in_degree
                        .get_mut(dep)
                        .expect("dependency not found in in_degree map");
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep);
                    }
                }
            }
        }

        if sorted.len() != databases.len() {
            let in_cycle: Vec<&str> = in_degree
                .iter()
                .filter(|(_, deg)| **deg > 0)
                .map(|(&name, _)| name)
                .collect();
            return Err(WaypointError::MultiDbDependencyCycle {
                path: in_cycle.join(" -> "),
            });
        }

        Ok(sorted)
    }

    /// Connect to all databases (or a filtered subset). The engine for each
    /// database is auto-detected from the URL scheme — mixed PG/MySQL configs
    /// are fully supported here.
    pub async fn connect(
        databases: &[NamedDatabaseConfig],
        filter: Option<&str>,
    ) -> Result<HashMap<String, DbClient>> {
        let mut clients = HashMap::new();

        for db in databases {
            if let Some(name_filter) = filter {
                if db.name != name_filter {
                    continue;
                }
            }

            let config = db.to_waypoint_config();
            let conn_string = config.connection_string()?;
            let client = connect_one(&conn_string, &config).await?;
            clients.insert(db.name.clone(), client);
        }

        if let Some(name_filter) = filter {
            if !clients.contains_key(name_filter) {
                let available = databases
                    .iter()
                    .map(|d| d.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(WaypointError::DatabaseNotFound {
                    name: name_filter.to_string(),
                    available,
                });
            }
        }

        Ok(clients)
    }

    /// Run migrate on all databases in dependency order.
    pub async fn migrate(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, DbClient>,
        order: &[String],
        target_version: Option<&str>,
        fail_fast: bool,
    ) -> Result<MultiResult> {
        let mut results = Vec::new();

        for name in order {
            let db = databases.iter().find(|d| &d.name == name);
            let client = clients.get(name);

            match (db, client) {
                (Some(db), Some(client)) => {
                    let config = db.to_waypoint_config();
                    let outcome = dispatch_migrate(client, &config, target_version).await;
                    match outcome {
                        Ok(report) => {
                            results.push(DatabaseResult {
                                name: name.clone(),
                                success: true,
                                message: format!(
                                    "Applied {} migration(s) ({}ms)",
                                    report.migrations_applied, report.total_time_ms
                                ),
                            });
                        }
                        Err(e) => {
                            results.push(DatabaseResult {
                                name: name.clone(),
                                success: false,
                                message: format!("{}", e),
                            });
                            if fail_fast {
                                break;
                            }
                        }
                    }
                }
                _ => {
                    results.push(DatabaseResult {
                        name: name.clone(),
                        success: false,
                        message: "Database not connected".to_string(),
                    });
                    if fail_fast {
                        break;
                    }
                }
            }
        }

        let all_succeeded = results.iter().all(|r| r.success);
        Ok(MultiResult {
            results,
            all_succeeded,
        })
    }

    /// Run info on all databases in dependency order.
    pub async fn info(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, DbClient>,
        order: &[String],
    ) -> Result<HashMap<String, Vec<crate::commands::info::MigrationInfo>>> {
        let mut all_info = HashMap::new();

        for name in order {
            let db = databases.iter().find(|d| &d.name == name);
            let client = clients.get(name);

            if let (Some(db), Some(client)) = (db, client) {
                let config = db.to_waypoint_config();
                let info = crate::commands::info::execute_db(client, &config).await?;
                all_info.insert(name.clone(), info);
            }
        }

        Ok(all_info)
    }
}

/// Connect to one named database, auto-detecting the engine from the URL.
async fn connect_one(conn_string: &str, config: &WaypointConfig) -> Result<DbClient> {
    let kind = DialectKind::from_url(conn_string).unwrap_or(DialectKind::Postgres);
    match kind {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            let client = crate::db::connect_with_full_config(
                conn_string,
                &config.database.ssl_mode,
                config.database.connect_retries,
                config.database.connect_timeout_secs,
                config.database.statement_timeout_secs,
                config.database.keepalive_secs,
            )
            .await?;
            Ok(DbClient::with_postgres(client))
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            let pool = mysql_async::Pool::from_url(conn_string)
                .map_err(|e| WaypointError::ConfigError(format!("Invalid MySQL URL: {}", e)))?;
            Ok(DbClient::with_mysql(pool))
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

/// Dispatch migrate to the appropriate engine-specific implementation.
async fn dispatch_migrate(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
) -> Result<crate::commands::migrate::MigrateReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            crate::commands::migrate::execute(client.as_postgres()?, config, target_version).await
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            crate::commands::migrate::execute_mysql(client, config, target_version).await
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}
