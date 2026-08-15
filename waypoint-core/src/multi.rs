//! Multi-database orchestration.
//!
//! Allows managing migrations across multiple named databases with dependency
//! ordering between them. Supports mixed-engine deployments — one config can
//! mix `postgres://` and `mysql://` databases; the engine is auto-detected per
//! database from the URL scheme.

use std::collections::{HashMap, HashSet};

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
    /// Convert to a standalone `WaypointConfig` for running commands.
    ///
    /// Everything not declared per-database falls back to built-in defaults.
    /// Prefer [`Self::to_waypoint_config_inheriting`], which carries the
    /// top-level `[safety]`, `[preflight]`, `[guards]`, `[reversals]`,
    /// `[advisor]`, `[snapshots]`, `[lint]` and `[simulation]` sections over
    /// instead of silently dropping them.
    pub fn to_waypoint_config(&self) -> WaypointConfig {
        self.to_waypoint_config_inheriting(&WaypointConfig::default())
    }

    /// Convert to a standalone `WaypointConfig`, inheriting the global
    /// sections from `parent`.
    ///
    /// A `[[databases]]` entry only carries connection, migration, hook and
    /// placeholder settings. Every other section is process-wide policy — a
    /// `[safety] block_on_danger = true` must not stop applying just because
    /// the run happens to be multi-database.
    pub fn to_waypoint_config_inheriting(&self, parent: &WaypointConfig) -> WaypointConfig {
        WaypointConfig {
            database: self.database.clone(),
            migrations: self.migrations.clone(),
            hooks: self.hooks.clone(),
            placeholders: self.placeholders.clone(),
            // Inherited global policy.
            lint: parent.lint.clone(),
            snapshots: parent.snapshots.clone(),
            preflight: parent.preflight.clone(),
            guards: parent.guards.clone(),
            reversals: parent.reversals.clone(),
            safety: parent.safety.clone(),
            advisor: parent.advisor.clone(),
            simulation: parent.simulation.clone(),
            // Never inherited: a nested multi-database list would recurse.
            multi_database: None,
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
    /// Databases that are ready at the same moment run in **declaration order**
    /// — the order they appear in `[[databases]]` — which makes the result
    /// deterministic.
    ///
    /// The ready set used to be seeded by iterating a `HashMap`, whose order is
    /// randomly seeded per process, so three independent databases migrated in
    /// a different order on almost every run. Any topological order is correct,
    /// but a varying one makes `--fail-fast` leave a different subset migrated
    /// each time, and makes staging and production disagree for no reason.
    ///
    /// Uses borrowed `&str` references internally to avoid cloning database names
    /// during the topological sort; only clones into owned `String`s for the output.
    pub fn execution_order(databases: &[NamedDatabaseConfig]) -> Result<Vec<String>> {
        let all_names: HashSet<&str> = databases.iter().map(|d| d.name.as_str()).collect();
        // Declaration order, used both to break ties and to keep the
        // "available databases" error message stable.
        let declared: Vec<&str> = databases.iter().map(|d| d.name.as_str()).collect();
        let rank: HashMap<&str, usize> = declared
            .iter()
            .enumerate()
            .map(|(i, &name)| (name, i))
            .collect();

        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut reverse_edges: HashMap<&str, Vec<&str>> = HashMap::new();

        for db in databases {
            in_degree.entry(db.name.as_str()).or_insert(0);
            for dep in &db.depends_on {
                if !all_names.contains(dep.as_str()) {
                    return Err(WaypointError::DatabaseNotFound {
                        name: dep.clone(),
                        available: declared.join(", "),
                    });
                }
                *in_degree.entry(db.name.as_str()).or_insert(0) += 1;
                reverse_edges
                    .entry(dep.as_str())
                    .or_default()
                    .push(db.name.as_str());
            }
        }

        let mut ready: std::collections::BTreeSet<(usize, &str)> = declared
            .iter()
            .filter(|name| in_degree.get(*name).copied().unwrap_or(0) == 0)
            .map(|&name| (rank[name], name))
            .collect();

        let mut sorted = Vec::new();
        while let Some(&(_, name)) = ready.iter().next() {
            ready.remove(&(rank[name], name));
            sorted.push(name.to_string());
            if let Some(dependents) = reverse_edges.get(name) {
                for &dep in dependents {
                    let deg = in_degree
                        .get_mut(dep)
                        .expect("dependency not found in in_degree map");
                    *deg -= 1;
                    if *deg == 0 {
                        ready.insert((rank[dep], dep));
                    }
                }
            }
        }

        if sorted.len() != databases.len() {
            // Declaration order again, so the reported cycle is the same on
            // every run rather than a fresh permutation each time.
            let in_cycle: Vec<&str> = declared
                .iter()
                .copied()
                .filter(|name| in_degree.get(name).copied().unwrap_or(0) > 0)
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
    ///
    /// Uses built-in defaults for the global config sections. Prefer
    /// [`Self::connect_inheriting`] so that top-level `[database]` transport
    /// settings apply.
    pub async fn connect(
        databases: &[NamedDatabaseConfig],
        filter: Option<&str>,
    ) -> Result<HashMap<String, DbClient>> {
        Self::connect_inheriting(databases, filter, &WaypointConfig::default()).await
    }

    /// Like [`Self::connect`], but inheriting global sections from `parent`.
    pub async fn connect_inheriting(
        databases: &[NamedDatabaseConfig],
        filter: Option<&str>,
        parent: &WaypointConfig,
    ) -> Result<HashMap<String, DbClient>> {
        let mut clients = HashMap::new();

        for db in databases {
            if let Some(name_filter) = filter
                && db.name != name_filter
            {
                continue;
            }

            let config = db.to_waypoint_config_inheriting(parent);
            let conn_string = config.connection_string()?;
            let client = crate::db::connect_for_url(&conn_string, &config).await?;
            clients.insert(db.name.clone(), client);
        }

        if let Some(name_filter) = filter
            && !clients.contains_key(name_filter)
        {
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
        Self::migrate_with_options(databases, clients, order, target_version, fail_fast, false)
            .await
    }

    /// Run migrate on all databases in dependency order with the `force`
    /// flag for overriding DANGER safety verdicts on PostgreSQL.
    ///
    /// Uses built-in defaults for the global config sections. Prefer
    /// [`Self::migrate_inheriting`] so that top-level `[safety]`,
    /// `[preflight]`, `[guards]` and `[reversals]` policy applies.
    pub async fn migrate_with_options(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, DbClient>,
        order: &[String],
        target_version: Option<&str>,
        fail_fast: bool,
        force: bool,
    ) -> Result<MultiResult> {
        Self::migrate_inheriting(
            databases,
            clients,
            order,
            target_version,
            fail_fast,
            force,
            &WaypointConfig::default(),
        )
        .await
    }

    /// Like [`Self::migrate_with_options`], but inheriting global config
    /// sections from `parent`.
    #[allow(clippy::too_many_arguments)]
    pub async fn migrate_inheriting(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, DbClient>,
        order: &[String],
        target_version: Option<&str>,
        fail_fast: bool,
        force: bool,
        parent: &WaypointConfig,
    ) -> Result<MultiResult> {
        let mut results = Vec::new();

        for name in order {
            let db = databases.iter().find(|d| &d.name == name);
            let client = clients.get(name);

            match (db, client) {
                (Some(db), Some(client)) => {
                    let config = db.to_waypoint_config_inheriting(parent);
                    let outcome = dispatch_migrate(client, &config, target_version, force).await;
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
        Self::info_inheriting(databases, clients, order, &WaypointConfig::default()).await
    }

    /// Like [`Self::info`], but inheriting global config sections from `parent`.
    pub async fn info_inheriting(
        databases: &[NamedDatabaseConfig],
        clients: &HashMap<String, DbClient>,
        order: &[String],
        parent: &WaypointConfig,
    ) -> Result<HashMap<String, Vec<crate::commands::info::MigrationInfo>>> {
        let mut all_info = HashMap::new();

        for name in order {
            let db = databases.iter().find(|d| &d.name == name);
            let client = clients.get(name);

            if let (Some(db), Some(client)) = (db, client) {
                let config = db.to_waypoint_config_inheriting(parent);
                let info = crate::commands::info::execute_db(client, &config).await?;
                all_info.insert(name.clone(), info);
            }
        }

        Ok(all_info)
    }
}

/// Dispatch migrate to the appropriate engine-specific implementation.
async fn dispatch_migrate(
    client: &DbClient,
    config: &WaypointConfig,
    target_version: Option<&str>,
    force: bool,
) -> Result<crate::commands::migrate::MigrateReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            crate::commands::migrate::execute_with_options(
                client.as_postgres()?,
                config,
                target_version,
                force,
            )
            .await
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            crate::commands::migrate::execute_mysql_with_options(
                client,
                config,
                target_version,
                force,
            )
            .await
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatabaseConfig, HooksConfig, MigrationSettings};

    fn db(name: &str, depends_on: &[&str]) -> NamedDatabaseConfig {
        NamedDatabaseConfig {
            name: name.to_string(),
            database: DatabaseConfig::default(),
            migrations: MigrationSettings::default(),
            hooks: HooksConfig::default(),
            placeholders: std::collections::HashMap::new(),
            depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_execution_order_is_deterministic_for_independent_databases() {
        // The ready set used to be seeded by iterating a HashMap, so these
        // migrated in a different order on almost every process.
        let dbs = vec![
            db("alpha", &[]),
            db("bravo", &[]),
            db("charlie", &[]),
            db("delta", &[]),
        ];
        let order = MultiWaypoint::execution_order(&dbs).unwrap();
        assert_eq!(
            order,
            vec!["alpha", "bravo", "charlie", "delta"],
            "independent databases must run in declaration order"
        );
    }

    #[test]
    fn test_execution_order_respects_dependencies_then_declaration_order() {
        // `app` depends on `auth`, so it runs after it despite being declared
        // first; the two independent databases keep declaration order.
        let dbs = vec![db("app", &["auth"]), db("reports", &[]), db("auth", &[])];
        let order = MultiWaypoint::execution_order(&dbs).unwrap();
        assert_eq!(order, vec!["reports", "auth", "app"]);
    }

    #[test]
    fn test_execution_order_reports_a_missing_dependency_stably() {
        let dbs = vec![db("app", &["nope"]), db("auth", &[]), db("reports", &[])];
        let err = MultiWaypoint::execution_order(&dbs).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nope"), "{msg}");
        // Declaration order, not hash order, so the message is reproducible.
        assert!(msg.contains("app, auth, reports"), "{msg}");
    }
}
