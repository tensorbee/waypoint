//! Configuration loading and resolution.
//!
//! Supports TOML config files, environment variables, and CLI overrides
//! with a defined priority order (CLI > env > TOML > defaults).

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{Result, WaypointError};

/// Helper macro to apply an optional owned value directly to a target field.
///
/// Replaces: `if let Some(v) = $opt { $target = v; }`
macro_rules! apply_option {
    ($opt:expr => $target:expr) => {
        if let Some(v) = $opt {
            $target = v;
        }
    };
}

/// Helper macro to apply an optional owned value, wrapping it in `Some()`.
///
/// Replaces: `if let Some(v) = $opt { $target = Some(v); }`
macro_rules! apply_option_some {
    ($opt:expr => $target:expr) => {
        if let Some(v) = $opt {
            $target = Some(v);
        }
    };
}

/// Helper macro to clone a borrowed optional value directly to a target field.
///
/// Replaces: `if let Some(ref v) = $opt { $target = v.clone(); }`
macro_rules! apply_option_clone {
    ($opt:expr => $target:expr) => {
        if let Some(ref v) = $opt {
            $target = v.clone();
        }
    };
}

/// Helper macro to clone a borrowed optional value, wrapping it in `Some()`.
///
/// Replaces: `if let Some(ref v) = $opt { $target = Some(v.clone()); }`
macro_rules! apply_option_some_clone {
    ($opt:expr => $target:expr) => {
        if let Some(ref v) = $opt {
            $target = Some(v.clone());
        }
    };
}

/// SSL/TLS connection mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SslMode {
    /// Never use TLS (current default behavior).
    Disable,
    /// Try TLS first, fall back to plaintext.
    #[default]
    Prefer,
    /// Require TLS — fail if handshake fails.
    Require,
}

impl std::str::FromStr for SslMode {
    type Err = WaypointError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" | "disabled" => Ok(SslMode::Disable),
            "prefer" => Ok(SslMode::Prefer),
            "require" | "required" => Ok(SslMode::Require),
            _ => Err(WaypointError::ConfigError(format!(
                "Invalid SSL mode '{}'. Use 'disable', 'prefer', or 'require'.",
                s
            ))),
        }
    }
}

/// Top-level configuration for Waypoint.
#[derive(Debug, Clone, Default)]
pub struct WaypointConfig {
    /// Database connection settings (URL, host, port, credentials, etc.).
    pub database: DatabaseConfig,
    /// Migration behavior settings (locations, table name, ordering, etc.).
    pub migrations: MigrationSettings,
    /// SQL callback hook configuration for before/after migration phases.
    pub hooks: HooksConfig,
    /// Key-value placeholder substitutions applied to migration SQL.
    pub placeholders: HashMap<String, String>,
    /// Lint rule configuration.
    pub lint: LintConfig,
    /// Schema snapshot configuration for drift detection.
    pub snapshots: crate::commands::snapshot::SnapshotConfig,
    /// Pre-flight check configuration run before migrations.
    pub preflight: crate::preflight::PreflightConfig,
    /// Optional multi-database configuration for parallel migration targets.
    pub multi_database: Option<Vec<crate::multi::NamedDatabaseConfig>>,
    /// Guard (pre/post condition) configuration.
    pub guards: crate::guard::GuardsConfig,
    /// Auto-reversal generation configuration.
    pub reversals: crate::reversal::ReversalConfig,
    /// Safety analysis configuration.
    pub safety: crate::safety::SafetyConfig,
    /// Schema advisor configuration.
    pub advisor: crate::advisor::AdvisorConfig,
    /// Migration simulation configuration.
    pub simulation: SimulationConfig,
}

/// Database connection configuration.
#[derive(Clone)]
pub struct DatabaseConfig {
    /// Full connection URL (e.g., `postgres://user:pass@host/db`).
    pub url: Option<String>,
    /// Database server hostname.
    pub host: Option<String>,
    /// Database server port number.
    pub port: Option<u16>,
    /// Database user for authentication.
    pub user: Option<String>,
    /// Database password for authentication.
    pub password: Option<String>,
    /// Database name to connect to.
    pub database: Option<String>,
    /// Number of times to retry a failed connection (max 20).
    pub connect_retries: u32,
    /// SSL/TLS mode for the database connection.
    pub ssl_mode: SslMode,
    /// Connection timeout in seconds.
    pub connect_timeout_secs: u32,
    /// Statement timeout in seconds (0 means no timeout).
    pub statement_timeout_secs: u32,
    /// TCP keepalive interval in seconds (0 disables, default 120).
    pub keepalive_secs: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: None,
            host: None,
            port: None,
            user: None,
            password: None,
            database: None,
            connect_retries: 0,
            ssl_mode: SslMode::Prefer,
            connect_timeout_secs: 30,
            statement_timeout_secs: 0,
            keepalive_secs: 120,
        }
    }
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("url", &self.url.as_ref().map(|_| "[REDACTED]"))
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("database", &self.database)
            .field("connect_retries", &self.connect_retries)
            .field("ssl_mode", &self.ssl_mode)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("keepalive_secs", &self.keepalive_secs)
            .finish()
    }
}

/// Hook configuration for running SQL before/after migrations.
#[derive(Debug, Clone, Default)]
pub struct HooksConfig {
    /// SQL scripts to run once before the entire migration run.
    pub before_migrate: Vec<PathBuf>,
    /// SQL scripts to run once after the entire migration run.
    pub after_migrate: Vec<PathBuf>,
    /// SQL scripts to run before each individual migration.
    pub before_each_migrate: Vec<PathBuf>,
    /// SQL scripts to run after each individual migration.
    pub after_each_migrate: Vec<PathBuf>,
}

/// Lint configuration.
#[derive(Debug, Clone, Default)]
pub struct LintConfig {
    /// List of lint rule names to disable.
    pub disabled_rules: Vec<String>,
}

/// Migration behavior settings.
#[derive(Debug, Clone)]
pub struct MigrationSettings {
    /// Filesystem directories to scan for migration SQL files.
    pub locations: Vec<PathBuf>,
    /// Name of the schema history table.
    pub table: String,
    /// Database schema where the history table resides.
    pub schema: String,
    /// Whether to allow applying migrations with versions below the highest applied version.
    pub out_of_order: bool,
    /// Whether to validate already-applied migration checksums before migrating.
    pub validate_on_migrate: bool,
    /// Whether the `clean` command is allowed to run.
    pub clean_enabled: bool,
    /// Version to use when running the `baseline` command.
    pub baseline_version: String,
    /// Custom value for the `installed_by` column (defaults to database user).
    pub installed_by: Option<String>,
    /// Logical environment name (e.g., "production", "staging") for filtering.
    pub environment: Option<String>,
    /// Whether to use `@depends` directives to order migrations topologically.
    pub dependency_ordering: bool,
    /// Whether to display a progress indicator during migration.
    pub show_progress: bool,
    /// Whether to wrap all pending migrations in a single transaction (all-or-nothing).
    pub batch_transaction: bool,
}

impl Default for MigrationSettings {
    fn default() -> Self {
        Self {
            locations: vec![PathBuf::from("db/migrations")],
            table: "waypoint_schema_history".to_string(),
            schema: "public".to_string(),
            out_of_order: false,
            validate_on_migrate: true,
            clean_enabled: false,
            baseline_version: "1".to_string(),
            installed_by: None,
            environment: None,
            dependency_ordering: false,
            show_progress: true,
            batch_transaction: false,
        }
    }
}

/// Migration simulation configuration.
#[derive(Debug, Clone, Default)]
pub struct SimulationConfig {
    /// Whether to run simulation before migrate.
    pub simulate_before_migrate: bool,
}

// ── TOML deserialization structs ──

#[derive(Deserialize, Default)]
struct TomlConfig {
    database: Option<TomlDatabaseConfig>,
    migrations: Option<TomlMigrationSettings>,
    hooks: Option<TomlHooksConfig>,
    placeholders: Option<HashMap<String, String>>,
    lint: Option<TomlLintConfig>,
    snapshots: Option<TomlSnapshotConfig>,
    preflight: Option<TomlPreflightConfig>,
    databases: Option<Vec<TomlNamedDatabaseConfig>>,
    guards: Option<TomlGuardsConfig>,
    reversals: Option<TomlReversalConfig>,
    safety: Option<TomlSafetyConfig>,
    advisor: Option<TomlAdvisorConfig>,
    simulation: Option<TomlSimulationConfig>,
}

#[derive(Deserialize, Default)]
struct TomlDatabaseConfig {
    url: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    connect_retries: Option<u32>,
    ssl_mode: Option<String>,
    connect_timeout: Option<u32>,
    statement_timeout: Option<u32>,
    keepalive: Option<u32>,
}

#[derive(Deserialize, Default)]
struct TomlMigrationSettings {
    locations: Option<Vec<String>>,
    table: Option<String>,
    schema: Option<String>,
    out_of_order: Option<bool>,
    validate_on_migrate: Option<bool>,
    clean_enabled: Option<bool>,
    baseline_version: Option<String>,
    installed_by: Option<String>,
    environment: Option<String>,
    dependency_ordering: Option<bool>,
    show_progress: Option<bool>,
    batch_transaction: Option<bool>,
}

#[derive(Deserialize, Default)]
struct TomlLintConfig {
    disabled_rules: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct TomlSnapshotConfig {
    directory: Option<String>,
    auto_snapshot_on_migrate: Option<bool>,
    max_snapshots: Option<usize>,
    strip_definer_mysql: Option<bool>,
}

#[derive(Deserialize, Default)]
struct TomlPreflightConfig {
    enabled: Option<bool>,
    max_replication_lag_mb: Option<i64>,
    max_replication_lag_secs: Option<i64>,
    long_query_threshold_secs: Option<i64>,
}

#[derive(Deserialize, Default)]
struct TomlNamedDatabaseConfig {
    name: Option<String>,
    url: Option<String>,
    depends_on: Option<Vec<String>>,
    migrations: Option<TomlMigrationSettings>,
    hooks: Option<TomlHooksConfig>,
    placeholders: Option<HashMap<String, String>>,
}

#[derive(Deserialize, Default)]
struct TomlHooksConfig {
    before_migrate: Option<Vec<String>>,
    after_migrate: Option<Vec<String>>,
    before_each_migrate: Option<Vec<String>>,
    after_each_migrate: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct TomlGuardsConfig {
    on_require_fail: Option<String>,
}

#[derive(Deserialize, Default)]
struct TomlReversalConfig {
    enabled: Option<bool>,
    warn_data_loss: Option<bool>,
}

#[derive(Deserialize, Default)]
struct TomlSafetyConfig {
    enabled: Option<bool>,
    block_on_danger: Option<bool>,
    large_table_threshold: Option<i64>,
    huge_table_threshold: Option<i64>,
    refresh_stats_mysql: Option<bool>,
}

#[derive(Deserialize, Default)]
struct TomlAdvisorConfig {
    run_after_migrate: Option<bool>,
    disabled_rules: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
struct TomlSimulationConfig {
    simulate_before_migrate: Option<bool>,
}

/// CLI overrides that take highest priority.
#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    /// Override database connection URL.
    pub url: Option<String>,
    /// Override the database schema for the history table.
    pub schema: Option<String>,
    /// Override the schema history table name.
    pub table: Option<String>,
    /// Override migration file locations.
    pub locations: Option<Vec<PathBuf>>,
    /// Override whether out-of-order migrations are allowed.
    pub out_of_order: Option<bool>,
    /// Override whether to validate checksums on migrate.
    pub validate_on_migrate: Option<bool>,
    /// Override the baseline version string.
    pub baseline_version: Option<String>,
    /// Override the number of connection retries.
    pub connect_retries: Option<u32>,
    /// Override the SSL/TLS connection mode.
    pub ssl_mode: Option<String>,
    /// Override the connection timeout in seconds.
    pub connect_timeout: Option<u32>,
    /// Override the statement timeout in seconds.
    pub statement_timeout: Option<u32>,
    /// Override the logical environment name.
    pub environment: Option<String>,
    /// Override whether to use dependency-based migration ordering.
    pub dependency_ordering: Option<bool>,
    /// Override TCP keepalive interval in seconds.
    pub keepalive: Option<u32>,
    /// Override batch transaction mode (all-or-nothing).
    pub batch_transaction: Option<bool>,
}

impl WaypointConfig {
    /// Load configuration with the following priority (highest wins):
    /// 1. CLI arguments
    /// 2. Environment variables
    /// 3. TOML config file
    /// 4. Built-in defaults
    pub fn load(config_path: Option<&str>, overrides: &CliOverrides) -> Result<Self> {
        let mut config = WaypointConfig::default();

        // Layer 3: TOML config file
        let toml_path = config_path.unwrap_or("waypoint.toml");
        if let Ok(content) = std::fs::read_to_string(toml_path) {
            // Warn if config file has overly permissive permissions (Unix only)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(meta) = std::fs::metadata(toml_path) {
                    let mode = meta.permissions().mode();
                    if mode & 0o077 != 0 {
                        log::warn!("Config file has overly permissive permissions. Consider chmod 600.; path={}, mode={:o}", toml_path, mode);
                    }
                }
            }
            let toml_config: TomlConfig = toml::from_str(&content).map_err(|e| {
                WaypointError::ConfigError(format!(
                    "Failed to parse config file '{}': {}",
                    toml_path, e
                ))
            })?;
            config.apply_toml(toml_config);
        } else if config_path.is_some() {
            // If explicitly specified, error if not found
            return Err(WaypointError::ConfigError(format!(
                "Config file '{}' not found",
                toml_path
            )));
        }

        // Layer 2: Environment variables
        config.apply_env();

        // Layer 1: CLI overrides
        config.apply_cli(overrides);

        // Validate identifiers
        crate::db::validate_identifier(&config.migrations.schema)?;
        crate::db::validate_identifier(&config.migrations.table)?;

        // Cap connect_retries at 20
        if config.database.connect_retries > 20 {
            config.database.connect_retries = 20;
            log::warn!("connect_retries capped at 20");
        }

        Ok(config)
    }

    fn apply_toml(&mut self, toml: TomlConfig) {
        if let Some(db) = toml.database {
            apply_option_some!(db.url => self.database.url);
            apply_option_some!(db.host => self.database.host);
            apply_option_some!(db.port => self.database.port);
            apply_option_some!(db.user => self.database.user);
            apply_option_some!(db.password => self.database.password);
            apply_option_some!(db.database => self.database.database);
            apply_option!(db.connect_retries => self.database.connect_retries);
            if let Some(v) = db.ssl_mode {
                match v.parse() {
                    Ok(mode) => self.database.ssl_mode = mode,
                    Err(_) => log::warn!(
                        "Invalid ssl_mode '{}' in config, using default 'prefer'. Valid values: disable, prefer, require",
                        v
                    ),
                }
            }
            apply_option!(db.connect_timeout => self.database.connect_timeout_secs);
            apply_option!(db.statement_timeout => self.database.statement_timeout_secs);
            apply_option!(db.keepalive => self.database.keepalive_secs);
        }

        if let Some(m) = toml.migrations {
            if let Some(v) = m.locations {
                self.migrations.locations = v.into_iter().map(|s| normalize_location(&s)).collect();
            }
            apply_option!(m.table => self.migrations.table);
            apply_option!(m.schema => self.migrations.schema);
            apply_option!(m.out_of_order => self.migrations.out_of_order);
            apply_option!(m.validate_on_migrate => self.migrations.validate_on_migrate);
            apply_option!(m.clean_enabled => self.migrations.clean_enabled);
            apply_option!(m.baseline_version => self.migrations.baseline_version);
            apply_option_some!(m.installed_by => self.migrations.installed_by);
            apply_option_some!(m.environment => self.migrations.environment);
            apply_option!(m.dependency_ordering => self.migrations.dependency_ordering);
            apply_option!(m.show_progress => self.migrations.show_progress);
            apply_option!(m.batch_transaction => self.migrations.batch_transaction);
        }

        if let Some(h) = toml.hooks {
            if let Some(v) = h.before_migrate {
                self.hooks.before_migrate = v.into_iter().map(PathBuf::from).collect();
            }
            if let Some(v) = h.after_migrate {
                self.hooks.after_migrate = v.into_iter().map(PathBuf::from).collect();
            }
            if let Some(v) = h.before_each_migrate {
                self.hooks.before_each_migrate = v.into_iter().map(PathBuf::from).collect();
            }
            if let Some(v) = h.after_each_migrate {
                self.hooks.after_each_migrate = v.into_iter().map(PathBuf::from).collect();
            }
        }

        if let Some(p) = toml.placeholders {
            self.placeholders.extend(p);
        }

        if let Some(l) = toml.lint {
            apply_option!(l.disabled_rules => self.lint.disabled_rules);
        }

        if let Some(s) = toml.snapshots {
            if let Some(v) = s.directory {
                self.snapshots.directory = PathBuf::from(v);
            }
            apply_option!(s.auto_snapshot_on_migrate => self.snapshots.auto_snapshot_on_migrate);
            apply_option!(s.max_snapshots => self.snapshots.max_snapshots);
            apply_option!(s.strip_definer_mysql => self.snapshots.strip_definer_mysql);
        }

        if let Some(p) = toml.preflight {
            apply_option!(p.enabled => self.preflight.enabled);
            apply_option!(p.max_replication_lag_mb => self.preflight.max_replication_lag_mb);
            apply_option!(p.max_replication_lag_secs => self.preflight.max_replication_lag_secs);
            apply_option!(p.long_query_threshold_secs => self.preflight.long_query_threshold_secs);
        }

        if let Some(g) = toml.guards {
            if let Some(v) = g.on_require_fail {
                match v.parse() {
                    Ok(policy) => self.guards.on_require_fail = policy,
                    Err(_) => log::warn!(
                        "Invalid on_require_fail '{}' in config, using default 'error'. Valid values: error, warn, skip",
                        v
                    ),
                }
            }
        }

        if let Some(r) = toml.reversals {
            apply_option!(r.enabled => self.reversals.enabled);
            apply_option!(r.warn_data_loss => self.reversals.warn_data_loss);
        }

        if let Some(s) = toml.safety {
            apply_option!(s.enabled => self.safety.enabled);
            apply_option!(s.block_on_danger => self.safety.block_on_danger);
            apply_option!(s.large_table_threshold => self.safety.large_table_threshold);
            apply_option!(s.huge_table_threshold => self.safety.huge_table_threshold);
            apply_option!(s.refresh_stats_mysql => self.safety.refresh_stats_mysql);
        }

        if let Some(a) = toml.advisor {
            apply_option!(a.run_after_migrate => self.advisor.run_after_migrate);
            apply_option!(a.disabled_rules => self.advisor.disabled_rules);
        }

        if let Some(s) = toml.simulation {
            apply_option!(s.simulate_before_migrate => self.simulation.simulate_before_migrate);
        }

        if let Some(databases) = toml.databases {
            let mut named_dbs = Vec::new();
            for db in databases {
                let name = db.name.unwrap_or_default();
                let mut db_config = DatabaseConfig::default();
                apply_option_some!(db.url => db_config.url);
                // Check for per-database env var
                let env_url_key = format!("WAYPOINT_DB_{}_URL", name.to_uppercase());
                if let Ok(url) = std::env::var(&env_url_key) {
                    db_config.url = Some(url);
                }

                let mut mig_settings = MigrationSettings::default();
                if let Some(m) = db.migrations {
                    if let Some(v) = m.locations {
                        mig_settings.locations =
                            v.into_iter().map(|s| normalize_location(&s)).collect();
                    }
                    apply_option!(m.table => mig_settings.table);
                    apply_option!(m.schema => mig_settings.schema);
                    apply_option!(m.out_of_order => mig_settings.out_of_order);
                    apply_option!(m.validate_on_migrate => mig_settings.validate_on_migrate);
                    apply_option!(m.clean_enabled => mig_settings.clean_enabled);
                    apply_option!(m.baseline_version => mig_settings.baseline_version);
                    apply_option_some!(m.installed_by => mig_settings.installed_by);
                    apply_option_some!(m.environment => mig_settings.environment);
                    apply_option!(m.dependency_ordering => mig_settings.dependency_ordering);
                    apply_option!(m.show_progress => mig_settings.show_progress);
                    apply_option!(m.batch_transaction => mig_settings.batch_transaction);
                }

                let mut hooks_config = HooksConfig::default();
                if let Some(h) = db.hooks {
                    if let Some(v) = h.before_migrate {
                        hooks_config.before_migrate = v.into_iter().map(PathBuf::from).collect();
                    }
                    if let Some(v) = h.after_migrate {
                        hooks_config.after_migrate = v.into_iter().map(PathBuf::from).collect();
                    }
                    if let Some(v) = h.before_each_migrate {
                        hooks_config.before_each_migrate =
                            v.into_iter().map(PathBuf::from).collect();
                    }
                    if let Some(v) = h.after_each_migrate {
                        hooks_config.after_each_migrate =
                            v.into_iter().map(PathBuf::from).collect();
                    }
                }

                named_dbs.push(crate::multi::NamedDatabaseConfig {
                    name,
                    database: db_config,
                    migrations: mig_settings,
                    hooks: hooks_config,
                    placeholders: db.placeholders.unwrap_or_default(),
                    depends_on: db.depends_on.unwrap_or_default(),
                });
            }
            self.multi_database = Some(named_dbs);
        }
    }

    fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("WAYPOINT_DATABASE_URL") {
            self.database.url = Some(v);
        }
        if let Ok(v) = std::env::var("WAYPOINT_DATABASE_HOST") {
            self.database.host = Some(v);
        }
        if let Ok(v) = std::env::var("WAYPOINT_DATABASE_PORT") {
            if let Ok(port) = v.parse::<u16>() {
                self.database.port = Some(port);
            }
        }
        if let Ok(v) = std::env::var("WAYPOINT_DATABASE_USER") {
            self.database.user = Some(v);
        }
        if let Ok(v) = std::env::var("WAYPOINT_DATABASE_PASSWORD") {
            self.database.password = Some(v);
        }
        if let Ok(v) = std::env::var("WAYPOINT_DATABASE_NAME") {
            self.database.database = Some(v);
        }
        if let Ok(v) = std::env::var("WAYPOINT_CONNECT_RETRIES") {
            if let Ok(n) = v.parse::<u32>() {
                self.database.connect_retries = n;
            }
        }
        if let Ok(v) = std::env::var("WAYPOINT_SSL_MODE") {
            if let Ok(mode) = v.parse() {
                self.database.ssl_mode = mode;
            }
        }
        if let Ok(v) = std::env::var("WAYPOINT_CONNECT_TIMEOUT") {
            if let Ok(n) = v.parse::<u32>() {
                self.database.connect_timeout_secs = n;
            }
        }
        if let Ok(v) = std::env::var("WAYPOINT_STATEMENT_TIMEOUT") {
            if let Ok(n) = v.parse::<u32>() {
                self.database.statement_timeout_secs = n;
            }
        }
        if let Ok(v) = std::env::var("WAYPOINT_MIGRATIONS_LOCATIONS") {
            self.migrations.locations =
                v.split(',').map(|s| normalize_location(s.trim())).collect();
        }
        if let Ok(v) = std::env::var("WAYPOINT_MIGRATIONS_TABLE") {
            self.migrations.table = v;
        }
        if let Ok(v) = std::env::var("WAYPOINT_MIGRATIONS_SCHEMA") {
            self.migrations.schema = v;
        }

        if let Ok(v) = std::env::var("WAYPOINT_KEEPALIVE") {
            if let Ok(n) = v.parse::<u32>() {
                self.database.keepalive_secs = n;
            }
        }
        if let Ok(v) = std::env::var("WAYPOINT_BATCH_TRANSACTION") {
            self.migrations.batch_transaction = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("WAYPOINT_ENVIRONMENT") {
            self.migrations.environment = Some(v);
        }

        // Scan for placeholder env vars: WAYPOINT_PLACEHOLDER_{KEY}
        for (key, value) in std::env::vars() {
            if let Some(placeholder_key) = key.strip_prefix("WAYPOINT_PLACEHOLDER_") {
                self.placeholders
                    .insert(placeholder_key.to_lowercase(), value);
            }
        }
    }

    fn apply_cli(&mut self, overrides: &CliOverrides) {
        apply_option_some_clone!(overrides.url => self.database.url);
        apply_option_clone!(overrides.schema => self.migrations.schema);
        apply_option_clone!(overrides.table => self.migrations.table);
        apply_option_clone!(overrides.locations => self.migrations.locations);
        apply_option!(overrides.out_of_order => self.migrations.out_of_order);
        apply_option!(overrides.validate_on_migrate => self.migrations.validate_on_migrate);
        apply_option_clone!(overrides.baseline_version => self.migrations.baseline_version);
        apply_option!(overrides.connect_retries => self.database.connect_retries);
        if let Some(ref v) = overrides.ssl_mode {
            // Ignore parse errors here — they'll be caught in validation
            if let Ok(mode) = v.parse() {
                self.database.ssl_mode = mode;
            }
        }
        apply_option!(overrides.connect_timeout => self.database.connect_timeout_secs);
        apply_option!(overrides.statement_timeout => self.database.statement_timeout_secs);
        apply_option_some_clone!(overrides.environment => self.migrations.environment);
        apply_option!(overrides.dependency_ordering => self.migrations.dependency_ordering);
        apply_option!(overrides.keepalive => self.database.keepalive_secs);
        apply_option!(overrides.batch_transaction => self.migrations.batch_transaction);
    }

    /// Build a connection string from the config.
    /// Prefers `url` if set; otherwise builds from individual fields.
    /// Handles JDBC-style URLs by stripping the `jdbc:` prefix and
    /// extracting `user` and `password` query parameters.
    pub fn connection_string(&self) -> Result<String> {
        if let Some(ref url) = self.database.url {
            return Ok(normalize_jdbc_url(url));
        }

        let host = self.database.host.as_deref().unwrap_or("localhost");
        let port = self.database.port.unwrap_or(5432);
        let user =
            self.database.user.as_deref().ok_or_else(|| {
                WaypointError::ConfigError("Database user is required".to_string())
            })?;
        let database =
            self.database.database.as_deref().ok_or_else(|| {
                WaypointError::ConfigError("Database name is required".to_string())
            })?;

        let mut url = format!(
            "host={} port={} user={} dbname={}",
            host, port, user, database
        );

        if let Some(ref password) = self.database.password {
            // Quote password to handle special characters (spaces, quotes, etc.)
            let escaped = password.replace('\\', "\\\\").replace('\'', "\\'");
            url.push_str(&format!(" password='{}'", escaped));
        }

        Ok(url)
    }
}

/// Normalize a JDBC-style URL to a standard PostgreSQL connection string.
///
/// Handles:
///   - `jdbc:postgresql://host:port/db?user=x&password=y`  →  `postgresql://x:y@host:port/db`
///   - `postgresql://...` passed through as-is
///   - `postgres://...` passed through as-is
fn normalize_jdbc_url(url: &str) -> String {
    // Strip jdbc: prefix
    let url = url.strip_prefix("jdbc:").unwrap_or(url);

    // Parse query parameters for user/password if present
    if let Some((base, query)) = url.split_once('?') {
        let mut user = None;
        let mut password = None;
        let mut other_params = Vec::new();

        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key.to_lowercase().as_str() {
                    "user" => user = Some(value.to_string()),
                    "password" => password = Some(value.to_string()),
                    _ => other_params.push(param.to_string()),
                }
            }
        }

        // If we extracted user/password, rebuild the URL with credentials in the authority
        if user.is_some() || password.is_some() {
            if let Some(rest) = base
                .strip_prefix("postgresql://")
                .or_else(|| base.strip_prefix("postgres://"))
            {
                let scheme = if base.starts_with("postgresql://") {
                    "postgresql"
                } else {
                    "postgres"
                };

                let auth = match (user, password) {
                    (Some(u), Some(p)) => format!("{}:{}@", u, p),
                    (Some(u), None) => format!("{}@", u),
                    (None, Some(p)) => format!(":{p}@"),
                    (None, None) => String::new(),
                };

                let mut result = format!("{}://{}{}", scheme, auth, rest);
                if !other_params.is_empty() {
                    result.push('?');
                    result.push_str(&other_params.join("&"));
                }
                return result;
            }
        }

        // No user/password in query, return with jdbc: stripped
        if other_params.is_empty() {
            return base.to_string();
        }
        return format!("{}?{}", base, other_params.join("&"));
    }

    url.to_string()
}

/// Strip `filesystem:` prefix from a location path (Flyway compatibility).
pub fn normalize_location(location: &str) -> PathBuf {
    let stripped = location.strip_prefix("filesystem:").unwrap_or(location);
    PathBuf::from(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = WaypointConfig::default();
        assert_eq!(config.migrations.table, "waypoint_schema_history");
        assert_eq!(config.migrations.schema, "public");
        assert!(!config.migrations.out_of_order);
        assert!(config.migrations.validate_on_migrate);
        assert!(!config.migrations.clean_enabled);
        assert_eq!(config.migrations.baseline_version, "1");
        assert_eq!(
            config.migrations.locations,
            vec![PathBuf::from("db/migrations")]
        );
    }

    #[test]
    fn test_connection_string_from_url() {
        let mut config = WaypointConfig::default();
        config.database.url = Some("postgres://user:pass@localhost/db".to_string());
        assert_eq!(
            config.connection_string().unwrap(),
            "postgres://user:pass@localhost/db"
        );
    }

    #[test]
    fn test_connection_string_from_fields() {
        let mut config = WaypointConfig::default();
        config.database.host = Some("myhost".to_string());
        config.database.port = Some(5433);
        config.database.user = Some("myuser".to_string());
        config.database.database = Some("mydb".to_string());
        config.database.password = Some("secret".to_string());

        let conn = config.connection_string().unwrap();
        assert!(conn.contains("host=myhost"));
        assert!(conn.contains("port=5433"));
        assert!(conn.contains("user=myuser"));
        assert!(conn.contains("dbname=mydb"));
        assert!(conn.contains("password='secret'"));
    }

    #[test]
    fn test_connection_string_missing_user() {
        let mut config = WaypointConfig::default();
        config.database.database = Some("mydb".to_string());
        assert!(config.connection_string().is_err());
    }

    #[test]
    fn test_cli_overrides() {
        let mut config = WaypointConfig::default();
        let overrides = CliOverrides {
            url: Some("postgres://override@localhost/db".to_string()),
            schema: Some("custom_schema".to_string()),
            table: Some("custom_table".to_string()),
            locations: Some(vec![PathBuf::from("custom/path")]),
            out_of_order: Some(true),
            validate_on_migrate: Some(false),
            baseline_version: Some("5".to_string()),
            connect_retries: None,
            ssl_mode: None,
            connect_timeout: None,
            statement_timeout: None,
            environment: None,
            dependency_ordering: None,
            keepalive: None,
            batch_transaction: None,
        };

        config.apply_cli(&overrides);

        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://override@localhost/db")
        );
        assert_eq!(config.migrations.schema, "custom_schema");
        assert_eq!(config.migrations.table, "custom_table");
        assert_eq!(
            config.migrations.locations,
            vec![PathBuf::from("custom/path")]
        );
        assert!(config.migrations.out_of_order);
        assert!(!config.migrations.validate_on_migrate);
        assert_eq!(config.migrations.baseline_version, "5");
    }

    #[test]
    fn test_toml_parsing() {
        let toml_str = r#"
[database]
url = "postgres://user:pass@localhost/mydb"

[migrations]
table = "my_history"
schema = "app"
out_of_order = true
locations = ["sql/migrations", "sql/seeds"]

[placeholders]
env = "production"
app_name = "myapp"
"#;

        let toml_config: TomlConfig = toml::from_str(toml_str).unwrap();
        let mut config = WaypointConfig::default();
        config.apply_toml(toml_config);

        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://user:pass@localhost/mydb")
        );
        assert_eq!(config.migrations.table, "my_history");
        assert_eq!(config.migrations.schema, "app");
        assert!(config.migrations.out_of_order);
        assert_eq!(
            config.migrations.locations,
            vec![PathBuf::from("sql/migrations"), PathBuf::from("sql/seeds")]
        );
        assert_eq!(config.placeholders.get("env").unwrap(), "production");
        assert_eq!(config.placeholders.get("app_name").unwrap(), "myapp");
    }

    #[test]
    fn test_normalize_jdbc_url_with_credentials() {
        let url = "jdbc:postgresql://myhost:5432/mydb?user=admin&password=secret";
        assert_eq!(
            normalize_jdbc_url(url),
            "postgresql://admin:secret@myhost:5432/mydb"
        );
    }

    #[test]
    fn test_normalize_jdbc_url_user_only() {
        let url = "jdbc:postgresql://myhost:5432/mydb?user=admin";
        assert_eq!(
            normalize_jdbc_url(url),
            "postgresql://admin@myhost:5432/mydb"
        );
    }

    #[test]
    fn test_normalize_jdbc_url_strips_jdbc_prefix() {
        let url = "jdbc:postgresql://myhost:5432/mydb";
        assert_eq!(normalize_jdbc_url(url), "postgresql://myhost:5432/mydb");
    }

    #[test]
    fn test_normalize_jdbc_url_passthrough() {
        let url = "postgresql://user:pass@myhost:5432/mydb";
        assert_eq!(normalize_jdbc_url(url), url);
    }

    #[test]
    fn test_normalize_jdbc_url_preserves_other_params() {
        let url = "jdbc:postgresql://myhost:5432/mydb?user=admin&password=secret&sslmode=require";
        assert_eq!(
            normalize_jdbc_url(url),
            "postgresql://admin:secret@myhost:5432/mydb?sslmode=require"
        );
    }

    #[test]
    fn test_normalize_location_filesystem_prefix() {
        assert_eq!(
            normalize_location("filesystem:/flyway/sql"),
            PathBuf::from("/flyway/sql")
        );
    }

    #[test]
    fn test_normalize_location_plain_path() {
        assert_eq!(
            normalize_location("/my/migrations"),
            PathBuf::from("/my/migrations")
        );
    }

    #[test]
    fn test_normalize_location_relative() {
        assert_eq!(
            normalize_location("filesystem:db/migrations"),
            PathBuf::from("db/migrations")
        );
    }

    #[test]
    fn test_connection_string_password_special_chars() {
        let config = WaypointConfig {
            database: DatabaseConfig {
                host: Some("localhost".to_string()),
                port: Some(5432),
                user: Some("admin".to_string()),
                database: Some("mydb".to_string()),
                password: Some("p@ss'w ord".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let conn = config.connection_string().unwrap();
        assert!(conn.contains("password='p@ss\\'w ord'"));
    }
}
