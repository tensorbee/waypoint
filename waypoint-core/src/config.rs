use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{Result, WaypointError};

/// SSL/TLS connection mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SslMode {
    /// Never use TLS (current default behavior).
    Disable,
    /// Try TLS first, fall back to plaintext.
    Prefer,
    /// Require TLS — fail if handshake fails.
    Require,
}

impl Default for SslMode {
    fn default() -> Self {
        SslMode::Prefer
    }
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
#[derive(Debug, Clone)]
pub struct WaypointConfig {
    pub database: DatabaseConfig,
    pub migrations: MigrationSettings,
    pub hooks: HooksConfig,
    pub placeholders: HashMap<String, String>,
}

/// Database connection configuration.
#[derive(Clone)]
pub struct DatabaseConfig {
    pub url: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub connect_retries: u32,
    pub ssl_mode: SslMode,
    pub connect_timeout_secs: u32,
    pub statement_timeout_secs: u32,
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
            .finish()
    }
}

/// Hook configuration for running SQL before/after migrations.
#[derive(Debug, Clone, Default)]
pub struct HooksConfig {
    pub before_migrate: Vec<PathBuf>,
    pub after_migrate: Vec<PathBuf>,
    pub before_each_migrate: Vec<PathBuf>,
    pub after_each_migrate: Vec<PathBuf>,
}

/// Migration behavior settings.
#[derive(Debug, Clone)]
pub struct MigrationSettings {
    pub locations: Vec<PathBuf>,
    pub table: String,
    pub schema: String,
    pub out_of_order: bool,
    pub validate_on_migrate: bool,
    pub clean_enabled: bool,
    pub baseline_version: String,
    pub installed_by: Option<String>,
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
        }
    }
}

impl Default for WaypointConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            migrations: MigrationSettings::default(),
            hooks: HooksConfig::default(),
            placeholders: HashMap::new(),
        }
    }
}

// ── TOML deserialization structs ──

#[derive(Deserialize, Default)]
struct TomlConfig {
    database: Option<TomlDatabaseConfig>,
    migrations: Option<TomlMigrationSettings>,
    hooks: Option<TomlHooksConfig>,
    placeholders: Option<HashMap<String, String>>,
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
}

#[derive(Deserialize, Default)]
struct TomlHooksConfig {
    before_migrate: Option<Vec<String>>,
    after_migrate: Option<Vec<String>>,
    before_each_migrate: Option<Vec<String>>,
    after_each_migrate: Option<Vec<String>>,
}

/// CLI overrides that take highest priority.
#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    pub url: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub locations: Option<Vec<PathBuf>>,
    pub out_of_order: Option<bool>,
    pub validate_on_migrate: Option<bool>,
    pub baseline_version: Option<String>,
    pub connect_retries: Option<u32>,
    pub ssl_mode: Option<String>,
    pub connect_timeout: Option<u32>,
    pub statement_timeout: Option<u32>,
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
                        tracing::warn!(
                            path = %toml_path,
                            mode = format!("{:o}", mode),
                            "Config file has overly permissive permissions. Consider chmod 600."
                        );
                    }
                }
            }
            let toml_config: TomlConfig = toml::from_str(&content).map_err(|e| {
                WaypointError::ConfigError(format!("Failed to parse config file '{}': {}", toml_path, e))
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
            tracing::warn!("connect_retries capped at 20");
        }

        Ok(config)
    }

    fn apply_toml(&mut self, toml: TomlConfig) {
        if let Some(db) = toml.database {
            if let Some(v) = db.url { self.database.url = Some(v); }
            if let Some(v) = db.host { self.database.host = Some(v); }
            if let Some(v) = db.port { self.database.port = Some(v); }
            if let Some(v) = db.user { self.database.user = Some(v); }
            if let Some(v) = db.password { self.database.password = Some(v); }
            if let Some(v) = db.database { self.database.database = Some(v); }
            if let Some(v) = db.connect_retries { self.database.connect_retries = v; }
            if let Some(v) = db.ssl_mode {
                if let Ok(mode) = v.parse() {
                    self.database.ssl_mode = mode;
                }
            }
            if let Some(v) = db.connect_timeout { self.database.connect_timeout_secs = v; }
            if let Some(v) = db.statement_timeout { self.database.statement_timeout_secs = v; }
        }

        if let Some(m) = toml.migrations {
            if let Some(v) = m.locations {
                self.migrations.locations = v.into_iter().map(|s| normalize_location(&s)).collect();
            }
            if let Some(v) = m.table { self.migrations.table = v; }
            if let Some(v) = m.schema { self.migrations.schema = v; }
            if let Some(v) = m.out_of_order { self.migrations.out_of_order = v; }
            if let Some(v) = m.validate_on_migrate { self.migrations.validate_on_migrate = v; }
            if let Some(v) = m.clean_enabled { self.migrations.clean_enabled = v; }
            if let Some(v) = m.baseline_version { self.migrations.baseline_version = v; }
            if let Some(v) = m.installed_by { self.migrations.installed_by = Some(v); }
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
            self.migrations.locations = v.split(',').map(|s| normalize_location(s.trim())).collect();
        }
        if let Ok(v) = std::env::var("WAYPOINT_MIGRATIONS_TABLE") {
            self.migrations.table = v;
        }
        if let Ok(v) = std::env::var("WAYPOINT_MIGRATIONS_SCHEMA") {
            self.migrations.schema = v;
        }

        // Scan for placeholder env vars: WAYPOINT_PLACEHOLDER_{KEY}
        for (key, value) in std::env::vars() {
            if let Some(placeholder_key) = key.strip_prefix("WAYPOINT_PLACEHOLDER_") {
                self.placeholders.insert(placeholder_key.to_lowercase(), value);
            }
        }
    }

    fn apply_cli(&mut self, overrides: &CliOverrides) {
        if let Some(ref v) = overrides.url {
            self.database.url = Some(v.clone());
        }
        if let Some(ref v) = overrides.schema {
            self.migrations.schema = v.clone();
        }
        if let Some(ref v) = overrides.table {
            self.migrations.table = v.clone();
        }
        if let Some(ref v) = overrides.locations {
            self.migrations.locations = v.clone();
        }
        if let Some(v) = overrides.out_of_order {
            self.migrations.out_of_order = v;
        }
        if let Some(v) = overrides.validate_on_migrate {
            self.migrations.validate_on_migrate = v;
        }
        if let Some(ref v) = overrides.baseline_version {
            self.migrations.baseline_version = v.clone();
        }
        if let Some(v) = overrides.connect_retries {
            self.database.connect_retries = v;
        }
        if let Some(ref v) = overrides.ssl_mode {
            // Ignore parse errors here — they'll be caught in validation
            if let Ok(mode) = v.parse() {
                self.database.ssl_mode = mode;
            }
        }
        if let Some(v) = overrides.connect_timeout {
            self.database.connect_timeout_secs = v;
        }
        if let Some(v) = overrides.statement_timeout {
            self.database.statement_timeout_secs = v;
        }
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
        let user = self.database.user.as_deref().ok_or_else(|| {
            WaypointError::ConfigError("Database user is required".to_string())
        })?;
        let database = self.database.database.as_deref().ok_or_else(|| {
            WaypointError::ConfigError("Database name is required".to_string())
        })?;

        let mut url = format!("host={} port={} user={} dbname={}", host, port, user, database);

        if let Some(ref password) = self.database.password {
            url.push_str(&format!(" password={}", password));
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
            if let Some(rest) = base.strip_prefix("postgresql://").or_else(|| base.strip_prefix("postgres://")) {
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
    let stripped = location
        .strip_prefix("filesystem:")
        .unwrap_or(location);
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
        assert_eq!(config.migrations.locations, vec![PathBuf::from("db/migrations")]);
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
        assert!(conn.contains("password=secret"));
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
        };

        config.apply_cli(&overrides);

        assert_eq!(config.database.url.as_deref(), Some("postgres://override@localhost/db"));
        assert_eq!(config.migrations.schema, "custom_schema");
        assert_eq!(config.migrations.table, "custom_table");
        assert_eq!(config.migrations.locations, vec![PathBuf::from("custom/path")]);
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

        assert_eq!(config.database.url.as_deref(), Some("postgres://user:pass@localhost/mydb"));
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
        assert_eq!(
            normalize_jdbc_url(url),
            "postgresql://myhost:5432/mydb"
        );
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
}
