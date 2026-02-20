use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use tokio_postgres::Client;

use crate::config::{HooksConfig, WaypointConfig};
use crate::db;
use crate::error::{Result, WaypointError};
use crate::placeholder::replace_placeholders;

/// The phase at which a hook runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookType {
    BeforeMigrate,
    AfterMigrate,
    BeforeEachMigrate,
    AfterEachMigrate,
}

impl fmt::Display for HookType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HookType::BeforeMigrate => write!(f, "beforeMigrate"),
            HookType::AfterMigrate => write!(f, "afterMigrate"),
            HookType::BeforeEachMigrate => write!(f, "beforeEachMigrate"),
            HookType::AfterEachMigrate => write!(f, "afterEachMigrate"),
        }
    }
}

/// A hook SQL script discovered on disk or specified in config.
#[derive(Debug, Clone)]
pub struct ResolvedHook {
    pub hook_type: HookType,
    pub script_name: String,
    pub sql: String,
}

/// File prefixes that indicate hook callback files (Flyway-compatible).
const HOOK_PREFIXES: &[(&str, fn() -> HookType)] = &[
    ("beforeEachMigrate", || HookType::BeforeEachMigrate),
    ("afterEachMigrate", || HookType::AfterEachMigrate),
    ("beforeMigrate", || HookType::BeforeMigrate),
    ("afterMigrate", || HookType::AfterMigrate),
];

/// Check if a filename is a hook callback file (not a migration).
pub fn is_hook_file(filename: &str) -> bool {
    HOOK_PREFIXES.iter().any(|(prefix, _)| filename.starts_with(prefix) && filename.ends_with(".sql"))
}

/// Scan migration locations for SQL callback hook files.
///
/// Recognizes:
///   - `beforeMigrate.sql` / `beforeMigrate__*.sql`
///   - `afterMigrate.sql` / `afterMigrate__*.sql`
///   - `beforeEachMigrate.sql` / `beforeEachMigrate__*.sql`
///   - `afterEachMigrate.sql` / `afterEachMigrate__*.sql`
///
/// Multiple files per hook type are sorted alphabetically.
pub fn scan_hooks(locations: &[PathBuf]) -> Result<Vec<ResolvedHook>> {
    let mut hooks = Vec::new();

    for location in locations {
        if !location.exists() {
            continue;
        }

        let entries = std::fs::read_dir(location).map_err(|e| {
            WaypointError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read hook directory '{}': {}", location.display(), e),
            ))
        })?;

        let mut files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        // Sort alphabetically for deterministic ordering
        files.sort_by_key(|e| e.file_name());

        for entry in files {
            let path = entry.path();
            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            if !filename.ends_with(".sql") {
                continue;
            }

            // Check each hook prefix
            for (prefix, type_fn) in HOOK_PREFIXES {
                if filename.starts_with(prefix) {
                    // Must be exactly `prefix.sql` or `prefix__*.sql`
                    let rest = &filename[prefix.len()..filename.len() - 4]; // strip prefix and .sql
                    if rest.is_empty() || rest.starts_with("__") {
                        let sql = std::fs::read_to_string(&path)?;
                        hooks.push(ResolvedHook {
                            hook_type: type_fn(),
                            script_name: filename.clone(),
                            sql,
                        });
                        break;
                    }
                }
            }
        }
    }

    // Sort within each hook type alphabetically by script name
    hooks.sort_by(|a, b| {
        a.hook_type
            .to_string()
            .cmp(&b.hook_type.to_string())
            .then_with(|| a.script_name.cmp(&b.script_name))
    });

    Ok(hooks)
}

/// Load hook SQL files specified in the TOML `[hooks]` config section.
pub fn load_config_hooks(config: &HooksConfig) -> Result<Vec<ResolvedHook>> {
    let mut hooks = Vec::new();

    let sections: &[(HookType, &[PathBuf])] = &[
        (HookType::BeforeMigrate, &config.before_migrate),
        (HookType::AfterMigrate, &config.after_migrate),
        (HookType::BeforeEachMigrate, &config.before_each_migrate),
        (HookType::AfterEachMigrate, &config.after_each_migrate),
    ];

    for (hook_type, paths) in sections {
        for path in *paths {
            let sql = std::fs::read_to_string(path).map_err(|e| {
                WaypointError::IoError(std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to read hook file '{}': {}",
                        path.display(),
                        e
                    ),
                ))
            })?;

            let script_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_else(|| path.to_str().unwrap_or("unknown"))
                .to_string();

            hooks.push(ResolvedHook {
                hook_type: hook_type.clone(),
                script_name,
                sql,
            });
        }
    }

    Ok(hooks)
}

/// Run all hooks of a given type.
///
/// Returns total execution time in milliseconds.
pub async fn run_hooks(
    client: &Client,
    _config: &WaypointConfig,
    hooks: &[ResolvedHook],
    phase: &HookType,
    placeholders: &HashMap<String, String>,
) -> Result<(usize, i32)> {
    let mut total_ms = 0;
    let mut count = 0;

    for hook in hooks.iter().filter(|h| &h.hook_type == phase) {
        tracing::info!("Running {} hook: {}", phase, hook.script_name);

        let sql = replace_placeholders(&hook.sql, placeholders)?;

        match db::execute_in_transaction(client, &sql).await {
            Ok(exec_time) => {
                total_ms += exec_time;
                count += 1;
            }
            Err(e) => {
                let reason = match &e {
                    WaypointError::DatabaseError(db_err) => crate::error::format_db_error(db_err),
                    other => other.to_string(),
                };
                return Err(WaypointError::HookFailed {
                    phase: phase.to_string(),
                    script: hook.script_name.clone(),
                    reason,
                });
            }
        }
    }

    Ok((count, total_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("waypoint_hooks_test_{}", name));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_is_hook_file() {
        assert!(is_hook_file("beforeMigrate.sql"));
        assert!(is_hook_file("afterMigrate.sql"));
        assert!(is_hook_file("beforeEachMigrate.sql"));
        assert!(is_hook_file("afterEachMigrate.sql"));
        assert!(is_hook_file("beforeMigrate__Disable_triggers.sql"));
        assert!(is_hook_file("afterMigrate__Refresh_views.sql"));

        assert!(!is_hook_file("V1__Create_table.sql"));
        assert!(!is_hook_file("R__Create_view.sql"));
        assert!(!is_hook_file("beforeMigrate.txt"));
        assert!(!is_hook_file("random.sql"));
    }

    #[test]
    fn test_scan_hooks_finds_callback_files() {
        let dir = create_temp_dir("scan");
        fs::write(dir.join("beforeMigrate.sql"), "SELECT 1;").unwrap();
        fs::write(dir.join("afterMigrate__Refresh_views.sql"), "SELECT 2;").unwrap();
        fs::write(dir.join("V1__Create_table.sql"), "CREATE TABLE t(id INT);").unwrap();
        fs::write(dir.join("R__Create_view.sql"), "CREATE VIEW v AS SELECT 1;").unwrap();

        let hooks = scan_hooks(&[dir.clone()]).unwrap();

        assert_eq!(hooks.len(), 2);

        let before: Vec<_> = hooks.iter().filter(|h| h.hook_type == HookType::BeforeMigrate).collect();
        let after: Vec<_> = hooks.iter().filter(|h| h.hook_type == HookType::AfterMigrate).collect();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].script_name, "beforeMigrate.sql");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].script_name, "afterMigrate__Refresh_views.sql");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_scan_hooks_multiple_sorted_alphabetically() {
        let dir = create_temp_dir("multi");
        fs::write(dir.join("beforeMigrate__B_second.sql"), "SELECT 2;").unwrap();
        fs::write(dir.join("beforeMigrate__A_first.sql"), "SELECT 1;").unwrap();
        fs::write(dir.join("beforeMigrate.sql"), "SELECT 0;").unwrap();

        let hooks = scan_hooks(&[dir.clone()]).unwrap();

        assert_eq!(hooks.len(), 3);
        assert_eq!(hooks[0].script_name, "beforeMigrate.sql");
        assert_eq!(hooks[1].script_name, "beforeMigrate__A_first.sql");
        assert_eq!(hooks[2].script_name, "beforeMigrate__B_second.sql");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_config_hooks() {
        let dir = create_temp_dir("config");
        let hook_file = dir.join("pre.sql");
        fs::write(&hook_file, "SET work_mem = '256MB';").unwrap();

        let config = HooksConfig {
            before_migrate: vec![hook_file],
            after_migrate: vec![],
            before_each_migrate: vec![],
            after_each_migrate: vec![],
        };

        let hooks = load_config_hooks(&config).unwrap();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].hook_type, HookType::BeforeMigrate);
        assert_eq!(hooks[0].sql, "SET work_mem = '256MB';");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_config_hooks_missing_file() {
        let config = HooksConfig {
            before_migrate: vec![PathBuf::from("/nonexistent/hook.sql")],
            after_migrate: vec![],
            before_each_migrate: vec![],
            after_each_migrate: vec![],
        };

        assert!(load_config_hooks(&config).is_err());
    }
}
