//! Logical schema snapshots for rollback without undo files.
//!
//! Takes a snapshot of the current schema as DDL, stores it as a SQL file,
//! and can restore from a previous snapshot.

use std::path::PathBuf;

use serde::Serialize;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

use crate::config::WaypointConfig;
use crate::db::DbClient;
use crate::dialect::DialectKind;
use crate::error::{Result, WaypointError};
#[cfg(feature = "postgres")]
use crate::schema;

/// Configuration for snapshots.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Directory where snapshot files are stored.
    pub directory: PathBuf,
    /// Whether to automatically take a snapshot before each migration.
    pub auto_snapshot_on_migrate: bool,
    /// Maximum number of snapshots to retain (oldest are pruned).
    pub max_snapshots: usize,
    /// MySQL only: strip `DEFINER=...` clauses from view (and routine) DDL in
    /// the snapshot. Defaults to `true` because the most common restore
    /// scenario is cross-account/cross-host, where the original definer user
    /// doesn't exist on the target. Set to `false` to preserve `DEFINER`
    /// clauses verbatim (the restoring user then needs `SUPER` /
    /// `SET_USER_ID` privileges).
    pub strip_definer_mysql: bool,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from(".waypoint/snapshots"),
            auto_snapshot_on_migrate: false,
            max_snapshots: 10,
            strip_definer_mysql: true,
        }
    }
}

/// Report from a snapshot operation.
#[derive(Debug, Serialize)]
pub struct SnapshotReport {
    /// Unique identifier for the snapshot (timestamp-based).
    pub snapshot_id: String,
    /// Filesystem path where the snapshot SQL file was written.
    pub snapshot_path: String,
    /// Total number of schema objects captured in the snapshot.
    pub objects_captured: usize,
}

/// Report from a restore operation.
#[derive(Debug, Serialize)]
pub struct RestoreReport {
    /// Identifier of the snapshot that was restored.
    pub snapshot_id: String,
    /// Number of schema objects successfully restored.
    pub objects_restored: usize,
}

/// Info about an available snapshot.
#[derive(Debug, Serialize)]
pub struct SnapshotInfo {
    /// Unique identifier for the snapshot.
    pub id: String,
    /// Filesystem path to the snapshot SQL file.
    pub path: PathBuf,
    /// Size of the snapshot file in bytes.
    pub size_bytes: u64,
    /// Human-readable creation timestamp.
    pub created: String,
}

/// Take a snapshot of the current schema (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute_snapshot(
    client: &Client,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
) -> Result<SnapshotReport> {
    let schema_name = &config.migrations.schema;

    // Introspect the schema
    let snapshot = schema::introspect(client, schema_name).await?;

    // Generate DDL
    let ddl = schema::to_ddl(&snapshot);

    // Create snapshot directory
    let dir = &snapshot_config.directory;
    std::fs::create_dir_all(dir)?;

    // Generate snapshot ID
    let snapshot_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let sql_path = dir.join(format!("{}.sql", snapshot_id));
    let meta_path = dir.join(format!("{}.json", snapshot_id));

    // Count objects
    let objects_captured = snapshot.tables.len()
        + snapshot.views.len()
        + snapshot.indexes.len()
        + snapshot.sequences.len()
        + snapshot.functions.len()
        + snapshot.enums.len()
        + snapshot.constraints.len()
        + snapshot.triggers.len();

    // Write SQL file
    std::fs::write(&sql_path, &ddl)?;

    // Write metadata
    let meta = serde_json::json!({
        "snapshot_id": snapshot_id,
        "schema": schema_name,
        "objects_captured": objects_captured,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "tables": snapshot.tables.len(),
        "views": snapshot.views.len(),
        "indexes": snapshot.indexes.len(),
        "sequences": snapshot.sequences.len(),
        "functions": snapshot.functions.len(),
        "enums": snapshot.enums.len(),
    });
    std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap())?;

    // Prune old snapshots if over max
    prune_snapshots(dir, snapshot_config.max_snapshots)?;

    Ok(SnapshotReport {
        snapshot_id,
        snapshot_path: sql_path.display().to_string(),
        objects_captured,
    })
}

/// Restore a schema from a snapshot (PostgreSQL legacy entry).
#[cfg(feature = "postgres")]
pub async fn execute_restore(
    client: &Client,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
    snapshot_id: &str,
) -> Result<RestoreReport> {
    let schema_name = &config.migrations.schema;
    let sql_path = snapshot_config
        .directory
        .join(format!("{}.sql", snapshot_id));

    if !sql_path.exists() {
        return Err(WaypointError::SnapshotError {
            reason: format!(
                "Snapshot '{}' not found at {}",
                snapshot_id,
                sql_path.display()
            ),
        });
    }

    let sql = std::fs::read_to_string(&sql_path)?;

    // Drop all objects in schema (like clean)
    let drop_sql = format!(
        "DROP SCHEMA IF EXISTS {} CASCADE; CREATE SCHEMA {};",
        crate::db::quote_ident(schema_name),
        crate::db::quote_ident(schema_name),
    );
    client.batch_execute(&drop_sql).await?;

    // Set search_path and execute snapshot DDL
    client
        .batch_execute(&format!(
            "SET search_path TO {}",
            crate::db::quote_ident(schema_name)
        ))
        .await?;

    // Execute the snapshot SQL
    let statements = crate::sql_parser::split_statements(&sql);
    let mut objects_restored = 0;
    for stmt in &statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        match client.batch_execute(trimmed).await {
            Ok(()) => objects_restored += 1,
            Err(e) => {
                log::warn!(
                    "Failed to restore statement, continuing; statement={}, error={}",
                    &trimmed[..trimmed.len().min(80)],
                    e
                );
            }
        }
    }

    Ok(RestoreReport {
        snapshot_id: snapshot_id.to_string(),
        objects_restored,
    })
}

/// List available snapshots.
pub fn list_snapshots(snapshot_config: &SnapshotConfig) -> Result<Vec<SnapshotInfo>> {
    let dir = &snapshot_config.directory;
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut snapshots = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "sql") {
            let id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            let meta = entry.metadata()?;
            let created = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                .map(|d| {
                    chrono::DateTime::from_timestamp(d.as_secs() as i64, 0)
                        .unwrap_or_default()
                        .format("%Y-%m-%d %H:%M:%S UTC")
                        .to_string()
                })
                .unwrap_or_default();

            snapshots.push(SnapshotInfo {
                id,
                path,
                size_bytes: meta.len(),
                created,
            });
        }
    }

    snapshots.sort_by(|a, b| b.id.cmp(&a.id)); // Newest first
    Ok(snapshots)
}

/// Take a snapshot of the current schema (dialect-aware entry).
pub async fn execute_snapshot_db(
    client: &DbClient,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
) -> Result<SnapshotReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            execute_snapshot(client.as_postgres()?, config, snapshot_config).await
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => execute_snapshot_mysql(client, config, snapshot_config).await,
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

/// Restore a schema from a snapshot (dialect-aware entry).
pub async fn execute_restore_db(
    client: &DbClient,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
    snapshot_id: &str,
) -> Result<RestoreReport> {
    match client.dialect_kind() {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            execute_restore(client.as_postgres()?, config, snapshot_config, snapshot_id).await
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            execute_restore_mysql(client, config, snapshot_config, snapshot_id).await
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

// ── MySQL snapshot/restore ────────────────────────────────────────────────────
//
// MySQL doesn't get the full schema:: introspection treatment yet. Instead we
// use SHOW CREATE TABLE / SHOW CREATE VIEW as the canonical DDL source. This
// captures: tables (with columns, indexes, constraints, AUTO_INCREMENT,
// ENGINE/CHARSET clauses) and views. It deliberately skips: routines, triggers,
// events. Add those when the underlying use cases need them.

#[cfg(feature = "mysql")]
async fn execute_snapshot_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
) -> Result<SnapshotReport> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let schema_name = client.resolve_schema(&config.migrations.schema).await?;
    let mut conn = pool.get_conn().await?;

    let dir = &snapshot_config.directory;
    std::fs::create_dir_all(dir)?;
    let snapshot_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let sql_path = dir.join(format!("{}.sql", snapshot_id));
    let meta_path = dir.join(format!("{}.json", snapshot_id));

    // Tables (excluding views, which information_schema reports separately
    // but SHOW FULL TABLES bundles together with a Table_type column).
    let tables: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
            (schema_name.as_str(),),
        )
        .await?;

    // Views in dependency-safe alphabetical order (good enough for most cases;
    // cyclic view dependencies aren't allowed by MySQL).
    let views: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
            (schema_name.as_str(),),
        )
        .await?;

    let mut ddl = String::new();
    ddl.push_str(&format!(
        "-- Waypoint MySQL snapshot\n-- database: {}\n-- created: {}\n\n",
        schema_name,
        chrono::Utc::now().to_rfc3339()
    ));

    for table_name in &tables {
        let stmt = format!("SHOW CREATE TABLE `{}`.`{}`", schema_name, table_name);
        let row: Option<(String, String)> = conn.query_first(&stmt).await?;
        if let Some((_, create_sql)) = row {
            ddl.push_str(&format!("-- Table: {}\n", table_name));
            ddl.push_str(&create_sql);
            ddl.push_str(";\n\n");
        }
    }

    for view_name in &views {
        let stmt = format!("SHOW CREATE VIEW `{}`.`{}`", schema_name, view_name);
        // SHOW CREATE VIEW returns (View, Create View, character_set_client, collation_connection)
        let row: Option<(String, String, String, String)> = conn.query_first(&stmt).await?;
        if let Some((_, create_sql, _, _)) = row {
            let create_sql = if snapshot_config.strip_definer_mysql {
                strip_mysql_definer(&create_sql)
            } else {
                create_sql
            };
            ddl.push_str(&format!("-- View: {}\n", view_name));
            ddl.push_str(&create_sql);
            ddl.push_str(";\n\n");
        }
    }

    let objects_captured = tables.len() + views.len();
    std::fs::write(&sql_path, &ddl)?;
    let meta = serde_json::json!({
        "snapshot_id": snapshot_id,
        "engine": "mysql",
        "database": schema_name,
        "objects_captured": objects_captured,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "tables": tables.len(),
        "views": views.len(),
    });
    std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap())?;
    prune_snapshots(dir, snapshot_config.max_snapshots)?;

    Ok(SnapshotReport {
        snapshot_id,
        snapshot_path: sql_path.display().to_string(),
        objects_captured,
    })
}

/// Strip a `DEFINER=...` clause (and an `SQL SECURITY DEFINER` clause, which
/// inherits the missing definer's privileges) from a MySQL `SHOW CREATE VIEW`
/// result. The clause appears between `CREATE ALGORITHM=...` and the `VIEW
/// name` keyword, in the form `DEFINER=`user`@`host``. Stripping it makes the
/// view restore as the current user, which is what callers want for
/// cross-account snapshot restores.
#[cfg(feature = "mysql")]
fn strip_mysql_definer(create_sql: &str) -> String {
    use std::sync::LazyLock;
    // `DEFINER=`u`@`h`` — both identifier parts are quoted with backticks
    // (MySQL escapes backticks inside them by doubling, so [^`]* is safe).
    // We also strip an optional trailing `SQL SECURITY DEFINER` since after
    // removing the definer that clause refers to a now-absent user. Match
    // is case-insensitive (MySQL keywords) and tolerant of extra whitespace.
    static DEFINER_RE: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\s*DEFINER\s*=\s*`[^`]*`@`[^`]*`").unwrap());
    static SECURITY_DEFINER_RE: LazyLock<regex_lite::Regex> =
        LazyLock::new(|| regex_lite::Regex::new(r"(?i)\s+SQL\s+SECURITY\s+DEFINER").unwrap());

    let stripped = DEFINER_RE.replace(create_sql, "");
    SECURITY_DEFINER_RE.replace(&stripped, "").into_owned()
}

#[cfg(feature = "mysql")]
async fn execute_restore_mysql(
    client: &DbClient,
    config: &WaypointConfig,
    snapshot_config: &SnapshotConfig,
    snapshot_id: &str,
) -> Result<RestoreReport> {
    use mysql_async::prelude::*;
    let pool = client.as_mysql()?;
    let schema_name = client.resolve_schema(&config.migrations.schema).await?;
    let sql_path = snapshot_config
        .directory
        .join(format!("{}.sql", snapshot_id));

    if !sql_path.exists() {
        return Err(WaypointError::SnapshotError {
            reason: format!(
                "Snapshot '{}' not found at {}",
                snapshot_id,
                sql_path.display()
            ),
        });
    }

    let sql = std::fs::read_to_string(&sql_path)?;
    let mut conn = pool.get_conn().await?;

    // Make sure we're operating against the right database. Pool URL has it,
    // but USE makes the session unambiguous and protects against connection
    // state quirks across checkout.
    let use_stmt = format!("USE `{}`", schema_name);
    conn.query_drop(&use_stmt).await?;

    // Wipe the database in the same destructive way PG's restore wipes the
    // schema. We disable FK checks to make drops happen in any order.
    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0").await?;
    // Drop views first
    let views: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?",
            (schema_name.as_str(),),
        )
        .await?;
    for v in &views {
        let s = format!("DROP VIEW IF EXISTS `{}`.`{}`", schema_name, v);
        conn.query_drop(&s).await?;
    }
    let tables: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
            (schema_name.as_str(),),
        )
        .await?;
    for t in &tables {
        let s = format!("DROP TABLE IF EXISTS `{}`.`{}`", schema_name, t);
        conn.query_drop(&s).await?;
    }
    // Keep FOREIGN_KEY_CHECKS=0 across the apply phase too: MySQL validates
    // FK references at CREATE TABLE time (error 1822 when the referenced
    // table doesn't exist yet), and snapshots are written in alphabetical
    // order, not FK-dependency order. With FK checks off, MySQL records the
    // constraint without validating that the referenced table exists yet,
    // and forward references resolve once the referenced table is created.
    // We restore FK checks at the end.

    // Apply snapshot. The snapshot is a series of SHOW CREATE TABLE outputs,
    // each terminated with `;`. We use a MySQL-aware splitter that respects
    // backtick-quoted identifiers and string literals.
    let mut objects_restored = 0;
    for stmt in crate::sql_parser::split_mysql_statements(&sql) {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        // MySQL accepts leading `--` comments before a statement, so we don't
        // pre-filter comment-only chunks (the chunk may carry real DDL after
        // the comments). If the chunk is truly comments-only it executes as
        // a no-op.
        match conn.query_drop(trimmed).await {
            Ok(()) => objects_restored += 1,
            Err(e) => {
                log::warn!(
                    "Failed to restore statement, continuing; statement={}, error={}",
                    &trimmed[..trimmed.len().min(80)],
                    e
                );
            }
        }
    }

    // Restore FK checks for subsequent operations on this connection. If this
    // fails, log but don't surface — the snapshot apply already succeeded and
    // the connection is short-lived (returns to the pool on drop).
    if let Err(e) = conn.query_drop("SET FOREIGN_KEY_CHECKS = 1").await {
        log::warn!(
            "Failed to restore FOREIGN_KEY_CHECKS=1 on restore conn: {}",
            e
        );
    }

    Ok(RestoreReport {
        snapshot_id: snapshot_id.to_string(),
        objects_restored,
    })
}

fn prune_snapshots(dir: &PathBuf, max: usize) -> Result<()> {
    let mut sql_files: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sql"))
        .collect();

    sql_files.sort_by_key(|e| {
        e.metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    });

    while sql_files.len() > max {
        if let Some(oldest) = sql_files.first() {
            let sql_path = oldest.path();
            let json_path = sql_path.with_extension("json");
            let _ = std::fs::remove_file(&sql_path);
            let _ = std::fs::remove_file(&json_path);
            sql_files.remove(0);
        }
    }

    Ok(())
}

#[cfg(all(test, feature = "mysql"))]
mod tests_mysql_definer {
    use super::strip_mysql_definer;

    #[test]
    fn strip_typical_show_create_view() {
        let input = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` \
                     SQL SECURITY DEFINER VIEW `v_active_users` AS \
                     select `u`.`id` AS `id` from `users` `u` where `u`.`active` = 1";
        let out = strip_mysql_definer(input);
        assert!(!out.contains("DEFINER"), "definer not stripped: {}", out);
        assert!(
            !out.contains("SQL SECURITY"),
            "SQL SECURITY not stripped: {}",
            out
        );
        // The view body should still be intact.
        assert!(out.contains("VIEW `v_active_users`"));
        assert!(out.contains("from `users`"));
    }

    #[test]
    fn strip_view_with_at_in_host() {
        // Host can be `%` or contain dots — the regex uses [^`]* so any
        // non-backtick char inside the host part is fine.
        let input = "CREATE ALGORITHM=UNDEFINED DEFINER=`app_user`@`10.0.%.%` \
                     SQL SECURITY DEFINER VIEW `v` AS select 1";
        let out = strip_mysql_definer(input);
        assert!(!out.contains("DEFINER"));
        assert!(!out.contains("SQL SECURITY"));
    }

    #[test]
    fn strip_without_security_clause() {
        // Older MySQL or non-default config: no SQL SECURITY clause emitted.
        let input = "CREATE DEFINER=`root`@`localhost` VIEW `v` AS select 1";
        let out = strip_mysql_definer(input);
        assert!(!out.contains("DEFINER"));
        assert_eq!(out, "CREATE VIEW `v` AS select 1");
    }

    #[test]
    fn passthrough_when_no_definer() {
        // SHOW CREATE TABLE output (no DEFINER clause) must be unchanged.
        let input =
            "CREATE TABLE `users` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB";
        assert_eq!(strip_mysql_definer(input), input);
    }

    #[test]
    fn strip_sql_security_invoker_preserved() {
        // We only strip "SQL SECURITY DEFINER". "SQL SECURITY INVOKER" must be left.
        let input = "CREATE DEFINER=`u`@`h` SQL SECURITY INVOKER VIEW `v` AS select 1";
        let out = strip_mysql_definer(input);
        assert!(!out.contains("DEFINER=`u`"));
        assert!(out.contains("SQL SECURITY INVOKER"));
    }
}
