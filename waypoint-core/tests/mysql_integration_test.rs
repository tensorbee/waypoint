//! Integration tests for MySQL 8.0+ support.
//!
//! Requires a running MySQL 8.0+ instance. Defaults to the developer harness
//! container at mysql://root:mysql@127.0.0.1:13306/devdb but can be overridden
//! via `TEST_MYSQL_URL`.
//!
//! Run with:
//!   cargo test --features mysql --test mysql_integration_test
//!
//! Each test isolates itself in a uniquely-named database to avoid collisions.

#![cfg(feature = "mysql")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use mysql_async::prelude::*;
use waypoint_core::config::{DatabaseConfig, HooksConfig, MigrationSettings, WaypointConfig};
use waypoint_core::dialect::DialectKind;
use waypoint_core::Waypoint;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn root_url() -> String {
    std::env::var("TEST_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:mysql@127.0.0.1:13306/mysql".into())
}

fn db_url(database: &str) -> String {
    let root = root_url();
    // Replace the path component with the target database
    if let Some(idx) = root.rfind('/') {
        format!("{}/{}", &root[..idx], database)
    } else {
        format!("{}/{}", root, database)
    }
}

/// RAII guard for a per-test MySQL database. Drop spawns an async cleanup
/// task so the database is dropped even when a test panics — best-effort
/// since we can't `await` from `Drop`. Combined with `DROP DATABASE IF EXISTS`
/// on creation, the next test run cleans up any leaks from earlier runs.
struct TestDb {
    name: String,
}

impl TestDb {
    async fn create(prefix: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("waypoint_test_{}_{}", prefix, id);
        let pool = mysql_async::Pool::from_url(root_url()).expect("invalid root URL");
        let mut conn = pool.get_conn().await.expect("connect mysql");
        conn.query_drop(format!("DROP DATABASE IF EXISTS `{}`", name))
            .await
            .expect("drop db");
        conn.query_drop(format!("CREATE DATABASE `{}`", name))
            .await
            .expect("create db");
        drop(conn);
        pool.disconnect().await.ok();
        TestDb { name }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        // We're typically inside #[tokio::test]'s current-thread runtime. Spawn
        // a fire-and-forget cleanup task — it runs as the runtime unwinds. If
        // the runtime tears down before it completes, the next run's
        // DROP IF EXISTS picks up the orphan.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let pool = match mysql_async::Pool::from_url(root_url()) {
                    Ok(p) => p,
                    Err(_) => return,
                };
                if let Ok(mut conn) = pool.get_conn().await {
                    let _ = conn
                        .query_drop(format!("DROP DATABASE IF EXISTS `{}`", name))
                        .await;
                }
                pool.disconnect().await.ok();
            });
        }
    }
}

/// Convenience: create a fresh test database. Caller MUST bind the returned
/// `TestDb` (e.g. `let db = fresh_database("x").await; let name = db.name();`)
/// so the guard's Drop fires when the test function returns or panics.
async fn fresh_database(prefix: &str) -> TestDb {
    TestDb::create(prefix).await
}

fn write_migrations(dir: &std::path::Path, files: &[(&str, &str)]) {
    std::fs::create_dir_all(dir).unwrap();
    for (name, content) in files {
        std::fs::write(dir.join(name), content).unwrap();
    }
}

fn config_for(db_name: &str, migrations_dir: PathBuf) -> WaypointConfig {
    WaypointConfig {
        database: DatabaseConfig {
            url: Some(db_url(db_name)),
            ..Default::default()
        },
        migrations: MigrationSettings {
            locations: vec![migrations_dir],
            table: "waypoint_schema_history".to_string(),
            schema: db_name.to_string(),
            out_of_order: false,
            validate_on_migrate: false,
            clean_enabled: true,
            baseline_version: "1".to_string(),
            installed_by: None,
            ..Default::default()
        },
        hooks: HooksConfig::default(),
        placeholders: HashMap::new(),
        ..Default::default()
    }
}

#[tokio::test]
async fn dialect_kind_detected_from_url() {
    let db = fresh_database("dialect").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let config = config_for(name, tempdir.path().to_path_buf());
    let wp = Waypoint::new(config).await.expect("connect");
    assert_eq!(wp.client().dialect_kind(), DialectKind::Mysql);
}

#[tokio::test]
async fn migrate_creates_history_table_and_applies_versioned_migrations() {
    let db = fresh_database("apply").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            (
                "V1__Create_users.sql",
                "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL);",
            ),
            (
                "V2__Add_active.sql",
                "ALTER TABLE users ADD COLUMN active TINYINT(1) NOT NULL DEFAULT 1;",
            ),
        ],
    );

    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.migrate(None).await.expect("migrate");
    assert_eq!(report.migrations_applied, 2);
    assert!(report.total_time_ms >= 0);

    // Re-run: should be a no-op
    let report2 = wp.migrate(None).await.expect("migrate again");
    assert_eq!(report2.migrations_applied, 0);

    // Verify the history table contents directly
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let rows: Vec<(i32, Option<String>, String, String, bool)> = conn
        .query(
            "SELECT installed_rank, version, description, type, success \
                FROM waypoint_schema_history ORDER BY installed_rank",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1.as_deref(), Some("1"));
    assert_eq!(rows[0].3, "SQL");
    assert!(rows[0].4);
    assert_eq!(rows[1].1.as_deref(), Some("2"));
    drop(conn);
    pool.disconnect().await.ok();
}

#[tokio::test]
async fn migrate_applies_repeatable_only_when_checksum_changes() {
    let db = fresh_database("repeat").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "R__Create_view.sql",
            "CREATE OR REPLACE VIEW v AS SELECT 1 AS x;",
        )],
    );
    let config = config_for(name, migrations.clone());
    let wp = Waypoint::new(config).await.expect("connect");

    let r1 = wp.migrate(None).await.expect("first apply");
    assert_eq!(r1.migrations_applied, 1);
    let r2 = wp.migrate(None).await.expect("second apply (no change)");
    assert_eq!(r2.migrations_applied, 0);

    // Modify the repeatable migration; checksum changes; should be re-applied
    std::fs::write(
        migrations.join("R__Create_view.sql"),
        "CREATE OR REPLACE VIEW v AS SELECT 2 AS x;",
    )
    .unwrap();
    let r3 = wp.migrate(None).await.expect("third apply after change");
    assert_eq!(r3.migrations_applied, 1);
}

#[tokio::test]
async fn migrate_records_failure_when_sql_is_invalid() {
    let db = fresh_database("fail").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__Bad.sql",
            "CREATE TABLE oops (id INT, NOT VALID THIS SHOULD FAIL);",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    let err = wp.migrate(None).await.expect_err("should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("V1__Bad.sql") || msg.contains("oops"),
        "expected failure to reference the script or table, got: {}",
        msg
    );
}

#[tokio::test]
async fn info_lists_pending_and_applied_states() {
    use waypoint_core::commands::info::MigrationState;
    let db = fresh_database("info").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            ("V1__Done.sql", "CREATE TABLE done (id INT PRIMARY KEY);"),
            (
                "V2__Pending.sql",
                "CREATE TABLE pending (id INT PRIMARY KEY);",
            ),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    // Apply only up to V1
    let applied = wp.migrate(Some("1")).await.expect("migrate to V1");
    assert_eq!(applied.migrations_applied, 1);

    let infos = wp.info().await.expect("info");
    let by_version: std::collections::HashMap<_, _> = infos
        .iter()
        .map(|i| (i.version.clone().unwrap(), i))
        .collect();
    assert_eq!(by_version["1"].state, MigrationState::Applied);
    assert_eq!(by_version["2"].state, MigrationState::Pending);
}

#[tokio::test]
async fn validate_passes_after_migrate() {
    let db = fresh_database("validate").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[("V1__T.sql", "CREATE TABLE t (id INT PRIMARY KEY);")],
    );
    let config = config_for(name, migrations.clone());
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let report = wp.validate().await.expect("validate");
    assert!(report.valid);
    assert!(report.issues.is_empty());

    // Now corrupt: modify the file but leave history checksum stale → validate should fail
    std::fs::write(
        migrations.join("V1__T.sql"),
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50));",
    )
    .unwrap();
    let err = wp.validate().await.expect_err("should fail validation");
    assert!(err.to_string().contains("Checksum mismatch"));

    // Repair should normalise the recorded checksum to match the file
    let repair = wp.repair().await.expect("repair");
    assert_eq!(repair.checksums_updated, 1);

    let report2 = wp.validate().await.expect("validate after repair");
    assert!(report2.valid);
}

#[tokio::test]
async fn baseline_records_a_baseline_row() {
    let db = fresh_database("baseline").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(&migrations, &[]);
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    wp.baseline(Some("5"), Some("imported existing"))
        .await
        .expect("baseline");

    // A second baseline must fail because history is no longer empty
    let err = wp
        .baseline(Some("5"), Some("again"))
        .await
        .expect_err("second baseline");
    assert!(err.to_string().contains("Baseline already exists"));

    // Info should show the baseline row
    let infos = wp.info().await.expect("info");
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].version.as_deref(), Some("5"));
    assert_eq!(infos[0].migration_type, "BASELINE");
}

#[tokio::test]
async fn schema_public_falls_back_to_current_database() {
    // When config.schema is the PG default "public", MySQL paths should fall
    // back to the connection's current database so a PG-shaped config works.
    let db = fresh_database("publicschema").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[("V1__Empty.sql", "CREATE TABLE t (id INT PRIMARY KEY);")],
    );
    let mut config = config_for(name, migrations);
    config.migrations.schema = "public".to_string(); // simulate PG-default
    let wp = Waypoint::new(config).await.expect("connect");
    let r = wp.migrate(None).await.expect("migrate");
    assert_eq!(r.migrations_applied, 1);
}

#[tokio::test]
async fn clean_drops_all_objects_in_database() {
    let db = fresh_database("clean").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            (
                "V1__Tables.sql",
                "CREATE TABLE a (id INT PRIMARY KEY); CREATE TABLE b (id INT PRIMARY KEY);",
            ),
            (
                "V2__View.sql",
                "CREATE OR REPLACE VIEW v AS SELECT id FROM a;",
            ),
        ],
    );
    let mut config = config_for(name, migrations);
    config.migrations.clean_enabled = true;
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    // Verify objects exist
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let before: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
            (name,),
        )
        .await
        .unwrap();
    assert!(before.contains(&"a".to_string()));
    assert!(before.contains(&"b".to_string()));
    drop(conn);
    pool.disconnect().await.ok();

    // Clean
    let dropped = wp.clean(true).await.expect("clean");
    assert!(dropped.iter().any(|d| d.contains("a")));
    assert!(dropped.iter().any(|d| d.contains("b")));
    assert!(dropped.iter().any(|d| d.contains("v")));

    // Verify everything's gone
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let after: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (name,),
        )
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "expected no objects left, got: {:?}",
        after
    );
    drop(conn);
    pool.disconnect().await.ok();
}

#[tokio::test]
async fn snapshot_captures_tables_and_views_via_show_create() {
    use waypoint_core::commands::snapshot::SnapshotConfig;
    let db = fresh_database("snap").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            (
                "V1__T.sql",
                "CREATE TABLE thing (id INT PRIMARY KEY, name VARCHAR(100));",
            ),
            (
                "V2__V.sql",
                "CREATE OR REPLACE VIEW thing_names AS SELECT name FROM thing;",
            ),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let snap_dir = tempfile::tempdir().unwrap();
    let snap_config = SnapshotConfig {
        directory: snap_dir.path().to_path_buf(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
        strip_definer_mysql: true,
    };
    let report = wp.snapshot(&snap_config).await.expect("snapshot");
    // 1 table + 1 view + waypoint_schema_history table = 3 objects
    assert!(report.objects_captured >= 2);

    let snapshot_sql = std::fs::read_to_string(&report.snapshot_path).unwrap();
    assert!(snapshot_sql.contains("CREATE TABLE"));
    assert!(snapshot_sql.contains("`thing`"));
    assert!(snapshot_sql.contains("thing_names"));
    // Default config strips DEFINER from view DDL so cross-account restores
    // don't fail on a missing definer user.
    assert!(
        !snapshot_sql.contains("DEFINER="),
        "expected DEFINER to be stripped from view DDL with default config; \
         snapshot:\n{}",
        snapshot_sql
    );
}

#[tokio::test]
async fn snapshot_preserves_definer_when_strip_disabled() {
    use waypoint_core::commands::snapshot::SnapshotConfig;
    let db = fresh_database("snap_def").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            ("V1__T.sql", "CREATE TABLE rows_t (id INT PRIMARY KEY);"),
            (
                "V2__V.sql",
                "CREATE OR REPLACE VIEW rows_v AS SELECT id FROM rows_t;",
            ),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let snap_dir = tempfile::tempdir().unwrap();
    let snap_config = SnapshotConfig {
        directory: snap_dir.path().to_path_buf(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
        strip_definer_mysql: false,
    };
    let report = wp.snapshot(&snap_config).await.expect("snapshot");
    let snapshot_sql = std::fs::read_to_string(&report.snapshot_path).unwrap();
    // With stripping disabled we should see the DEFINER MySQL emitted on
    // SHOW CREATE VIEW. MySQL always emits one for views, even if the test
    // connection ran the CREATE — the definer becomes the current user.
    assert!(
        snapshot_sql.contains("DEFINER="),
        "expected DEFINER to be preserved with strip_definer_mysql=false; \
         snapshot:\n{}",
        snapshot_sql
    );
}

#[tokio::test]
async fn restore_recreates_schema_from_snapshot() {
    use waypoint_core::commands::snapshot::SnapshotConfig;
    let db = fresh_database("restore").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__T.sql",
            "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));",
        )],
    );
    let mut config = config_for(name, migrations);
    config.migrations.clean_enabled = true;
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let snap_dir = tempfile::tempdir().unwrap();
    let snap_config = SnapshotConfig {
        directory: snap_dir.path().to_path_buf(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
        strip_definer_mysql: true,
    };
    let report = wp.snapshot(&snap_config).await.expect("snapshot");

    // Clean: blow everything away
    wp.clean(true).await.expect("clean");

    // Verify it's gone
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let before: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (name,),
        )
        .await
        .unwrap();
    assert!(before.is_empty());
    drop(conn);
    pool.disconnect().await.ok();

    // Restore from snapshot
    wp.restore(&snap_config, &report.snapshot_id)
        .await
        .expect("restore");

    // users table should be back
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let after: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (name,),
        )
        .await
        .unwrap();
    assert!(after.contains(&"users".to_string()));
    drop(conn);
    pool.disconnect().await.ok();
}

#[tokio::test]
async fn clean_refuses_when_disabled_unless_force() {
    let db = fresh_database("cleandis").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(&migrations, &[]);
    let mut config = config_for(name, migrations);
    config.migrations.clean_enabled = false;
    let wp = Waypoint::new(config).await.expect("connect");

    // allow_clean=false + clean_enabled=false → CleanDisabled
    let err = wp.clean(false).await.expect_err("should refuse");
    assert!(err.to_string().to_lowercase().contains("clean"));

    // allow_clean=true overrides
    let _ = wp.clean(true).await.expect("clean with allow");
}

#[tokio::test]
async fn migrate_runs_lifecycle_hooks() {
    let db = fresh_database("hooks").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    std::fs::create_dir_all(&migrations).unwrap();

    // Hooks: beforeMigrate creates a marker table, afterMigrate inserts into it.
    std::fs::write(
        migrations.join("beforeMigrate.sql"),
        "CREATE TABLE _wp_hook_marker (phase VARCHAR(64), n INT NOT NULL DEFAULT 0);",
    )
    .unwrap();
    std::fs::write(
        migrations.join("beforeEachMigrate.sql"),
        "INSERT INTO _wp_hook_marker (phase, n) VALUES ('before_each', 1);",
    )
    .unwrap();
    std::fs::write(
        migrations.join("afterEachMigrate.sql"),
        "INSERT INTO _wp_hook_marker (phase, n) VALUES ('after_each', 1);",
    )
    .unwrap();
    std::fs::write(
        migrations.join("afterMigrate.sql"),
        "INSERT INTO _wp_hook_marker (phase, n) VALUES ('after', 1);",
    )
    .unwrap();
    std::fs::write(
        migrations.join("V1__T.sql"),
        "CREATE TABLE t (id INT PRIMARY KEY);",
    )
    .unwrap();
    std::fs::write(
        migrations.join("V2__T2.sql"),
        "CREATE TABLE t2 (id INT PRIMARY KEY);",
    )
    .unwrap();

    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.migrate(None).await.expect("migrate with hooks");
    assert_eq!(report.migrations_applied, 2);
    // 1 beforeMigrate + 2 beforeEachMigrate + 2 afterEachMigrate + 1 afterMigrate = 6
    assert_eq!(report.hooks_executed, 6);

    // Verify marker rows
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let rows: Vec<(String, i32)> = conn
        .query("SELECT phase, n FROM _wp_hook_marker ORDER BY phase, n")
        .await
        .unwrap();
    let count = |phase: &str| rows.iter().filter(|(p, _)| p == phase).count();
    assert_eq!(count("before_each"), 2);
    assert_eq!(count("after_each"), 2);
    assert_eq!(count("after"), 1);
    drop(conn);
    pool.disconnect().await.ok();

    // Re-running migrate with nothing pending should NOT fire beforeMigrate /
    // afterMigrate (only fire when there's pending work).
    let report2 = wp.migrate(None).await.expect("migrate no-op");
    assert_eq!(report2.migrations_applied, 0);
    assert_eq!(report2.hooks_executed, 0);
}

#[tokio::test]
async fn simulate_runs_pending_migrations_in_throwaway_db() {
    let db = fresh_database("sim").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            ("V1__Base.sql", "CREATE TABLE base (id INT PRIMARY KEY);"),
            (
                "V2__Add.sql",
                "ALTER TABLE base ADD COLUMN name VARCHAR(50);",
            ),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    // Apply V1 to the source DB so V2 has something to ALTER
    wp.migrate(Some("1")).await.expect("migrate V1");

    // Simulate everything pending (V2)
    let report = wp.simulate().await.expect("simulate");
    assert!(report.passed, "simulation failed: {:?}", report.errors);
    assert_eq!(report.migrations_simulated, 1);
    assert!(report.temp_schema.starts_with("waypoint_sim_"));

    // Verify the temp DB was cleaned up
    let pool = mysql_async::Pool::from_url(root_url()).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let dbs: Vec<String> = conn
        .exec(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
            (report.temp_schema.as_str(),),
        )
        .await
        .unwrap();
    assert!(
        dbs.is_empty(),
        "temp DB should be dropped, found: {:?}",
        dbs
    );
    drop(conn);
    pool.disconnect().await.ok();
}

#[tokio::test]
async fn simulate_reports_sql_errors_without_failing() {
    let db = fresh_database("simerr").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__Bad.sql",
            "CREATE TABLE t (id INT, INVALID GIBBERISH HERE);",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.simulate().await.expect("simulate ran");
    assert!(!report.passed);
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].script, "V1__Bad.sql");
}

#[tokio::test]
async fn preflight_runs_mysql_checks() {
    let db = fresh_database("preflight").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(&migrations, &[]);
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.preflight().await.expect("preflight");
    // 6 checks: read-only, active connections, long queries, replication lag,
    // database size, lock contention. All should pass on the local container.
    assert_eq!(report.checks.len(), 6);
    assert!(report.passed, "preflight failed: {:?}", report.checks);

    // Spot-check that each named check is present
    let names: Vec<&str> = report.checks.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"Read-only"));
    assert!(names.contains(&"Active Connections"));
    assert!(names.contains(&"Long-Running Queries"));
    assert!(names.contains(&"Replication Lag"));
    assert!(names.contains(&"Database Size"));
    assert!(names.contains(&"Lock Contention"));
}

#[tokio::test]
async fn undo_with_manual_u_file_reverts_table() {
    use waypoint_core::commands::undo::UndoTarget;
    let db = fresh_database("undo").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            ("V1__Create.sql", "CREATE TABLE t (id INT PRIMARY KEY);"),
            ("U1__Drop.sql", "DROP TABLE t;"),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    // Confirm t exists
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let before: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't'",
            (name,),
        )
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    drop(conn);
    pool.disconnect().await.ok();

    // Undo last migration
    let report = wp.undo(UndoTarget::Last).await.expect("undo");
    assert_eq!(report.migrations_undone, 1);
    assert_eq!(report.details[0].version, "1");
    assert!(!report.details[0].auto_reversal); // manual U-file

    // t should be gone
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let after: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 't'",
            (name,),
        )
        .await
        .unwrap();
    assert!(after.is_empty());

    // History should record an UNDO_SQL row
    let history_rows: Vec<(String, Option<String>)> = conn
        .exec(
            "SELECT type, version FROM waypoint_schema_history \
             ORDER BY installed_rank",
            (),
        )
        .await
        .unwrap();
    assert_eq!(history_rows.len(), 2);
    assert_eq!(history_rows[1].0, "UNDO_SQL");
    assert_eq!(history_rows[1].1.as_deref(), Some("1"));
    drop(conn);
    pool.disconnect().await.ok();
}

#[tokio::test]
async fn undo_without_u_file_uses_auto_reversal_by_default() {
    use waypoint_core::commands::undo::UndoTarget;
    let db = fresh_database("undomiss").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    // V1 with no corresponding U1 file: auto-reversal kicks in by default.
    write_migrations(
        &migrations,
        &[("V1__Create.sql", "CREATE TABLE t (id INT PRIMARY KEY);")],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let report = wp
        .undo(UndoTarget::Last)
        .await
        .expect("undo via auto-reversal");
    assert_eq!(report.migrations_undone, 1);
    assert!(report.details[0].auto_reversal);
}

#[tokio::test]
async fn undo_errors_when_no_u_file_and_reversals_disabled() {
    use waypoint_core::commands::undo::UndoTarget;
    let db = fresh_database("undonorev").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[("V1__Create.sql", "CREATE TABLE t (id INT PRIMARY KEY);")],
    );
    let mut config = config_for(name, migrations);
    // Disable reversal generation so undo has nothing to fall back to.
    config.reversals.enabled = false;
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let err = wp.undo(UndoTarget::Last).await.expect_err("should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("No undo migration found") || msg.contains("U1__"),
        "expected UndoMissing-style error, got: {}",
        msg
    );
}

#[tokio::test]
async fn undo_falls_back_to_auto_reversal_on_mysql() {
    use waypoint_core::commands::undo::UndoTarget;
    let db = fresh_database("autorev").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    // V1 creates a table — no U1 file. The auto-reversal stored at migrate
    // time should let undo drop the table without manual undo SQL.
    write_migrations(
        &migrations,
        &[(
            "V1__Create.sql",
            "CREATE TABLE auto_rev_target (id INT PRIMARY KEY);",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    wp.migrate(None).await.expect("migrate");

    // Confirm table is there + reversal_sql was populated
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let before: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'auto_rev_target'",
            (name,),
        )
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        1,
        "auto_rev_target should exist after migrate"
    );
    let stored: Option<Option<String>> = conn
        .exec_first(
            "SELECT reversal_sql FROM waypoint_schema_history \
             WHERE version = '1' AND success = 1 ORDER BY installed_rank DESC LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert!(
        stored.flatten().is_some(),
        "reversal_sql should be populated by migrate"
    );
    drop(conn);
    pool.disconnect().await.ok();

    // Undo using the stored auto-reversal (no U1 file exists)
    let report = wp
        .undo(UndoTarget::Last)
        .await
        .expect("undo via auto-reversal");
    assert_eq!(report.migrations_undone, 1);
    assert!(
        report.details[0].auto_reversal,
        "should report auto_reversal=true"
    );

    // Table should be gone
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let after: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'auto_rev_target'",
            (name,),
        )
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "auto_rev_target should be dropped by auto-reversal"
    );
}

// ── Phase 3 analysis-command coverage on MySQL ───────────────────────────────

#[tokio::test]
async fn safety_returns_verdicts_for_pending_migrations() {
    use waypoint_core::safety::SafetyVerdict;
    let db = fresh_database("safety").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[
            (
                "V1__New.sql",
                "CREATE TABLE new_table (id INT PRIMARY KEY);",
            ),
            (
                "V2__Risky.sql",
                "ALTER TABLE new_table ADD COLUMN risky VARCHAR(255) NOT NULL DEFAULT '';",
            ),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.safety().await.expect("safety");
    assert_eq!(
        report.reports.len(),
        2,
        "should analyze both pending migrations"
    );
    // V1 is a CREATE TABLE — pessimistic mapping says LockLevel::None → Safe.
    // V2 is an ALTER TABLE ADD COLUMN — pessimistic worst-case ACCESS EXCLUSIVE
    // but the empty table is Small, so verdict is Caution.
    let v2_report = report
        .reports
        .iter()
        .find(|r| r.script == "V2__Risky.sql")
        .expect("V2 report present");
    assert!(
        matches!(
            v2_report.overall_verdict,
            SafetyVerdict::Caution | SafetyVerdict::Danger
        ),
        "ALTER TABLE on MySQL should be at least Caution, got {:?}",
        v2_report.overall_verdict
    );
}

#[tokio::test]
async fn advise_surfaces_mysql_specific_rules() {
    use waypoint_core::advisor::AdvisorySeverity;
    let db = fresh_database("advise").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    // Three rule-trigger scenarios at once:
    // - M002: table without a primary key
    // - M003: non-utf8mb4 table charset
    // - M004: non-InnoDB storage engine (MyISAM still buildable on 8.4)
    write_migrations(
        &migrations,
        &[(
            "V1__Triggers.sql",
            "CREATE TABLE no_pk (id INT) ENGINE=MyISAM DEFAULT CHARSET=latin1;",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let report = wp.advise().await.expect("advise");
    let rules: std::collections::HashSet<&str> = report
        .advisories
        .iter()
        .map(|a| a.rule_id.as_str())
        .collect();
    assert!(rules.contains("M002"), "M002 (no PK) should fire");
    assert!(rules.contains("M003"), "M003 (non-utf8mb4) should fire");
    assert!(rules.contains("M004"), "M004 (non-InnoDB) should fire");
    // The history table is utf8mb4 + InnoDB + has PK so it shouldn't trigger.
    let warnings: usize = report
        .advisories
        .iter()
        .filter(|a| a.severity == AdvisorySeverity::Warning)
        .count();
    assert!(
        warnings >= 2,
        "expected at least 2 warnings, got {}",
        warnings
    );
}

#[tokio::test]
async fn guards_require_blocks_when_table_missing() {
    let db = fresh_database("guardreq").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    // V1 requires a table that doesn't exist — should fail with GuardFailed.
    write_migrations(
        &migrations,
        &[(
            "V1__Add_email.sql",
            "-- waypoint:require table_exists(\"users\")\n\
             ALTER TABLE users ADD COLUMN email VARCHAR(255);",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let err = wp
        .migrate(None)
        .await
        .expect_err("require guard should fail");
    assert!(
        err.to_string().contains("table_exists") || err.to_string().contains("require"),
        "expected guard failure, got: {}",
        err
    );
}

#[tokio::test]
async fn guards_ensure_passes_when_postcondition_met() {
    let db = fresh_database("guardens").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__Add_users.sql",
            "-- waypoint:ensure table_exists(\"users\")\n\
             -- waypoint:ensure column_exists(\"users\", \"email\")\n\
             CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.migrate(None).await.expect("ensure should pass");
    assert_eq!(report.migrations_applied, 1);
}

#[tokio::test]
async fn diff_detects_added_table_against_empty_target() {
    use waypoint_core::commands::diff::DiffTarget;
    let source = fresh_database("diffsrc").await;
    let target = fresh_database("difftgt").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__T.sql",
            "CREATE TABLE only_in_source (id INT PRIMARY KEY);",
        )],
    );
    let config = config_for(source.name(), migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate source");

    let report = wp
        .diff(DiffTarget::Database(db_url(target.name())))
        .await
        .expect("diff");
    assert!(report.has_changes);
    // Source has `only_in_source` + `waypoint_schema_history`; target is empty.
    // Diff goes source → target so it sees both as TableDropped (from source's
    // perspective they need to be dropped to match target).
    assert!(
        report.diffs.iter().any(
            |d| matches!(d, waypoint_core::schema::SchemaDiff::TableDropped(n)
                if n == "only_in_source")
        ),
        "should see only_in_source as dropped vs empty target: {:?}",
        report.diffs
    );
}

#[tokio::test]
async fn drift_detects_manual_schema_change() {
    let db = fresh_database("drift").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[("V1__T.sql", "CREATE TABLE managed (id INT PRIMARY KEY);")],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    // Add a table outside the migration system.
    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop("CREATE TABLE drifted (id INT PRIMARY KEY)")
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.ok();

    let report = wp.drift().await.expect("drift");
    assert!(report.has_drift);
    assert!(
        report.drifts.iter().any(|d| d.object.contains("drifted")),
        "drift should detect the manually-added table: {:?}",
        report.drifts
    );
}

#[tokio::test]
async fn restore_handles_foreign_keys_across_dependency_order() {
    use waypoint_core::commands::snapshot::SnapshotConfig;
    // Regression: SHOW CREATE TABLE returns tables in alphabetical order, not
    // foreign-key-dependency order. If FOREIGN_KEY_CHECKS=1 during the restore
    // apply, a CREATE TABLE with a forward FK reference fails with error 1822.
    // The fix keeps FK_CHECKS=0 throughout the apply and re-enables at the end.
    let db = fresh_database("fkrestore").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    // `orders` references `users` — and "orders" sorts BEFORE "users"
    // alphabetically, so the snapshot will emit the CREATE TABLE for orders
    // first.
    write_migrations(
        &migrations,
        &[(
            "V1__Schema.sql",
            "CREATE TABLE users (id INT PRIMARY KEY); \
                 CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, \
                   FOREIGN KEY (user_id) REFERENCES users (id));",
        )],
    );
    let mut config = config_for(name, migrations);
    config.migrations.clean_enabled = true;
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let snap_dir = tempfile::tempdir().unwrap();
    let snap_config = SnapshotConfig {
        directory: snap_dir.path().to_path_buf(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
        strip_definer_mysql: true,
    };
    let report = wp.snapshot(&snap_config).await.expect("snapshot");

    wp.clean(true).await.expect("clean");

    // The restore must succeed even though `orders` is created before `users`
    // in the snapshot. Pre-fix this would have errored with 1822 (table
    // doesn't exist) on the orders CREATE TABLE.
    wp.restore(&snap_config, &report.snapshot_id)
        .await
        .expect("restore should handle FK forward-reference");

    let pool = mysql_async::Pool::from_url(db_url(name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let tables: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
            (name,),
        )
        .await
        .unwrap();
    assert!(tables.contains(&"orders".to_string()));
    assert!(tables.contains(&"users".to_string()));
}

#[tokio::test]
async fn migrate_with_options_routes_to_mysql_path() {
    // Regression: the CLI's Migrate handler historically called
    // `migrate::execute_with_options(wp.postgres_client()?, ..., force)` to
    // pass the force flag, which errored on MySQL with "operation is not yet
    // implemented for MySQL." `Waypoint::migrate_with_options` now dispatches
    // on dialect_kind so the CLI works on both engines.
    let db = fresh_database("mwo").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__Init.sql",
            "CREATE TABLE mwo_target (id INT PRIMARY KEY);",
        )],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    // force=true is benign on MySQL (safety analysis doesn't gate migrations
    // there), so we mainly check the call goes through without "operation is
    // not yet implemented for MySQL".
    let report = wp
        .migrate_with_options(None, true)
        .await
        .expect("migrate_with_options should dispatch to MySQL path");
    assert_eq!(report.migrations_applied, 1);
}

#[tokio::test]
async fn explain_classifies_ddl_and_dml() {
    let db = fresh_database("explain").await;
    let name = db.name();
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    // V1 = DDL (not explainable), V2 = DML (explainable) referencing V1's table.
    write_migrations(
        &migrations,
        &[
            (
                "V1__Make.sql",
                "CREATE TABLE explain_t (id INT PRIMARY KEY, name VARCHAR(50));",
            ),
            (
                "V2__Seed.sql",
                "INSERT INTO explain_t (id, name) VALUES (1, 'a');",
            ),
        ],
    );
    let config = config_for(name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    // V1 must be applied first so V2's INSERT has somewhere to EXPLAIN against.
    wp.migrate(Some("1")).await.expect("apply V1");

    let report = wp.explain().await.expect("explain");
    let v2 = report
        .migrations
        .iter()
        .find(|m| m.script == "V2__Seed.sql")
        .expect("V2 explained");
    // INSERT is not DDL.
    assert!(v2.statements.iter().any(|s| !s.is_ddl));
}
