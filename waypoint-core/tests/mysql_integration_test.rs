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

async fn fresh_database(prefix: &str) -> String {
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
    name
}

async fn drop_database(name: &str) {
    let pool = mysql_async::Pool::from_url(root_url()).expect("invalid root URL");
    let mut conn = pool.get_conn().await.expect("connect mysql");
    let _ = conn
        .query_drop(format!("DROP DATABASE IF EXISTS `{}`", name))
        .await;
    drop(conn);
    pool.disconnect().await.ok();
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
    let name = fresh_database("dialect").await;
    let tempdir = tempfile::tempdir().unwrap();
    let config = config_for(&name, tempdir.path().to_path_buf());
    let wp = Waypoint::new(config).await.expect("connect");
    assert_eq!(wp.client().dialect_kind(), DialectKind::Mysql);
    drop_database(&name).await;
}

#[tokio::test]
async fn migrate_creates_history_table_and_applies_versioned_migrations() {
    let name = fresh_database("apply").await;
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

    let config = config_for(&name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");

    let report = wp.migrate(None).await.expect("migrate");
    assert_eq!(report.migrations_applied, 2);
    assert!(report.total_time_ms >= 0);

    // Re-run: should be a no-op
    let report2 = wp.migrate(None).await.expect("migrate again");
    assert_eq!(report2.migrations_applied, 0);

    // Verify the history table contents directly
    let pool = mysql_async::Pool::from_url(db_url(&name)).unwrap();
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

    drop_database(&name).await;
}

#[tokio::test]
async fn migrate_applies_repeatable_only_when_checksum_changes() {
    let name = fresh_database("repeat").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "R__Create_view.sql",
            "CREATE OR REPLACE VIEW v AS SELECT 1 AS x;",
        )],
    );
    let config = config_for(&name, migrations.clone());
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

    drop_database(&name).await;
}

#[tokio::test]
async fn migrate_records_failure_when_sql_is_invalid() {
    let name = fresh_database("fail").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__Bad.sql",
            "CREATE TABLE oops (id INT, NOT VALID THIS SHOULD FAIL);",
        )],
    );
    let config = config_for(&name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    let err = wp.migrate(None).await.expect_err("should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("V1__Bad.sql") || msg.contains("oops"),
        "expected failure to reference the script or table, got: {}",
        msg
    );
    drop_database(&name).await;
}

#[tokio::test]
async fn info_lists_pending_and_applied_states() {
    use waypoint_core::commands::info::MigrationState;
    let name = fresh_database("info").await;
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
    let config = config_for(&name, migrations);
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

    drop_database(&name).await;
}

#[tokio::test]
async fn validate_passes_after_migrate() {
    let name = fresh_database("validate").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[("V1__T.sql", "CREATE TABLE t (id INT PRIMARY KEY);")],
    );
    let config = config_for(&name, migrations.clone());
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

    drop_database(&name).await;
}

#[tokio::test]
async fn baseline_records_a_baseline_row() {
    let name = fresh_database("baseline").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(&migrations, &[]);
    let config = config_for(&name, migrations);
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

    drop_database(&name).await;
}

#[tokio::test]
async fn schema_public_falls_back_to_current_database() {
    // When config.schema is the PG default "public", MySQL paths should fall
    // back to the connection's current database so a PG-shaped config works.
    let name = fresh_database("publicschema").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[("V1__Empty.sql", "CREATE TABLE t (id INT PRIMARY KEY);")],
    );
    let mut config = config_for(&name, migrations);
    config.migrations.schema = "public".to_string(); // simulate PG-default
    let wp = Waypoint::new(config).await.expect("connect");
    let r = wp.migrate(None).await.expect("migrate");
    assert_eq!(r.migrations_applied, 1);
    drop_database(&name).await;
}

#[tokio::test]
async fn clean_drops_all_objects_in_database() {
    let name = fresh_database("clean").await;
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
    let mut config = config_for(&name, migrations);
    config.migrations.clean_enabled = true;
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    // Verify objects exist
    let pool = mysql_async::Pool::from_url(db_url(&name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let before: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
            (name.as_str(),),
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
    let pool = mysql_async::Pool::from_url(db_url(&name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let after: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (name.as_str(),),
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

    drop_database(&name).await;
}

#[tokio::test]
async fn snapshot_captures_tables_and_views_via_show_create() {
    use waypoint_core::commands::snapshot::SnapshotConfig;
    let name = fresh_database("snap").await;
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
    let config = config_for(&name, migrations);
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let snap_dir = tempfile::tempdir().unwrap();
    let snap_config = SnapshotConfig {
        directory: snap_dir.path().to_path_buf(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
    };
    let report = wp.snapshot(&snap_config).await.expect("snapshot");
    // 1 table + 1 view + waypoint_schema_history table = 3 objects
    assert!(report.objects_captured >= 2);

    let snapshot_sql = std::fs::read_to_string(&report.snapshot_path).unwrap();
    assert!(snapshot_sql.contains("CREATE TABLE"));
    assert!(snapshot_sql.contains("`thing`"));
    assert!(snapshot_sql.contains("thing_names"));

    drop_database(&name).await;
}

#[tokio::test]
async fn restore_recreates_schema_from_snapshot() {
    use waypoint_core::commands::snapshot::SnapshotConfig;
    let name = fresh_database("restore").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(
        &migrations,
        &[(
            "V1__T.sql",
            "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));",
        )],
    );
    let mut config = config_for(&name, migrations);
    config.migrations.clean_enabled = true;
    let wp = Waypoint::new(config).await.expect("connect");
    wp.migrate(None).await.expect("migrate");

    let snap_dir = tempfile::tempdir().unwrap();
    let snap_config = SnapshotConfig {
        directory: snap_dir.path().to_path_buf(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
    };
    let report = wp.snapshot(&snap_config).await.expect("snapshot");

    // Clean: blow everything away
    wp.clean(true).await.expect("clean");

    // Verify it's gone
    let pool = mysql_async::Pool::from_url(db_url(&name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let before: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (name.as_str(),),
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
    let pool = mysql_async::Pool::from_url(db_url(&name)).unwrap();
    let mut conn = pool.get_conn().await.unwrap();
    let after: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (name.as_str(),),
        )
        .await
        .unwrap();
    assert!(after.contains(&"users".to_string()));
    drop(conn);
    pool.disconnect().await.ok();

    drop_database(&name).await;
}

#[tokio::test]
async fn clean_refuses_when_disabled_unless_force() {
    let name = fresh_database("cleandis").await;
    let tempdir = tempfile::tempdir().unwrap();
    let migrations = tempdir.path().to_path_buf();
    write_migrations(&migrations, &[]);
    let mut config = config_for(&name, migrations);
    config.migrations.clean_enabled = false;
    let wp = Waypoint::new(config).await.expect("connect");

    // allow_clean=false + clean_enabled=false → CleanDisabled
    let err = wp.clean(false).await.expect_err("should refuse");
    assert!(err.to_string().to_lowercase().contains("clean"));

    // allow_clean=true overrides
    let _ = wp.clean(true).await.expect("clean with allow");
    drop_database(&name).await;
}
