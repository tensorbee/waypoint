//! Integration tests for waypoint-core (PostgreSQL).
//!
//! Requires a running PostgreSQL instance.
//! Set TEST_DATABASE_URL env var, e.g.:
//!   TEST_DATABASE_URL="host=localhost user=postgres dbname=waypoint_test"
//!
//! Run with: cargo test --test integration_test

#![cfg(feature = "postgres")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use waypoint_core::commands::info::MigrationState;
use waypoint_core::commands::snapshot::SnapshotConfig;
use waypoint_core::commands::undo::UndoTarget;
use waypoint_core::config::{DatabaseConfig, HooksConfig, MigrationSettings, WaypointConfig};
use waypoint_core::db::{self, quote_ident};
use waypoint_core::dependency::DependencyGraph;
use waypoint_core::history;
use waypoint_core::migration::{scan_migrations, MigrationVersion};
use waypoint_core::safety::SafetyVerdict;
use waypoint_core::Waypoint;

fn get_test_url() -> String {
    std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set for integration tests")
}

/// Build a config pointing at a unique schema to isolate test runs.
fn test_config(schema: &str, migrations_dir: &str) -> WaypointConfig {
    WaypointConfig {
        database: DatabaseConfig {
            url: Some(get_test_url()),
            ..Default::default()
        },
        migrations: MigrationSettings {
            locations: vec![PathBuf::from(migrations_dir)],
            table: "waypoint_schema_history".to_string(),
            schema: schema.to_string(),
            out_of_order: false,
            validate_on_migrate: false, // disable for most tests
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

/// Helper: connect, create a fresh schema, return client + schema name.
async fn setup_schema(prefix: &str) -> (tokio_postgres::Client, String) {
    let url = get_test_url();
    let client = db::connect(&url).await.expect("Failed to connect to DB");

    // Use a unique schema name per test to avoid collisions
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let schema = format!("waypoint_test_{}_{}", prefix, id);

    // Drop any stale schema from a previous run, then create fresh
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE; CREATE SCHEMA {}",
            quote_ident(&schema),
            quote_ident(&schema)
        ))
        .await
        .expect("Failed to create test schema");

    (client, schema)
}

/// Helper: drop the test schema.
async fn teardown_schema(client: &tokio_postgres::Client, schema: &str) {
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(schema)
        ))
        .await;
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a temporary migrations directory with given files.
fn create_temp_migrations(files: &[(&str, &str)]) -> TempDir {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_migrations_{}_{}",
        std::process::id(),
        id
    ));
    std::fs::create_dir_all(&dir).unwrap();

    for (name, content) in files {
        std::fs::write(dir.join(name), content).unwrap();
    }

    TempDir(dir)
}

/// Temp directory wrapper that cleans up on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ─── Tests ───

#[tokio::test]
async fn test_migrate_applies_versioned_migrations() {
    let (client, schema) = setup_schema("migrate_v").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__Create_things.sql",
            &format!(
                "CREATE TABLE {}.things (id SERIAL PRIMARY KEY, name TEXT);",
                schema
            ),
        ),
        (
            "V2__Add_value.sql",
            &format!("ALTER TABLE {}.things ADD COLUMN value INTEGER;", schema),
        ),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config, client);

    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 2);
    assert_eq!(report.details.len(), 2);
    assert_eq!(report.details[0].version.as_deref(), Some("1"));
    assert_eq!(report.details[1].version.as_deref(), Some("2"));

    // Run migrate again — should be no-op
    let report2 = wp.migrate(None).await.expect("second migrate failed");
    assert_eq!(report2.migrations_applied, 0);

    // Verify table exists by querying it
    let conn = db::connect(&get_test_url()).await.unwrap();
    let rows = conn
        .query(&format!("SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = 'things' ORDER BY ordinal_position", schema), &[])
        .await
        .unwrap();
    let columns: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert!(columns.contains(&"id".to_string()));
    assert!(columns.contains(&"name".to_string()));
    assert!(columns.contains(&"value".to_string()));

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_migrate_applies_repeatable_and_reapplies_on_change() {
    let (client, schema) = setup_schema("migrate_r").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_rep_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Write initial versioned + repeatable
    std::fs::write(
        dir.join("V1__Create_items.sql"),
        format!("CREATE TABLE {}.items (id SERIAL PRIMARY KEY);", schema),
    )
    .unwrap();
    std::fs::write(
        dir.join("R__Items_view.sql"),
        format!(
            "CREATE OR REPLACE VIEW {}.items_view AS SELECT id FROM {}.items;",
            schema, schema
        ),
    )
    .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 2); // V1 + R

    // Now modify the repeatable
    std::fs::write(
        dir.join("R__Items_view.sql"),
        format!(
            "CREATE OR REPLACE VIEW {}.items_view AS SELECT id FROM {}.items WHERE id > 0;",
            schema, schema
        ),
    )
    .unwrap();

    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config, client2);

    let report2 = wp2.migrate(None).await.expect("second migrate failed");
    assert_eq!(report2.migrations_applied, 1); // Only R re-applied

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_info_shows_correct_states() {
    let (client, schema) = setup_schema("info").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__First.sql",
            &format!("CREATE TABLE {}.info_test (id SERIAL);", schema),
        ),
        (
            "V2__Second.sql",
            &format!("ALTER TABLE {}.info_test ADD COLUMN name TEXT;", schema),
        ),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    // Apply only V1 using target
    let wp = Waypoint::with_client(config.clone(), client);

    let report = wp.migrate(Some("1")).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 1);

    // Now check info
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config, client2);
    let infos = wp2.info().await.expect("info failed");

    assert_eq!(infos.len(), 2);
    assert_eq!(infos[0].state, MigrationState::Applied);
    assert_eq!(infos[0].version.as_deref(), Some("1"));
    assert_eq!(infos[1].state, MigrationState::Pending);
    assert_eq!(infos[1].version.as_deref(), Some("2"));

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_validate_detects_checksum_mismatch() {
    let (client, schema) = setup_schema("validate").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_val_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    std::fs::write(
        dir.join("V1__Create_val.sql"),
        format!("CREATE TABLE {}.val_test (id SERIAL);", schema),
    )
    .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);
    wp.migrate(None).await.expect("migrate failed");

    // Modify the file after migration
    std::fs::write(
        dir.join("V1__Create_val.sql"),
        format!("CREATE TABLE {}.val_test (id SERIAL, extra TEXT);", schema),
    )
    .unwrap();

    // Validate should fail
    let mut config2 = config;
    config2.migrations.validate_on_migrate = false;
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config2, client2);

    let result = wp2.validate().await;
    assert!(result.is_err(), "validate should fail on checksum mismatch");

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_repair_removes_failed_and_updates_checksums() {
    let (client, schema) = setup_schema("repair").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_rep2_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    std::fs::write(
        dir.join("V1__Good.sql"),
        format!("CREATE TABLE {}.repair_test (id SERIAL);", schema),
    )
    .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());

    // Apply V1
    let wp = Waypoint::with_client(config.clone(), client);
    wp.migrate(None).await.expect("migrate failed");

    // Manually insert a failed row
    let client2 = db::connect(&get_test_url()).await.unwrap();
    history::insert_applied_migration(
        &client2,
        &schema,
        "waypoint_schema_history",
        Some("2"),
        "Bad migration",
        "SQL",
        "V2__Bad.sql",
        Some(12345),
        "test",
        0,
        false,
    )
    .await
    .unwrap();

    // Now modify V1 file to trigger checksum update
    std::fs::write(
        dir.join("V1__Good.sql"),
        format!(
            "CREATE TABLE {}.repair_test (id SERIAL PRIMARY KEY);",
            schema
        ),
    )
    .unwrap();

    let client3 = db::connect(&get_test_url()).await.unwrap();
    let wp3 = Waypoint::with_client(config, client3);
    let report = wp3.repair().await.expect("repair failed");

    assert_eq!(report.failed_removed, 1);
    assert_eq!(report.checksums_updated, 1);

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_baseline_inserts_baseline_row() {
    let (client, schema) = setup_schema("baseline").await;

    let migrations = create_temp_migrations(&[]);
    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    wp.baseline(Some("3"), None).await.expect("baseline failed");

    // Check that baseline row exists
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let applied = history::get_applied_migrations(&client2, &schema, "waypoint_schema_history")
        .await
        .unwrap();

    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version.as_deref(), Some("3"));
    assert_eq!(applied[0].migration_type, "BASELINE");
    assert!(applied[0].success);

    // Second baseline should fail
    let wp2 = Waypoint::with_client(config, client2);
    let result = wp2.baseline(None, None).await;
    assert!(result.is_err(), "second baseline should fail");

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_baseline_prevents_old_migrations() {
    let (client, schema) = setup_schema("baseline_skip").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__Old.sql",
            &format!("CREATE TABLE {}.old_tbl (id SERIAL);", schema),
        ),
        (
            "V2__Also_old.sql",
            &format!("CREATE TABLE {}.also_old_tbl (id SERIAL);", schema),
        ),
        (
            "V3__New.sql",
            &format!("CREATE TABLE {}.new_tbl (id SERIAL);", schema),
        ),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    // Baseline at version 2
    wp.baseline(Some("2"), None).await.expect("baseline failed");

    // Migrate — should only apply V3
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config, client2);
    let report = wp2.migrate(None).await.expect("migrate failed");

    assert_eq!(report.migrations_applied, 1);
    assert_eq!(report.details[0].version.as_deref(), Some("3"));

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_clean_drops_everything() {
    let (client, schema) = setup_schema("clean").await;

    let migrations = create_temp_migrations(&[(
        "V1__Create_clean_test.sql",
        &format!("CREATE TABLE {}.clean_tbl (id SERIAL PRIMARY KEY);", schema),
    )]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);
    wp.migrate(None).await.expect("migrate failed");

    // Verify table exists
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let exists = client2
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'clean_tbl')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(exists.get::<_, bool>(0));

    let wp2 = Waypoint::with_client(config, client2);
    let dropped = wp2.clean(true).await.expect("clean failed");
    assert!(!dropped.is_empty());

    // Verify table is gone
    let client3 = db::connect(&get_test_url()).await.unwrap();
    let exists2 = client3
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'clean_tbl')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(!exists2.get::<_, bool>(0));

    teardown_schema(&client3, &schema).await;
}

#[tokio::test]
async fn test_clean_disabled_by_default() {
    let (client, schema) = setup_schema("clean_dis").await;

    let migrations = create_temp_migrations(&[]);
    let mut config = test_config(&schema, migrations.path().to_str().unwrap());
    config.migrations.clean_enabled = false;

    let wp = Waypoint::with_client(config, client);
    let result = wp.clean(false).await;
    assert!(result.is_err(), "clean should fail when disabled");

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_out_of_order_rejected_by_default() {
    let (client, schema) = setup_schema("ooo").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_ooo_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // First apply V2 only
    std::fs::write(
        dir.join("V2__Second.sql"),
        format!("CREATE TABLE {}.ooo_tbl (id SERIAL);", schema),
    )
    .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);
    wp.migrate(None).await.expect("migrate V2 failed");

    // Now add V1 and try to migrate — should fail
    std::fs::write(
        dir.join("V1__First.sql"),
        format!("CREATE TABLE {}.ooo_first (id SERIAL);", schema),
    )
    .unwrap();

    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config, client2);
    let result = wp2.migrate(None).await;
    assert!(result.is_err(), "out-of-order should be rejected");

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_out_of_order_allowed_when_enabled() {
    let (client, schema) = setup_schema("ooo_ok").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_ooo_ok_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // First apply V2 only
    std::fs::write(
        dir.join("V2__Second.sql"),
        format!("CREATE TABLE {}.ooo_ok_tbl (id SERIAL);", schema),
    )
    .unwrap();

    let mut config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);
    wp.migrate(None).await.expect("migrate V2 failed");

    // Now add V1 and try with out_of_order enabled
    std::fs::write(
        dir.join("V1__First.sql"),
        format!("CREATE TABLE {}.ooo_ok_first (id SERIAL);", schema),
    )
    .unwrap();

    config.migrations.out_of_order = true;
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config, client2);
    let report = wp2
        .migrate(None)
        .await
        .expect("out-of-order migrate should succeed");
    assert_eq!(report.migrations_applied, 1);
    assert_eq!(report.details[0].version.as_deref(), Some("1"));

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_target_version_limits_migration() {
    let (client, schema) = setup_schema("target").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__One.sql",
            &format!("CREATE TABLE {}.t1 (id SERIAL);", schema),
        ),
        (
            "V2__Two.sql",
            &format!("CREATE TABLE {}.t2 (id SERIAL);", schema),
        ),
        (
            "V3__Three.sql",
            &format!("CREATE TABLE {}.t3 (id SERIAL);", schema),
        ),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config, client);

    let report = wp.migrate(Some("2")).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 2);
    assert_eq!(report.details.last().unwrap().version.as_deref(), Some("2"));

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

// ─── New Tests ───

#[tokio::test]
async fn test_undo_manual_u_file() {
    let (client, schema) = setup_schema("undo_manual").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__Create_undo_tbl.sql",
            &format!("CREATE TABLE {}.undo_tbl (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V2__Add_col.sql",
            &format!("ALTER TABLE {}.undo_tbl ADD COLUMN name TEXT;", schema),
        ),
        (
            "U2__Add_col.sql",
            &format!("ALTER TABLE {}.undo_tbl DROP COLUMN name;", schema),
        ),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    // Apply both versioned migrations
    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 2);

    // Undo the last migration (V2)
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config.clone(), client2);
    let undo_report = wp2.undo(UndoTarget::Last).await.expect("undo failed");
    assert_eq!(undo_report.migrations_undone, 1);
    assert_eq!(undo_report.details[0].version, "2");
    assert!(!undo_report.details[0].auto_reversal);

    // Verify only V1 is effectively applied via info
    let client3 = db::connect(&get_test_url()).await.unwrap();
    let wp3 = Waypoint::with_client(config, client3);
    let infos = wp3.info().await.expect("info failed");
    // V1 should be applied, V2 should be pending (since it was undone)
    let applied: Vec<_> = infos
        .iter()
        .filter(|i| i.state == MigrationState::Applied)
        .collect();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version.as_deref(), Some("1"));

    // Verify the column was actually dropped
    let conn = db::connect(&get_test_url()).await.unwrap();
    let rows = conn
        .query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'undo_tbl' ORDER BY ordinal_position",
            &[&schema],
        )
        .await
        .unwrap();
    let columns: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert!(columns.contains(&"id".to_string()));
    assert!(!columns.contains(&"name".to_string()));

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_undo_with_count() {
    let (client, schema) = setup_schema("undo_count").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__First.sql",
            &format!("CREATE TABLE {}.uc_t1 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V2__Second.sql",
            &format!("CREATE TABLE {}.uc_t2 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V3__Third.sql",
            &format!("CREATE TABLE {}.uc_t3 (id SERIAL PRIMARY KEY);", schema),
        ),
        ("U3__Third.sql", &format!("DROP TABLE {}.uc_t3;", schema)),
        ("U2__Second.sql", &format!("DROP TABLE {}.uc_t2;", schema)),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 3);

    // Undo the last 2 migrations
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config.clone(), client2);
    let undo_report = wp2
        .undo(UndoTarget::Count(2))
        .await
        .expect("undo count=2 failed");
    assert_eq!(undo_report.migrations_undone, 2);

    // Verify only V1 remains effectively applied
    let client3 = db::connect(&get_test_url()).await.unwrap();
    let applied = history::get_applied_migrations(&client3, &schema, "waypoint_schema_history")
        .await
        .unwrap();
    let effective = history::effective_applied_versions(&applied);
    assert!(effective.contains("1"));
    assert!(!effective.contains("2"));
    assert!(!effective.contains("3"));

    teardown_schema(&client3, &schema).await;
}

#[tokio::test]
async fn test_undo_to_target_version() {
    let (client, schema) = setup_schema("undo_target").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__First.sql",
            &format!("CREATE TABLE {}.ut_t1 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V2__Second.sql",
            &format!("CREATE TABLE {}.ut_t2 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V3__Third.sql",
            &format!("CREATE TABLE {}.ut_t3 (id SERIAL PRIMARY KEY);", schema),
        ),
        ("U3__Third.sql", &format!("DROP TABLE {}.ut_t3;", schema)),
        ("U2__Second.sql", &format!("DROP TABLE {}.ut_t2;", schema)),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 3);

    // Undo to version "1" — V2 and V3 should be undone
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config.clone(), client2);
    let target = MigrationVersion::parse("1").unwrap();
    let undo_report = wp2
        .undo(UndoTarget::Version(target))
        .await
        .expect("undo to target failed");
    assert_eq!(undo_report.migrations_undone, 2);

    // Verify effective state
    let client3 = db::connect(&get_test_url()).await.unwrap();
    let applied = history::get_applied_migrations(&client3, &schema, "waypoint_schema_history")
        .await
        .unwrap();
    let effective = history::effective_applied_versions(&applied);
    assert!(effective.contains("1"), "V1 should remain applied");
    assert!(!effective.contains("2"), "V2 should be undone");
    assert!(!effective.contains("3"), "V3 should be undone");

    teardown_schema(&client3, &schema).await;
}

#[tokio::test]
async fn test_batch_transaction_mode() {
    let (client, schema) = setup_schema("batch").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__Batch_one.sql",
            &format!("CREATE TABLE {}.batch_t1 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V2__Batch_two.sql",
            &format!("CREATE TABLE {}.batch_t2 (id SERIAL PRIMARY KEY);", schema),
        ),
    ]);

    let mut config = test_config(&schema, migrations.path().to_str().unwrap());
    config.migrations.batch_transaction = true;

    let wp = Waypoint::with_client(config.clone(), client);
    let report = wp.migrate(None).await.expect("batch migrate failed");
    assert_eq!(report.migrations_applied, 2);

    // Verify both tables exist
    let conn = db::connect(&get_test_url()).await.unwrap();
    for tbl in &["batch_t1", "batch_t2"] {
        let exists = conn
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                &[&schema, tbl],
            )
            .await
            .unwrap();
        assert!(exists.get::<_, bool>(0), "Table {} should exist", tbl);
    }

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_batch_transaction_rollback_on_failure() {
    let (client, schema) = setup_schema("batch_fail").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__Good.sql",
            &format!("CREATE TABLE {}.batch_ok (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V2__Bad.sql",
            // Invalid SQL — this is not a valid SQL command
            "THIS IS NOT VALID SQL;",
        ),
    ]);

    let mut config = test_config(&schema, migrations.path().to_str().unwrap());
    config.migrations.batch_transaction = true;

    let wp = Waypoint::with_client(config.clone(), client);
    let result = wp.migrate(None).await;
    assert!(result.is_err(), "batch migrate should fail on bad SQL");

    // Verify V1 table does NOT exist — the whole batch should have been rolled back
    let conn = db::connect(&get_test_url()).await.unwrap();
    let exists = conn
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'batch_ok')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(
        !exists.get::<_, bool>(0),
        "Table batch_ok should NOT exist after batch rollback"
    );

    // Verify no history rows were committed (the batch rolled everything back)
    // The history table might not even exist; that's fine.
    let history_exists = conn
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'waypoint_schema_history')",
            &[&schema],
        )
        .await
        .unwrap();
    if history_exists.get::<_, bool>(0) {
        let applied = history::get_applied_migrations(&conn, &schema, "waypoint_schema_history")
            .await
            .unwrap();
        let effective = history::effective_applied_versions(&applied);
        assert!(
            effective.is_empty(),
            "No migrations should be effectively applied after batch rollback"
        );
    }

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_environment_scoping() {
    let (client, schema) = setup_schema("env_scope").await;

    let migrations = create_temp_migrations(&[
        (
            "V1__Prod_only.sql",
            &format!(
                "-- waypoint:env prod\nCREATE TABLE {}.env_prod (id SERIAL PRIMARY KEY);",
                schema
            ),
        ),
        (
            "V2__All_envs.sql",
            &format!("CREATE TABLE {}.env_all (id SERIAL PRIMARY KEY);", schema),
        ),
    ]);

    let mut config = test_config(&schema, migrations.path().to_str().unwrap());
    config.migrations.environment = Some("dev".to_string());

    let wp = Waypoint::with_client(config.clone(), client);
    let report = wp.migrate(None).await.expect("migrate failed");

    // Only V2 should have been applied (V1 is prod-only, we're running in dev)
    assert_eq!(report.migrations_applied, 1);
    assert_eq!(report.details[0].version.as_deref(), Some("2"));

    // Verify that only env_all table exists, not env_prod
    let conn = db::connect(&get_test_url()).await.unwrap();
    let exists_prod = conn
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'env_prod')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(
        !exists_prod.get::<_, bool>(0),
        "env_prod table should NOT exist in dev environment"
    );

    let exists_all = conn
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'env_all')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(exists_all.get::<_, bool>(0), "env_all table should exist");

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_placeholders() {
    let (client, schema) = setup_schema("placeholders").await;

    let migrations = create_temp_migrations(&[(
        "V1__Create_with_placeholder.sql",
        &format!(
            "CREATE TABLE {}.{} (id SERIAL PRIMARY KEY, name TEXT);",
            schema, "${mytable}"
        ),
    )]);

    let mut config = test_config(&schema, migrations.path().to_str().unwrap());
    config
        .placeholders
        .insert("mytable".to_string(), "placeholder_tbl".to_string());

    let wp = Waypoint::with_client(config, client);
    let report = wp
        .migrate(None)
        .await
        .expect("migrate with placeholders failed");
    assert_eq!(report.migrations_applied, 1);

    // Verify the table was created with the replaced name
    let conn = db::connect(&get_test_url()).await.unwrap();
    let exists = conn
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'placeholder_tbl')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(
        exists.get::<_, bool>(0),
        "placeholder_tbl should exist after placeholder substitution"
    );

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_hooks_before_migrate() {
    let (client, schema) = setup_schema("hooks").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_hooks_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Create a beforeMigrate hook that creates a log table
    std::fs::write(
        dir.join("beforeMigrate.sql"),
        format!(
            "CREATE TABLE IF NOT EXISTS {}.hook_log (created_at TIMESTAMP DEFAULT now());",
            schema
        ),
    )
    .unwrap();

    // Create a regular migration
    std::fs::write(
        dir.join("V1__Main.sql"),
        format!(
            "CREATE TABLE {}.hooks_main (id SERIAL PRIMARY KEY);",
            schema
        ),
    )
    .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config, client);

    let report = wp.migrate(None).await.expect("migrate with hooks failed");
    assert_eq!(report.migrations_applied, 1);
    assert!(report.hooks_executed > 0, "hooks should have been executed");

    // Verify the hook ran by checking the log table exists
    let conn = db::connect(&get_test_url()).await.unwrap();
    let exists = conn
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'hook_log')",
            &[&schema],
        )
        .await
        .unwrap();
    assert!(
        exists.get::<_, bool>(0),
        "hook_log table should exist (created by beforeMigrate hook)"
    );

    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_dependency_ordering() {
    // Test the DependencyGraph topological sort API directly with
    // migration files that have depends directives.
    let migrations = create_temp_migrations(&[
        ("V1__Base.sql", "-- No deps\nSELECT 1;"),
        ("V2__Depends_on_V1.sql", "-- waypoint:depends 1\nSELECT 1;"),
        (
            "V3__Also_depends_on_V1.sql",
            "-- waypoint:depends 1\nSELECT 1;",
        ),
    ]);

    let resolved = scan_migrations(&[migrations.path().to_path_buf()]).expect("scan failed");
    let refs: Vec<&_> = resolved.iter().filter(|m| m.is_versioned()).collect();

    // Build dependency graph without implicit chaining
    let graph = DependencyGraph::build(&refs, false).expect("graph build failed");
    let order = graph.topological_sort().expect("topo sort failed");

    // V1 must come before V2 and V3
    let pos_v1 = order.iter().position(|v| v == "1").unwrap();
    let pos_v2 = order.iter().position(|v| v == "2").unwrap();
    let pos_v3 = order.iter().position(|v| v == "3").unwrap();
    assert!(
        pos_v1 < pos_v2,
        "V1 (pos {}) should be before V2 (pos {})",
        pos_v1,
        pos_v2
    );
    assert!(
        pos_v1 < pos_v3,
        "V1 (pos {}) should be before V3 (pos {})",
        pos_v1,
        pos_v3
    );
}

#[tokio::test]
async fn test_snapshot_and_drift() {
    let (client, schema) = setup_schema("snap_drift").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_snap_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // Use unqualified table names so drift detection can replay in temp schema
    // (drift sets search_path to a temp schema, so schema-qualified names break it).
    // Use INTEGER instead of SERIAL to avoid sequence-default drift between schemas.
    std::fs::write(
        dir.join("V1__Create_snap_tbl.sql"),
        "CREATE TABLE snap_tbl (id INTEGER NOT NULL, name TEXT);",
    )
    .unwrap();

    // Set search_path to our test schema so unqualified names resolve correctly
    client
        .batch_execute(&format!("SET search_path TO {}", quote_ident(&schema)))
        .await
        .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);
    wp.migrate(None).await.expect("migrate V1 failed");

    // Take a snapshot
    let snap_dir = std::env::temp_dir().join(format!(
        "waypoint_test_snapshots_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let snap_config = SnapshotConfig {
        directory: snap_dir.clone(),
        auto_snapshot_on_migrate: false,
        max_snapshots: 10,
        strip_definer_mysql: true,
    };

    let client2 = db::connect(&get_test_url()).await.unwrap();
    client2
        .batch_execute(&format!("SET search_path TO {}", quote_ident(&schema)))
        .await
        .unwrap();
    let wp2 = Waypoint::with_client(config.clone(), client2);
    let snap_report = wp2.snapshot(&snap_config).await.expect("snapshot failed");
    assert!(snap_report.objects_captured > 0);

    // Now add V2 to alter the table (unqualified)
    std::fs::write(
        dir.join("V2__Add_email.sql"),
        "ALTER TABLE snap_tbl ADD COLUMN email TEXT;",
    )
    .unwrap();

    let client3 = db::connect(&get_test_url()).await.unwrap();
    client3
        .batch_execute(&format!("SET search_path TO {}", quote_ident(&schema)))
        .await
        .unwrap();
    let wp3 = Waypoint::with_client(config.clone(), client3);
    wp3.migrate(None).await.expect("migrate V2 failed");

    // Run drift detection — should detect no drift since migrations match DB
    let client4 = db::connect(&get_test_url()).await.unwrap();
    let wp4 = Waypoint::with_client(config.clone(), client4);
    let drift_report = wp4.drift().await.expect("drift detection failed");
    assert!(
        !drift_report.has_drift,
        "No drift should be detected when DB matches migrations"
    );

    // Now introduce manual drift by adding a column outside migrations
    let client5 = db::connect(&get_test_url()).await.unwrap();
    client5
        .batch_execute(&format!(
            "ALTER TABLE {}.snap_tbl ADD COLUMN extra_col TEXT;",
            schema
        ))
        .await
        .expect("manual ALTER failed");

    // Drift detection should now find something
    let wp5 = Waypoint::with_client(config, client5);
    let drift_report2 = wp5.drift().await.expect("drift detection failed");
    assert!(
        drift_report2.has_drift,
        "Drift should be detected after manual schema change"
    );

    // Cleanup
    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&snap_dir);
}

#[tokio::test]
async fn test_safety_analysis_drop_table() {
    let (client, schema) = setup_schema("safety").await;

    // Create a table first so DROP TABLE can be analyzed
    client
        .batch_execute(&format!(
            "CREATE TABLE {}.safety_tbl (id SERIAL PRIMARY KEY);",
            schema
        ))
        .await
        .expect("create table failed");

    let migrations = create_temp_migrations(&[(
        "V1__Drop_table.sql",
        &format!("DROP TABLE {}.safety_tbl;", schema),
    )]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config, client);

    let safety_report = wp.safety().await.expect("safety analysis failed");

    // DROP TABLE should produce at minimum a Caution verdict (AccessExclusiveLock on small table)
    assert!(
        safety_report.overall_verdict >= SafetyVerdict::Caution,
        "DROP TABLE should be at least Caution, got {:?}",
        safety_report.overall_verdict
    );

    // Verify data_loss is detected in one of the statements
    let has_data_loss = safety_report
        .reports
        .iter()
        .flat_map(|r| &r.statements)
        .any(|s| s.data_loss);
    assert!(has_data_loss, "DROP TABLE should be flagged as data loss");

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_advisor_detects_table_without_pk() {
    let (client, schema) = setup_schema("advisor").await;

    // Create a table WITHOUT a primary key
    client
        .batch_execute(&format!(
            "CREATE TABLE {}.no_pk_tbl (name TEXT, value INTEGER);",
            schema
        ))
        .await
        .expect("create table failed");

    let migrations = create_temp_migrations(&[]);
    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config, client);

    let advisor_report = wp.advise().await.expect("advise failed");

    // Should detect A004: table without primary key
    let a004 = advisor_report
        .advisories
        .iter()
        .find(|a| a.rule_id == "A004");
    assert!(
        a004.is_some(),
        "A004 (table without PK) should be detected. Found rules: {:?}",
        advisor_report
            .advisories
            .iter()
            .map(|a| &a.rule_id)
            .collect::<Vec<_>>()
    );
    let advisory = a004.unwrap();
    assert_eq!(advisory.object, "no_pk_tbl");

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_advisory_lock_prevents_concurrent_access() {
    let (client, schema) = setup_schema("lock").await;

    let table = "waypoint_schema_history";

    // Acquire the advisory lock on the first connection
    db::acquire_advisory_lock(&client, table)
        .await
        .expect("first lock acquire failed");

    // Try to acquire the same lock on a second connection with a short timeout
    let client2 = db::connect(&get_test_url()).await.unwrap();
    let result = db::acquire_advisory_lock_with_timeout(&client2, table, 2).await;
    assert!(
        result.is_err(),
        "Second lock acquire should fail (timeout) while first holds it"
    );

    // Release the first lock
    db::release_advisory_lock(&client, table)
        .await
        .expect("release failed");

    // Now the second client should be able to acquire it
    let result2 = db::acquire_advisory_lock_with_timeout(&client2, table, 5).await;
    assert!(
        result2.is_ok(),
        "Second lock acquire should succeed after release"
    );

    // Clean up
    db::release_advisory_lock(&client2, table)
        .await
        .expect("cleanup release failed");

    teardown_schema(&client, &schema).await;
}

#[tokio::test]
async fn test_dotted_version_numbers() {
    let (client, schema) = setup_schema("dotted").await;

    let migrations = create_temp_migrations(&[
        (
            "V1.0__First.sql",
            &format!("CREATE TABLE {}.dot_t1 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V1.1__Second.sql",
            &format!("CREATE TABLE {}.dot_t2 (id SERIAL PRIMARY KEY);", schema),
        ),
        (
            "V2.0__Third.sql",
            &format!("CREATE TABLE {}.dot_t3 (id SERIAL PRIMARY KEY);", schema),
        ),
    ]);

    let config = test_config(&schema, migrations.path().to_str().unwrap());
    let wp = Waypoint::with_client(config, client);

    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 3);

    // Verify ordering: 1.0, 1.1, 2.0
    assert_eq!(report.details[0].version.as_deref(), Some("1.0"));
    assert_eq!(report.details[1].version.as_deref(), Some("1.1"));
    assert_eq!(report.details[2].version.as_deref(), Some("2.0"));

    // Verify all three tables exist
    let conn = db::connect(&get_test_url()).await.unwrap();
    for tbl in &["dot_t1", "dot_t2", "dot_t3"] {
        let exists = conn
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                &[&schema, tbl],
            )
            .await
            .unwrap();
        assert!(exists.get::<_, bool>(0), "Table {} should exist", tbl);
    }

    teardown_schema(&conn, &schema).await;
}

#[tokio::test]
async fn test_validate_on_migrate_detects_modification() {
    let (client, schema) = setup_schema("val_mig").await;

    let dir = std::env::temp_dir().join(format!(
        "waypoint_test_val_mig_{}",
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    std::fs::write(
        dir.join("V1__Create_val_mig.sql"),
        format!("CREATE TABLE {}.val_mig_tbl (id SERIAL);", schema),
    )
    .unwrap();

    // First run: migrate with validate_on_migrate disabled
    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config, client);
    wp.migrate(None).await.expect("first migrate failed");

    // Modify the migration file after first run
    std::fs::write(
        dir.join("V1__Create_val_mig.sql"),
        format!(
            "CREATE TABLE {}.val_mig_tbl (id SERIAL, extra TEXT);",
            schema
        ),
    )
    .unwrap();

    // Add a V2 so there's a pending migration to trigger the migrate path
    std::fs::write(
        dir.join("V2__Another.sql"),
        format!("CREATE TABLE {}.val_mig_tbl2 (id SERIAL);", schema),
    )
    .unwrap();

    // Second run with validate_on_migrate=true — should fail
    let mut config2 = test_config(&schema, dir.to_str().unwrap());
    config2.migrations.validate_on_migrate = true;

    let client2 = db::connect(&get_test_url()).await.unwrap();
    let wp2 = Waypoint::with_client(config2, client2);
    let result = wp2.migrate(None).await;
    assert!(
        result.is_err(),
        "migrate should fail when validate_on_migrate detects checksum change"
    );

    let conn = db::connect(&get_test_url()).await.unwrap();
    teardown_schema(&conn, &schema).await;
    let _ = std::fs::remove_dir_all(&dir);
}
