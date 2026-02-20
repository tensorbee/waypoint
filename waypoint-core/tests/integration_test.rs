//! Integration tests for waypoint-core.
//!
//! Requires a running PostgreSQL instance.
//! Set TEST_DATABASE_URL env var, e.g.:
//!   TEST_DATABASE_URL="host=localhost user=postgres dbname=waypoint_test"
//!
//! Run with: cargo test --test integration_test

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use waypoint_core::commands::info::MigrationState;
use waypoint_core::config::{DatabaseConfig, HooksConfig, MigrationSettings, WaypointConfig};
use waypoint_core::db::{self, quote_ident};
use waypoint_core::history;
use waypoint_core::Waypoint;

fn get_test_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for integration tests")
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
        },
        hooks: HooksConfig::default(),
        placeholders: HashMap::new(),
    }
}

/// Helper: connect, create a fresh schema, return client + schema name.
async fn setup_schema(prefix: &str) -> (tokio_postgres::Client, String) {
    let url = get_test_url();
    let client = db::connect(&url).await.expect("Failed to connect to DB");

    // Use a unique schema name per test to avoid collisions
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let schema = format!("waypoint_test_{}_{}", prefix, id);

    client
        .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(&schema)))
        .await
        .expect("Failed to create test schema");

    (client, schema)
}

/// Helper: drop the test schema.
async fn teardown_schema(client: &tokio_postgres::Client, schema: &str) {
    let _ = client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(schema)))
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
            &format!("CREATE TABLE {}.things (id SERIAL PRIMARY KEY, name TEXT);", schema),
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
        format!("CREATE OR REPLACE VIEW {}.items_view AS SELECT id FROM {}.items;", schema, schema),
    )
    .unwrap();

    let config = test_config(&schema, dir.to_str().unwrap());
    let wp = Waypoint::with_client(config.clone(), client);

    let report = wp.migrate(None).await.expect("migrate failed");
    assert_eq!(report.migrations_applied, 2); // V1 + R

    // Now modify the repeatable
    std::fs::write(
        dir.join("R__Items_view.sql"),
        format!("CREATE OR REPLACE VIEW {}.items_view AS SELECT id FROM {}.items WHERE id > 0;", schema, schema),
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
        format!("CREATE TABLE {}.repair_test (id SERIAL PRIMARY KEY);", schema),
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
    let applied = history::get_applied_migrations(
        &client2,
        &schema,
        "waypoint_schema_history",
    )
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

    let migrations = create_temp_migrations(&[
        (
            "V1__Create_clean_test.sql",
            &format!("CREATE TABLE {}.clean_tbl (id SERIAL PRIMARY KEY);", schema),
        ),
    ]);

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
    let report = wp2.migrate(None).await.expect("out-of-order migrate should succeed");
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
