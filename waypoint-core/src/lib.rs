pub mod checksum;
pub mod commands;
pub mod config;
pub mod db;
pub mod error;
pub mod history;
pub mod hooks;
pub mod migration;
pub mod placeholder;

use config::WaypointConfig;
use error::Result;
use tokio_postgres::Client;

pub use commands::info::{MigrationInfo, MigrationState};
pub use commands::migrate::MigrateReport;
pub use commands::repair::RepairReport;
pub use commands::validate::ValidateReport;
pub use config::CliOverrides;

/// Main entry point for the Waypoint library.
///
/// Create a `Waypoint` instance with a config and use its methods to
/// run migration commands programmatically.
pub struct Waypoint {
    pub config: WaypointConfig,
    client: Client,
}

impl Waypoint {
    /// Create a new Waypoint instance, connecting to the database.
    ///
    /// If `connect_retries` is configured, retries with exponential backoff.
    pub async fn new(config: WaypointConfig) -> Result<Self> {
        let conn_string = config.connection_string()?;
        let client = db::connect_with_config(
            &conn_string,
            &config.database.ssl_mode,
            config.database.connect_retries,
            config.database.connect_timeout_secs,
            config.database.statement_timeout_secs,
        )
        .await?;
        Ok(Self { config, client })
    }

    /// Create a new Waypoint instance with an existing database client.
    pub fn with_client(config: WaypointConfig, client: Client) -> Self {
        Self { config, client }
    }

    /// Apply pending migrations.
    pub async fn migrate(&self, target_version: Option<&str>) -> Result<MigrateReport> {
        commands::migrate::execute(&self.client, &self.config, target_version).await
    }

    /// Show migration status information.
    pub async fn info(&self) -> Result<Vec<MigrationInfo>> {
        commands::info::execute(&self.client, &self.config).await
    }

    /// Validate applied migrations against local files.
    pub async fn validate(&self) -> Result<ValidateReport> {
        commands::validate::execute(&self.client, &self.config).await
    }

    /// Repair the schema history table.
    pub async fn repair(&self) -> Result<RepairReport> {
        commands::repair::execute(&self.client, &self.config).await
    }

    /// Baseline an existing database.
    pub async fn baseline(
        &self,
        version: Option<&str>,
        description: Option<&str>,
    ) -> Result<()> {
        commands::baseline::execute(&self.client, &self.config, version, description).await
    }

    /// Drop all objects in managed schemas.
    pub async fn clean(&self, allow_clean: bool) -> Result<Vec<String>> {
        commands::clean::execute(&self.client, &self.config, allow_clean).await
    }
}
