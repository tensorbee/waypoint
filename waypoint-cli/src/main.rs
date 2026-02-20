mod output;

use std::process;

use clap::{Parser, Subcommand};
use colored::Colorize;
use tracing_subscriber::EnvFilter;

use waypoint_core::config::{normalize_location, CliOverrides, WaypointConfig};
use waypoint_core::error::WaypointError;
use waypoint_core::Waypoint;

#[derive(Parser)]
#[command(
    name = "waypoint",
    about = "Lightweight SQL migration tool",
    version = concat!(
        env!("CARGO_PKG_VERSION"),
        " (", env!("GIT_HASH"), " ", env!("BUILD_TIME"), ")"
    ),
    propagate_version = true
)]
struct Cli {
    /// Config file path
    #[arg(short, long, value_name = "PATH")]
    config: Option<String>,

    /// Database URL (overrides config)
    #[arg(long, value_name = "URL")]
    url: Option<String>,

    /// Target schema (overrides config)
    #[arg(long, value_name = "SCHEMA")]
    schema: Option<String>,

    /// History table name (overrides config)
    #[arg(long, value_name = "TABLE")]
    table: Option<String>,

    /// Migration locations, comma-separated (overrides config)
    #[arg(long, value_name = "PATHS")]
    locations: Option<String>,

    /// Number of retries when connecting to the database
    #[arg(long, value_name = "N")]
    connect_retries: Option<u32>,

    /// SSL/TLS mode: disable, prefer, require
    #[arg(long, value_name = "MODE")]
    ssl_mode: Option<String>,

    /// Connection timeout in seconds (default: 30, 0 = no timeout)
    #[arg(long, value_name = "SECS")]
    connect_timeout: Option<u32>,

    /// Statement timeout in seconds (default: 0 = no limit)
    #[arg(long, value_name = "SECS")]
    statement_timeout: Option<u32>,

    /// Allow out-of-order migrations
    #[arg(long, overrides_with = "no_out_of_order")]
    out_of_order: bool,

    /// Disallow out-of-order migrations (overrides --out-of-order)
    #[arg(long = "no-out-of-order", hide = true)]
    no_out_of_order: bool,

    /// Validate before migrating (default: true)
    #[arg(long, overrides_with = "no_validate_on_migrate")]
    validate_on_migrate: Option<bool>,

    /// Disable validate-on-migrate
    #[arg(long = "no-validate-on-migrate", hide = true)]
    no_validate_on_migrate: bool,

    /// Output results as JSON
    #[arg(long)]
    json: bool,

    /// Preview what would be done without making changes
    #[arg(long)]
    dry_run: bool,

    /// Suppress non-essential output
    #[arg(short, long)]
    quiet: bool,

    /// Enable verbose/debug output
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply pending migrations
    Migrate {
        /// Migrate up to this version only
        #[arg(long, value_name = "VERSION")]
        target: Option<String>,
    },

    /// Show migration status
    Info,

    /// Validate applied migrations
    Validate,

    /// Repair the schema history table
    Repair,

    /// Baseline an existing database
    Baseline {
        /// Version to baseline at
        #[arg(long, value_name = "VER")]
        baseline_version: Option<String>,

        /// Description for baseline entry
        #[arg(long, value_name = "DESC")]
        baseline_description: Option<String>,
    },

    /// Drop all objects in managed schemas
    Clean {
        /// Required flag to actually run clean
        #[arg(long)]
        allow_clean: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Set up logging (suppress when JSON output is requested)
    let filter = if cli.json {
        "error"
    } else if cli.verbose {
        "debug"
    } else if cli.quiet {
        "error"
    } else {
        "info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .with_target(false)
        .without_time()
        .init();

    if let Err(e) = run(cli).await {
        print_error(&e);
        process::exit(exit_code(&e));
    }
}

/// Map error types to differentiated exit codes.
fn exit_code(error: &WaypointError) -> i32 {
    match error {
        WaypointError::ConfigError(_) => 2,
        WaypointError::ValidationFailed(_) => 3,
        WaypointError::DatabaseError(_) => 4,
        WaypointError::MigrationFailed { .. } => 5,
        WaypointError::HookFailed { .. } => 5,
        WaypointError::LockError(_) => 6,
        WaypointError::CleanDisabled => 7,
        _ => 1,
    }
}

async fn run(cli: Cli) -> Result<(), WaypointError> {
    let json_output = cli.json;
    let dry_run = cli.dry_run;

    // Build CLI overrides with negation flag support
    let out_of_order = if cli.out_of_order {
        Some(true)
    } else if cli.no_out_of_order {
        Some(false)
    } else {
        None
    };

    let validate_on_migrate = if cli.no_validate_on_migrate {
        Some(false)
    } else {
        cli.validate_on_migrate
    };

    let overrides = CliOverrides {
        url: cli.url,
        schema: cli.schema,
        table: cli.table,
        locations: cli.locations.map(|l| {
            l.split(',')
                .map(|s| normalize_location(s.trim()))
                .collect()
        }),
        out_of_order,
        validate_on_migrate,
        baseline_version: match &cli.command {
            Commands::Baseline {
                baseline_version, ..
            } => baseline_version.clone(),
            _ => None,
        },
        connect_retries: cli.connect_retries,
        ssl_mode: cli.ssl_mode,
        connect_timeout: cli.connect_timeout,
        statement_timeout: cli.statement_timeout,
    };

    // Load config
    let config = WaypointConfig::load(cli.config.as_deref(), &overrides)?;

    // Dry-run mode: show what would be applied using info command
    if dry_run {
        if let Commands::Migrate { .. } = &cli.command {
            let wp = Waypoint::new(config).await?;
            let infos = wp.info().await?;
            let pending: Vec<_> = infos
                .into_iter()
                .filter(|i| {
                    matches!(
                        i.state,
                        waypoint_core::commands::info::MigrationState::Pending
                            | waypoint_core::commands::info::MigrationState::Outdated
                    )
                })
                .collect();

            if json_output {
                println!("{}", serde_json::to_string_pretty(&pending).unwrap());
            } else if pending.is_empty() {
                println!("{}", "Dry run: No pending migrations.".green());
            } else {
                println!(
                    "{}",
                    format!("Dry run: {} migration(s) would be applied:", pending.len())
                        .yellow()
                        .bold()
                );
                for info in &pending {
                    let version = info.version.as_deref().unwrap_or("(repeatable)");
                    println!(
                        "  {} {} — {} [{}]",
                        "→".yellow(),
                        version,
                        info.description,
                        info.script
                    );
                }
            }
            return Ok(());
        }
    }

    // Create waypoint instance
    let wp = Waypoint::new(config).await?;

    match cli.command {
        Commands::Migrate { target, .. } => {
            let report = wp.migrate(target.as_deref()).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_migrate_summary(&report);
            }
        }
        Commands::Info => {
            let infos = wp.info().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&infos).unwrap());
            } else {
                output::print_info_table(&infos);
            }
        }
        Commands::Validate => {
            let report = wp.validate().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_validate_result(&report);
            }
        }
        Commands::Repair => {
            let report = wp.repair().await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            } else {
                output::print_repair_result(&report);
            }
        }
        Commands::Baseline {
            baseline_version,
            baseline_description,
        } => {
            wp.baseline(
                baseline_version.as_deref(),
                baseline_description.as_deref(),
            )
            .await?;
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({"success": true, "message": "Successfully baselined schema."})
                );
            } else {
                println!(
                    "{}",
                    "Successfully baselined schema.".green().bold()
                );
            }
        }
        Commands::Clean { allow_clean } => {
            let dropped = wp.clean(allow_clean).await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&dropped).unwrap());
            } else {
                output::print_clean_result(&dropped);
            }
        }
    }

    Ok(())
}

fn print_error(error: &WaypointError) {
    eprintln!("{} {}", "ERROR:".red().bold(), error);

    // Provide actionable guidance
    match error {
        WaypointError::ConfigError(_) => {
            eprintln!(
                "{}",
                "Hint: Check your waypoint.toml or set WAYPOINT_DATABASE_URL environment variable."
                    .dimmed()
            );
        }
        WaypointError::DatabaseError(_) => {
            eprintln!(
                "{}",
                "Hint: Verify database is running and connection details are correct.".dimmed()
            );
        }
        WaypointError::CleanDisabled => {
            eprintln!(
                "{}",
                "Hint: Pass --allow-clean flag or set clean_enabled = true in waypoint.toml."
                    .dimmed()
            );
        }
        WaypointError::ChecksumMismatch { .. } => {
            eprintln!(
                "{}",
                "Hint: Run 'waypoint repair' to update checksums, or restore the original migration file."
                    .dimmed()
            );
        }
        WaypointError::OutOfOrder { .. } => {
            eprintln!(
                "{}",
                "Hint: Use --out-of-order flag to allow out-of-order migrations.".dimmed()
            );
        }
        _ => {}
    }
}
