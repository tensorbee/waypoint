//! CLI entry point for the waypoint migration tool.
//! Provides clap-based command routing for 16 subcommands, exit code mapping
//! based on error type, and multi-database dispatch.

mod output;
#[cfg(feature = "self-update")]
mod self_update;

use std::process;

use clap::{Parser, Subcommand};
use colored::Colorize;

use waypoint_core::config::{normalize_location, CliOverrides, WaypointConfig};
use waypoint_core::error::WaypointError;
use waypoint_core::migration::MigrationVersion;
use waypoint_core::{UndoTarget, Waypoint};

/// Print a report as JSON (when `--json` is active) or via a terminal formatter.
/// The 4-argument form accepts a `quiet` flag; when quiet and not JSON, output is suppressed.
macro_rules! print_report {
    ($report:expr, $json:expr, $printer:path) => {
        if $json {
            println!(
                "{}",
                serde_json::to_string_pretty(&$report).expect("JSON serialization failed")
            );
        } else {
            $printer(&$report);
        }
    };
    ($report:expr, $json:expr, $quiet:expr, $printer:path) => {
        if $json {
            println!(
                "{}",
                serde_json::to_string_pretty(&$report).expect("JSON serialization failed")
            );
        } else if !$quiet {
            $printer(&$report);
        }
    };
}

/// Top-level CLI definition with global flags and subcommand dispatch.
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
    #[arg(long, global = true)]
    json: bool,

    /// Preview what would be done without making changes
    #[arg(long, global = true)]
    dry_run: bool,

    /// Suppress non-essential output
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Enable verbose/debug output
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Environment for environment-scoped migrations
    #[arg(long, value_name = "ENV", global = true)]
    environment: Option<String>,

    /// Enable dependency-based migration ordering
    #[arg(long, global = true)]
    dependency_ordering: bool,

    /// Skip pre-flight health checks
    #[arg(long, global = true)]
    skip_preflight: bool,

    /// Filter to a specific database (multi-db mode)
    #[arg(long, value_name = "NAME", global = true)]
    database: Option<String>,

    /// Stop on first failure (multi-db mode)
    #[arg(long, global = true)]
    fail_fast: bool,

    /// Override DANGER safety blocks
    #[arg(long, global = true)]
    force: bool,

    /// Run simulation before migrate
    #[arg(long, global = true)]
    simulate: bool,

    /// Wrap all pending migrations in a single transaction (all-or-nothing)
    #[arg(long, global = true)]
    transaction: bool,

    /// TCP keepalive interval in seconds (0 to disable)
    #[arg(long, value_name = "SECS", global = true)]
    keepalive: Option<u32>,

    #[command(subcommand)]
    command: Commands,
}

/// All available waypoint subcommands.
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

    /// Undo applied migration(s)
    Undo {
        /// Undo all versions above this version (exclusive)
        #[arg(long, value_name = "VERSION", conflicts_with = "count")]
        target: Option<String>,

        /// Number of migrations to undo
        #[arg(long, value_name = "N", conflicts_with = "target")]
        count: Option<usize>,
    },

    /// Drop all objects in managed schemas
    Clean {
        /// Required flag to actually run clean
        #[arg(long)]
        allow_clean: bool,
    },

    /// Static analysis of migration SQL files
    Lint {
        /// Disable specific rules (comma-separated)
        #[arg(long, value_name = "RULES", value_delimiter = ',')]
        disable: Vec<String>,
        /// Exit code 1 if any errors found
        #[arg(long)]
        strict: bool,
    },

    /// Auto-generate changelog from migration DDL
    Changelog {
        /// Start from this version
        #[arg(long, value_name = "VERSION")]
        from: Option<String>,
        /// End at this version
        #[arg(long, value_name = "VERSION")]
        to: Option<String>,
        /// Output format: plain, markdown, json
        #[arg(long, default_value = "plain")]
        format: String,
    },

    /// Compare database schema against a target
    Diff {
        /// Compare against another database URL
        #[arg(long, value_name = "URL")]
        target_url: Option<String>,
        /// Write output SQL to file
        #[arg(long)]
        output: Option<String>,
        /// Auto-generate versioned migration file (V{next}__Auto_generated.sql)
        #[arg(long)]
        auto_version: bool,
    },

    /// Detect manual schema changes that bypassed migrations
    Drift,

    /// Take a schema snapshot
    Snapshot,

    /// Restore from a schema snapshot
    Restore {
        /// Snapshot ID to restore (omit to list available)
        #[arg(value_name = "ID")]
        snapshot_id: Option<String>,
    },

    /// Run pre-flight health checks
    Preflight,

    /// Detect migration conflicts between git branches
    CheckConflicts {
        /// Base branch to compare against
        #[arg(long, default_value = "main")]
        base: String,
        /// Minimal output for git hooks
        #[arg(long)]
        git_hook: bool,
    },

    /// Analyze migration safety (lock levels, impact estimation)
    Safety {
        /// Analyze a specific migration file
        #[arg(value_name = "FILE")]
        file: Option<String>,
    },

    /// Suggest schema improvements
    Advise {
        /// Write fix SQL to a migration file
        #[arg(long, value_name = "PATH")]
        fix_file: Option<String>,
    },

    /// Dry-run migrations in a temporary schema
    Simulate,

    /// Update waypoint to the latest version
    #[cfg(feature = "self-update")]
    SelfUpdate {
        /// Check for updates without installing
        #[arg(long)]
        check: bool,
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

    env_logger::Builder::new()
        .parse_env(env_logger::Env::default().default_filter_or(filter))
        .format_target(false)
        .format_timestamp(None)
        .init();

    if let Err(e) = run(cli).await {
        print_error(&e);
        process::exit(exit_code(&e));
    }
}

/// Map error types to differentiated exit codes.
// ChecksumMismatch and DiffFailed are deprecated reserved variants that no
// code path actually constructs. Their arms below are dead but kept until
// the variants are removed in 0.4.0 (so the match remains exhaustive).
#[allow(deprecated)]
fn exit_code(error: &WaypointError) -> i32 {
    match error {
        WaypointError::ConfigError(_) => 2,
        WaypointError::PlaceholderNotFound { .. } => 2,
        WaypointError::DatabaseNotFound { .. } => 2,
        WaypointError::ValidationFailed(_) => 3,
        WaypointError::ChecksumMismatch { .. } => 3,
        WaypointError::BaselineExists => 3,
        WaypointError::OutOfOrder { .. } => 3,
        WaypointError::DependencyCycle { .. } => 3,
        WaypointError::MissingDependency { .. } => 3,
        WaypointError::InvalidDirective { .. } => 3,
        WaypointError::MultiDbDependencyCycle { .. } => 3,
        #[cfg(feature = "postgres")]
        WaypointError::DatabaseError(_) => 4,
        #[cfg(feature = "mysql")]
        WaypointError::MysqlError(_) => 4,
        WaypointError::ConnectionLost { .. } => 4,
        WaypointError::MigrationFailed { .. } => 5,
        WaypointError::MigrationParseError(_) => 5,
        WaypointError::HookFailed { .. } => 5,
        WaypointError::UndoFailed { .. } => 5,
        WaypointError::UndoMissing { .. } => 5,
        WaypointError::NonTransactionalStatement { .. } => 5,
        WaypointError::MultiDbError { .. } => 5,
        WaypointError::LockError(_) => 6,
        WaypointError::CleanDisabled => 7,
        WaypointError::UpdateError(_) => 8,
        WaypointError::LintFailed { .. } => 9,
        WaypointError::DriftDetected { .. } => 10,
        WaypointError::ConflictsDetected { .. } => 11,
        WaypointError::PreflightFailed { .. } => 12,
        WaypointError::GuardFailed { .. } => 13,
        WaypointError::MigrationBlocked { .. } => 14,
        WaypointError::SimulationFailed { .. } => 15,
        WaypointError::DiffFailed { .. } => 1,
        WaypointError::SnapshotError { .. } => 1,
        WaypointError::GitError(_) => 1,
        WaypointError::AdvisorError(_) => 1,
        WaypointError::IoError(_) => 1,
    }
}

/// Build configuration, resolve multi-database mode, and dispatch the chosen subcommand.
async fn run(cli: Cli) -> Result<(), WaypointError> {
    let json_output = cli.json;
    let dry_run = cli.dry_run;
    let quiet = cli.quiet;
    let skip_preflight = cli.skip_preflight;
    let force = cli.force;
    let simulate_flag = cli.simulate;

    // Handle self-update before config/DB setup (no database needed)
    #[cfg(feature = "self-update")]
    if let Commands::SelfUpdate { check } = &cli.command {
        return self_update::self_update(*check, json_output);
    }

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
        locations: cli
            .locations
            .map(|l| l.split(',').map(|s| normalize_location(s.trim())).collect()),
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
        environment: cli.environment,
        dependency_ordering: if cli.dependency_ordering {
            Some(true)
        } else {
            None
        },
        keepalive: cli.keepalive,
        batch_transaction: if cli.transaction { Some(true) } else { None },
    };

    // Load config
    let mut config = WaypointConfig::load(cli.config.as_deref(), &overrides)?;

    // Override preflight if --skip-preflight
    if skip_preflight {
        config.preflight.enabled = false;
    }

    // === Commands that don't need a DB connection ===

    match &cli.command {
        Commands::Lint { disable, strict } => {
            let mut disabled = config.lint.disabled_rules.clone();
            disabled.extend(disable.iter().cloned());
            let report =
                waypoint_core::commands::lint::execute(&config.migrations.locations, &disabled)?;
            print_report!(report, json_output, output::print_lint_report);
            if *strict && report.error_count > 0 {
                return Err(WaypointError::LintFailed {
                    error_count: report.error_count,
                    details: format!("{} warning(s)", report.warning_count),
                });
            }
            return Ok(());
        }
        Commands::Changelog { from, to, format } => {
            let report = waypoint_core::commands::changelog::execute(
                &config.migrations.locations,
                from.as_deref(),
                to.as_deref(),
            )?;
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("JSON serialization failed")
                );
            } else {
                let fmt = waypoint_core::commands::changelog::ChangelogFormat::parse(format);
                match fmt {
                    waypoint_core::commands::changelog::ChangelogFormat::Markdown => {
                        print!(
                            "{}",
                            waypoint_core::commands::changelog::render_markdown(&report)
                        );
                    }
                    waypoint_core::commands::changelog::ChangelogFormat::Json => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&report)
                                .expect("JSON serialization failed")
                        );
                    }
                    waypoint_core::commands::changelog::ChangelogFormat::PlainText => {
                        print!(
                            "{}",
                            waypoint_core::commands::changelog::render_plain(&report)
                        );
                    }
                }
            }
            return Ok(());
        }
        Commands::CheckConflicts { base, git_hook } => {
            let report = waypoint_core::commands::check_conflicts::execute(
                &config.migrations.locations,
                base,
            )?;
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("JSON serialization failed")
                );
            } else if *git_hook {
                if report.has_conflicts {
                    eprintln!(
                        "Migration conflicts detected: {} conflict(s)",
                        report.conflicts.len()
                    );
                }
            } else {
                output::print_conflict_report(&report);
            }
            if report.has_conflicts {
                return Err(WaypointError::ConflictsDetected {
                    count: report.conflicts.len(),
                    details: report
                        .conflicts
                        .iter()
                        .map(|c| c.description.clone())
                        .collect::<Vec<_>>()
                        .join("; "),
                });
            }
            return Ok(());
        }
        _ => {}
    }

    // === Multi-database mode ===
    if let Some(ref databases) = config.multi_database {
        let order = waypoint_core::MultiWaypoint::execution_order(databases)?;
        let clients =
            waypoint_core::MultiWaypoint::connect(databases, cli.database.as_deref()).await?;

        match &cli.command {
            Commands::Migrate { target } => {
                let result = waypoint_core::MultiWaypoint::migrate_with_options(
                    databases,
                    &clients,
                    &order,
                    target.as_deref(),
                    cli.fail_fast,
                    force,
                )
                .await?;
                print_report!(result, json_output, output::print_multi_result);
                if !result.all_succeeded {
                    return Err(WaypointError::MultiDbError {
                        name: "multi".to_string(),
                        reason: "One or more databases failed".to_string(),
                    });
                }
            }
            Commands::Info => {
                let all_info =
                    waypoint_core::MultiWaypoint::info(databases, &clients, &order).await?;
                print_report!(all_info, json_output, output::print_multi_info);
            }
            _ => {
                // For other commands, run on filtered single DB
                if let Some(ref db_name) = cli.database {
                    if let Some(db) = databases.iter().find(|d| &d.name == db_name) {
                        let single_config = db.to_waypoint_config();
                        let wp = Waypoint::new(single_config).await?;
                        return run_single_db_command(
                            &cli.command,
                            &wp,
                            json_output,
                            dry_run,
                            force,
                            simulate_flag,
                            quiet,
                        )
                        .await;
                    }
                }
                return Err(WaypointError::ConfigError(
                    "Multi-database mode: use --database to select a database for this command"
                        .to_string(),
                ));
            }
        }
        return Ok(());
    }

    // === Single database mode ===

    // Dry-run mode: show what would be applied using info/explain
    if dry_run {
        if let Commands::Migrate { .. } = &cli.command {
            let wp = Waypoint::new(config).await?;
            let report =
                waypoint_core::commands::explain::execute_db(wp.client(), &wp.config).await?;
            print_report!(report, json_output, output::print_explain_report);
            return Ok(());
        }
    }

    // Create waypoint instance and run with transient error retry
    let max_retries = config.database.connect_retries.min(3);
    let mut retries_left = max_retries;

    loop {
        let wp = Waypoint::new(config.clone()).await?;
        match run_single_db_command(
            &cli.command,
            &wp,
            json_output,
            dry_run,
            force,
            simulate_flag,
            quiet,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if waypoint_core::db::is_transient_error(&e) && retries_left > 0 => {
                retries_left -= 1;
                let attempt = max_retries - retries_left;
                eprintln!(
                    "{}",
                    format!(
                        "Connection lost, reconnecting ({}/{})...",
                        attempt, max_retries
                    )
                    .yellow()
                );
                let backoff = std::time::Duration::from_secs(std::cmp::min(1u64 << attempt, 10));
                tokio::time::sleep(backoff).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Execute a subcommand against a single database instance.
async fn run_single_db_command(
    command: &Commands,
    wp: &Waypoint,
    json_output: bool,
    _dry_run: bool,
    force: bool,
    simulate_before: bool,
    quiet: bool,
) -> Result<(), WaypointError> {
    match command {
        Commands::Migrate { target, .. } => {
            // Optional: simulate before migrate
            if simulate_before || wp.config.simulation.simulate_before_migrate {
                let sim_report = wp.simulate().await?;
                if !sim_report.passed {
                    print_report!(sim_report, json_output, output::print_simulation_report);
                    return Err(WaypointError::SimulationFailed {
                        reason: sim_report
                            .errors
                            .iter()
                            .map(|e| format!("{}: {}", e.script, e.error))
                            .collect::<Vec<_>>()
                            .join("; "),
                    });
                }
                if !json_output && !quiet {
                    output::print_simulation_report(&sim_report);
                }
            }

            let report = wp.migrate_with_options(target.as_deref(), force).await?;
            print_report!(report, json_output, quiet, output::print_migrate_summary);
        }
        Commands::Info => {
            let infos = wp.info().await?;
            print_report!(infos, json_output, quiet, output::print_info_table);
        }
        Commands::Validate => {
            let report = wp.validate().await?;
            print_report!(report, json_output, quiet, output::print_validate_result);
        }
        Commands::Repair => {
            let report = wp.repair().await?;
            print_report!(report, json_output, quiet, output::print_repair_result);
        }
        Commands::Baseline {
            baseline_version,
            baseline_description,
        } => {
            wp.baseline(baseline_version.as_deref(), baseline_description.as_deref())
                .await?;
            if json_output {
                println!(
                    "{}",
                    serde_json::json!({"success": true, "message": "Successfully baselined schema."})
                );
            } else if !quiet {
                println!("{}", "Successfully baselined schema.".green().bold());
            }
        }
        Commands::Undo { target, count } => {
            let undo_target = if let Some(ver) = target {
                UndoTarget::Version(MigrationVersion::parse(ver)?)
            } else if let Some(n) = count {
                UndoTarget::Count(*n)
            } else {
                UndoTarget::Last
            };
            let report = wp.undo(undo_target).await?;
            print_report!(report, json_output, output::print_undo_summary);
        }
        Commands::Clean { allow_clean } => {
            let dropped = wp.clean(*allow_clean).await?;
            print_report!(dropped, json_output, output::print_clean_result);
        }
        Commands::Diff {
            target_url,
            output: output_file,
            auto_version,
        } => {
            let target = match target_url {
                Some(url) => waypoint_core::commands::diff::DiffTarget::Database(url.clone()),
                None => {
                    return Err(WaypointError::ConfigError(
                        "Diff requires --target-url".to_string(),
                    ));
                }
            };
            let report = wp.diff(target).await?;
            print_report!(report, json_output, output::print_diff_report);
            if report.has_changes {
                let output_path = if *auto_version {
                    // Determine next version from existing migrations
                    let infos = wp.info().await?;
                    let max_version = infos
                        .iter()
                        .filter_map(|i| i.version.as_ref())
                        .filter_map(|v| v.parse::<u64>().ok())
                        .max()
                        .unwrap_or(0);
                    let next_version = max_version + 1;
                    let dir = &wp.config.migrations.locations[0];
                    let filename = format!("V{}__Auto_generated.sql", next_version);
                    Some(dir.join(filename).display().to_string())
                } else {
                    output_file.clone()
                };
                if let Some(path) = output_path {
                    std::fs::write(&path, &report.generated_sql).map_err(WaypointError::IoError)?;
                    println!("{}", format!("Generated SQL written to {}", path).green());
                }
            }
        }
        Commands::Drift => {
            let report = wp.drift().await?;
            print_report!(report, json_output, output::print_drift_report);
            if report.has_drift {
                return Err(WaypointError::DriftDetected {
                    count: report.drifts.len(),
                    details: report
                        .drifts
                        .iter()
                        .map(|d| d.object.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                });
            }
        }
        Commands::Snapshot => {
            let report = wp.snapshot(&wp.config.snapshots).await?;
            print_report!(report, json_output, output::print_snapshot_report);
        }
        Commands::Restore { snapshot_id } => match snapshot_id {
            Some(id) => {
                let report = wp.restore(&wp.config.snapshots, id).await?;
                print_report!(report, json_output, output::print_restore_report);
            }
            None => {
                let snapshots =
                    waypoint_core::commands::snapshot::list_snapshots(&wp.config.snapshots)?;
                print_report!(snapshots, json_output, output::print_snapshot_list);
            }
        },
        Commands::Preflight => {
            let report = wp.preflight().await?;
            print_report!(report, json_output, output::print_preflight_report);
        }
        Commands::Safety { file } => {
            if let Some(path) = file {
                let report =
                    waypoint_core::commands::safety::execute_file_db(wp.client(), &wp.config, path)
                        .await?;
                print_report!(report, json_output, output::print_safety_report);
            } else {
                let report = wp.safety().await?;
                if json_output {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&report).expect("JSON serialization failed")
                    );
                } else {
                    for r in &report.reports {
                        output::print_safety_report(r);
                    }
                    output::print_safety_overall(report.overall_verdict);
                }
            }
        }
        Commands::Advise { fix_file } => {
            let report = wp.advise().await?;
            print_report!(report, json_output, output::print_advisor_report);
            if let Some(path) = fix_file {
                waypoint_core::commands::advisor::write_fix_file(&report, path)?;
                println!("{}", format!("Fix SQL written to {}", path).green());
            }
        }
        Commands::Simulate => {
            let report = wp.simulate().await?;
            print_report!(report, json_output, output::print_simulation_report);
            if !report.passed {
                return Err(WaypointError::SimulationFailed {
                    reason: report
                        .errors
                        .iter()
                        .map(|e| format!("{}: {}", e.script, e.error))
                        .collect::<Vec<_>>()
                        .join("; "),
                });
            }
        }
        // No-DB commands handled earlier
        Commands::Lint { .. } | Commands::Changelog { .. } | Commands::CheckConflicts { .. } => {
            unreachable!("handled before DB setup")
        }
        #[cfg(feature = "self-update")]
        Commands::SelfUpdate { .. } => {
            unreachable!("handled before DB setup")
        }
    }

    Ok(())
}

/// Print a formatted error message with actionable hints to stderr.
// Same deprecation-suppression as `exit_code` — keeps the match arms for
// reserved variants until 0.4.0 drops the variants entirely.
#[allow(deprecated)]
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
        WaypointError::UndoMissing { version } => {
            eprintln!(
                "{}",
                format!(
                    "Hint: Create a U{version}__<description>.sql file, or enable [reversals] for auto-generated undo."
                )
                .dimmed()
            );
        }
        WaypointError::MigrationBlocked { .. } => {
            eprintln!(
                "{}",
                "Hint: Use --force to override DANGER blocks, or add '-- waypoint:safety-override' to the migration."
                    .dimmed()
            );
        }
        WaypointError::GuardFailed { .. } => {
            eprintln!(
                "{}",
                "Hint: Check guard conditions in your migration directives (-- waypoint:require / -- waypoint:ensure)."
                    .dimmed()
            );
        }
        WaypointError::DriftDetected { .. } => {
            eprintln!(
                "{}",
                "Hint: Run 'waypoint diff' to generate a migration that resolves this drift."
                    .dimmed()
            );
        }
        WaypointError::LintFailed { .. } => {
            eprintln!(
                "{}",
                "Hint: Fix the issues or add rule IDs to [lint] disabled_rules in waypoint.toml."
                    .dimmed()
            );
        }
        WaypointError::NonTransactionalStatement { .. } => {
            eprintln!(
                "{}",
                "Hint: Remove --transaction to apply migrations individually, or rewrite the migration to avoid CONCURRENTLY/VACUUM/etc."
                    .dimmed()
            );
        }
        WaypointError::ConnectionLost { .. } => {
            eprintln!(
                "{}",
                "Hint: Run 'waypoint info' to check the current migration state.".dimmed()
            );
        }
        WaypointError::PlaceholderNotFound { key, .. } => {
            eprintln!(
                "{}",
                format!("Hint: Define placeholder '{}' in [placeholders] section of waypoint.toml or as an environment variable.", key).dimmed()
            );
        }
        WaypointError::MigrationFailed { script, .. } => {
            eprintln!(
                "{}",
                format!(
                    "Hint: Fix the SQL error in '{}', then run 'waypoint repair' if needed.",
                    script
                )
                .dimmed()
            );
        }
        WaypointError::HookFailed { script, .. } => {
            eprintln!(
                "{}",
                format!("Hint: Check the hook file '{}' for SQL errors.", script).dimmed()
            );
        }
        WaypointError::UndoFailed { script, .. } => {
            eprintln!(
                "{}",
                format!("Hint: Fix the SQL error in undo script '{}'.", script).dimmed()
            );
        }
        WaypointError::ValidationFailed(_) => {
            eprintln!(
                "{}",
                "Hint: Run 'waypoint validate' for details, then 'waypoint repair' to fix."
                    .dimmed()
            );
        }
        WaypointError::DependencyCycle { .. } => {
            eprintln!(
                "{}",
                "Hint: Check '-- waypoint:depends' directives for circular references.".dimmed()
            );
        }
        WaypointError::MissingDependency { .. } => {
            eprintln!(
                "{}",
                "Hint: Ensure the referenced migration version exists in your migration locations."
                    .dimmed()
            );
        }
        WaypointError::InvalidDirective { .. } => {
            eprintln!(
                "{}",
                "Hint: Check the '-- waypoint:' directive syntax in the migration file header."
                    .dimmed()
            );
        }
        WaypointError::PreflightFailed { .. } => {
            eprintln!(
                "{}",
                "Hint: Use --skip-preflight to bypass, or resolve the database health issues."
                    .dimmed()
            );
        }
        WaypointError::ConflictsDetected { .. } => {
            eprintln!(
                "{}",
                "Hint: Resolve migration version conflicts between branches before merging."
                    .dimmed()
            );
        }
        WaypointError::LockError(_) => {
            eprintln!(
                "{}",
                "Hint: Another migration may be running. Wait and retry, or check pg_locks."
                    .dimmed()
            );
        }
        WaypointError::SimulationFailed { .. } => {
            eprintln!(
                "{}",
                "Hint: Fix the SQL errors shown above before running the actual migration."
                    .dimmed()
            );
        }
        WaypointError::BaselineExists => {
            eprintln!(
                "{}",
                "Hint: A baseline already exists. Use 'waypoint info' to see the current state."
                    .dimmed()
            );
        }
        WaypointError::DatabaseNotFound { .. } => {
            eprintln!(
                "{}",
                "Hint: Check the database name in --database flag or [[databases]] config."
                    .dimmed()
            );
        }
        WaypointError::MigrationParseError(_) => {
            eprintln!(
                "{}",
                "Hint: Check migration filenames follow the pattern V{version}__{description}.sql."
                    .dimmed()
            );
        }
        WaypointError::MultiDbDependencyCycle { .. } | WaypointError::MultiDbError { .. } => {
            eprintln!(
                "{}",
                "Hint: Check [[databases]] dependency configuration in waypoint.toml.".dimmed()
            );
        }
        // Remaining errors with no specific guidance
        WaypointError::UpdateError(_)
        | WaypointError::DiffFailed { .. }
        | WaypointError::SnapshotError { .. }
        | WaypointError::GitError(_)
        | WaypointError::AdvisorError(_)
        | WaypointError::IoError(_) => {}
        #[cfg(feature = "mysql")]
        WaypointError::MysqlError(_) => {}
    }
}
