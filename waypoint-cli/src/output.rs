//! Terminal output formatting for all waypoint commands.
//! Uses comfy-table for tabular output and colored for
//! severity-aware terminal styling.

use std::collections::HashMap;

use colored::Colorize;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};

use waypoint_core::commands::info::{MigrationInfo, MigrationState};

/// Format migration info as a colored table.
pub fn print_info_table(infos: &[MigrationInfo]) {
    if infos.is_empty() {
        println!("{}", "No migrations found.".yellow());
        return;
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Version"),
            Cell::new("Description"),
            Cell::new("Type"),
            Cell::new("State"),
            Cell::new("Installed On"),
            Cell::new("Execution Time"),
        ]);

    for info in infos {
        let version = info.version.as_deref().unwrap_or("");
        let installed_on = info
            .installed_on
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();
        let exec_time = info
            .execution_time
            .map(|t| format!("{}ms", t))
            .unwrap_or_default();

        let state_str = format_state(&info.state);

        table.add_row(vec![
            Cell::new(version),
            Cell::new(&info.description),
            Cell::new(&info.migration_type),
            Cell::new(&state_str),
            Cell::new(&installed_on),
            Cell::new(&exec_time),
        ]);
    }

    println!("{table}");
}

/// Return a colored string representation of a migration state.
fn format_state(state: &MigrationState) -> String {
    match state {
        MigrationState::Pending => "Pending".yellow().to_string(),
        MigrationState::Applied => "Applied".green().to_string(),
        MigrationState::Failed => "Failed".red().bold().to_string(),
        MigrationState::Missing => "Missing".red().to_string(),
        MigrationState::Outdated => "Outdated".cyan().to_string(),
        MigrationState::OutOfOrder => "Out of Order".yellow().to_string(),
        MigrationState::BelowBaseline => "Below Baseline".dimmed().to_string(),
        MigrationState::Ignored => "Ignored".dimmed().to_string(),
        MigrationState::Baseline => "Baseline".blue().to_string(),
        MigrationState::Undone => "Undone".magenta().to_string(),
    }
}

/// Print a migration report summary.
pub fn print_migrate_summary(report: &waypoint_core::MigrateReport) {
    if report.hooks_executed > 0 {
        println!(
            "{}",
            format!(
                "Executed {} hook(s) ({}ms)",
                report.hooks_executed, report.hooks_time_ms
            )
            .dimmed()
        );
    }

    if report.migrations_applied == 0 {
        println!(
            "{}",
            "Schema is up to date. No migration necessary.".green()
        );
        return;
    }

    println!(
        "{}",
        format!(
            "Successfully applied {} migration(s) (execution time {}ms)",
            report.migrations_applied, report.total_time_ms
        )
        .green()
        .bold()
    );

    for detail in &report.details {
        let version = detail.version.as_deref().unwrap_or("(repeatable)");
        println!(
            "  {} {} — {} ({}ms)",
            "→".green(),
            version,
            detail.description,
            detail.execution_time_ms
        );
    }
}

/// Print a validate report.
pub fn print_validate_result(report: &waypoint_core::ValidateReport) {
    if report.valid {
        println!(
            "{}",
            "Successfully validated all applied migrations."
                .green()
                .bold()
        );
    }

    for warning in &report.warnings {
        println!("{} {}", "WARNING:".yellow().bold(), warning);
    }

    for issue in &report.issues {
        println!("{} {}", "ERROR:".red().bold(), issue);
    }
}

/// Print a repair report.
pub fn print_repair_result(report: &waypoint_core::RepairReport) {
    if report.failed_removed == 0 && report.checksums_updated == 0 {
        println!("{}", "Repair complete. No changes needed.".green());
        return;
    }

    println!("{}", "Repair complete:".green().bold());
    for detail in &report.details {
        println!("  {} {}", "→".green(), detail);
    }
}

/// Print an undo report summary.
pub fn print_undo_summary(report: &waypoint_core::UndoReport) {
    if report.migrations_undone == 0 {
        println!(
            "{}",
            "No migrations to undo. Schema is already at its earliest state.".green()
        );
        return;
    }

    println!(
        "{}",
        format!(
            "Successfully undone {} migration(s) (execution time {}ms)",
            report.migrations_undone, report.total_time_ms
        )
        .green()
        .bold()
    );

    for detail in &report.details {
        println!(
            "  {} {} — {} ({}ms)",
            "←".magenta(),
            detail.version,
            detail.description,
            detail.execution_time_ms
        );
    }
}

/// Print items dropped by clean.
pub fn print_clean_result(dropped: &[String]) {
    if dropped.is_empty() {
        println!("{}", "Nothing to clean.".green());
        return;
    }

    println!(
        "{}",
        format!("Successfully cleaned. Dropped {} object(s):", dropped.len())
            .green()
            .bold()
    );
    for item in dropped {
        println!("  {} {}", "✗".red(), item);
    }
}

/// Print lint report with colored severity.
pub fn print_lint_report(report: &waypoint_core::LintReport) {
    if report.issues.is_empty() {
        println!(
            "{}",
            format!("Checked {} file(s). No issues found.", report.files_checked)
                .green()
                .bold()
        );
        return;
    }

    println!(
        "{}",
        format!(
            "Checked {} file(s): {} error(s), {} warning(s), {} info",
            report.files_checked, report.error_count, report.warning_count, report.info_count
        )
        .bold()
    );
    println!();

    for issue in &report.issues {
        let severity = match issue.severity {
            waypoint_core::commands::lint::LintSeverity::Error => {
                format!("[{}]", issue.rule_id).red().bold().to_string()
            }
            waypoint_core::commands::lint::LintSeverity::Warning => {
                format!("[{}]", issue.rule_id).yellow().bold().to_string()
            }
            waypoint_core::commands::lint::LintSeverity::Info => {
                format!("[{}]", issue.rule_id).blue().to_string()
            }
        };

        let line_info = issue.line.map(|l| format!(":{}", l)).unwrap_or_default();

        println!(
            "  {} {}{} {}",
            severity, issue.script, line_info, issue.message
        );

        if let Some(ref suggestion) = issue.suggestion {
            println!("    {} {}", "→".dimmed(), suggestion.dimmed());
        }
    }
}

/// Print diff report.
pub fn print_diff_report(report: &waypoint_core::DiffReport) {
    if !report.has_changes {
        println!("{}", "No schema differences detected.".green().bold());
        return;
    }

    println!(
        "{}",
        format!("Found {} schema difference(s):", report.diffs.len())
            .yellow()
            .bold()
    );
    println!();

    for diff in &report.diffs {
        let line = format!("{}", diff);
        if line.starts_with('+') {
            println!("  {}", line.green());
        } else if line.starts_with('-') {
            println!("  {}", line.red());
        } else {
            println!("  {}", line.yellow());
        }
    }

    if !report.generated_sql.is_empty() {
        println!();
        println!("{}", "Generated SQL:".bold());
        println!("{}", report.generated_sql.dimmed());
    }
}

/// Print drift report.
pub fn print_drift_report(report: &waypoint_core::DriftReport) {
    if !report.has_drift {
        println!(
            "{}",
            format!("No drift detected in schema '{}'.", report.schema)
                .green()
                .bold()
        );
        return;
    }

    println!(
        "{}",
        format!(
            "Schema drift detected in '{}': {} difference(s)",
            report.schema,
            report.drifts.len()
        )
        .red()
        .bold()
    );
    println!();

    for drift in &report.drifts {
        let icon = match drift.drift_type {
            waypoint_core::commands::drift::DriftType::ExtraObject => "+".green(),
            waypoint_core::commands::drift::DriftType::MissingObject => "-".red(),
            waypoint_core::commands::drift::DriftType::ModifiedObject => "~".yellow(),
        };
        println!("  {} {} — {}", icon, drift.object, drift.detail.dimmed());
    }

    println!();
    println!(
        "{}",
        "Hint: Run 'waypoint diff' to generate a migration that resolves this drift.".dimmed()
    );
}

/// Print snapshot report.
pub fn print_snapshot_report(report: &waypoint_core::SnapshotReport) {
    println!(
        "{}",
        format!(
            "Snapshot '{}' created ({} objects captured)",
            report.snapshot_id, report.objects_captured
        )
        .green()
        .bold()
    );
    println!("  {} {}", "→".green(), report.snapshot_path);
}

/// Print restore report.
pub fn print_restore_report(report: &waypoint_core::RestoreReport) {
    println!(
        "{}",
        format!(
            "Restored from snapshot '{}' ({} objects restored)",
            report.snapshot_id, report.objects_restored
        )
        .green()
        .bold()
    );
}

/// Print list of available snapshots.
pub fn print_snapshot_list(snapshots: &[waypoint_core::commands::snapshot::SnapshotInfo]) {
    if snapshots.is_empty() {
        println!("{}", "No snapshots found.".yellow());
        return;
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("ID"),
            Cell::new("Created"),
            Cell::new("Size"),
        ]);

    for s in snapshots {
        let size = if s.size_bytes > 1024 * 1024 {
            format!("{:.1}MB", s.size_bytes as f64 / (1024.0 * 1024.0))
        } else if s.size_bytes > 1024 {
            format!("{:.1}KB", s.size_bytes as f64 / 1024.0)
        } else {
            format!("{}B", s.size_bytes)
        };
        table.add_row(vec![
            Cell::new(&s.id),
            Cell::new(&s.created),
            Cell::new(&size),
        ]);
    }

    println!("{table}");
}

/// Print preflight report.
pub fn print_preflight_report(report: &waypoint_core::PreflightReport) {
    println!(
        "{}",
        if report.passed {
            "Pre-flight checks passed.".green().bold()
        } else {
            "Pre-flight checks FAILED.".red().bold()
        }
    );
    println!();

    for check in &report.checks {
        let icon = match check.status {
            waypoint_core::preflight::CheckStatus::Pass => "✓".green(),
            waypoint_core::preflight::CheckStatus::Warn => "!".yellow(),
            waypoint_core::preflight::CheckStatus::Fail => "✗".red(),
        };
        println!("  {} {} — {}", icon, check.name, check.detail);
    }
}

/// Print explain report (enhanced dry-run).
pub fn print_explain_report(report: &waypoint_core::ExplainReport) {
    if report.migrations.is_empty() {
        println!("{}", "Dry run: No pending migrations.".green());
        return;
    }

    println!(
        "{}",
        format!(
            "Dry run: {} migration(s) would be applied:",
            report.migrations.len()
        )
        .yellow()
        .bold()
    );
    println!();

    for migration in &report.migrations {
        let version = migration.version.as_deref().unwrap_or("(repeatable)");
        println!("  {} {} [{}]", "→".yellow(), version, migration.script);

        for (i, stmt) in migration.statements.iter().enumerate() {
            let prefix = format!("    [{}/{}]", i + 1, migration.statements.len());
            if stmt.is_ddl {
                println!(
                    "  {} {} {}",
                    prefix.dimmed(),
                    stmt.statement_preview.dimmed(),
                    "(DDL)".dimmed()
                );
            } else {
                let cost_info = match (stmt.estimated_rows, stmt.estimated_cost) {
                    (Some(rows), Some(cost)) => {
                        format!("(~{:.0} rows, cost {:.1})", rows, cost)
                    }
                    _ => String::new(),
                };
                println!(
                    "  {} {} {}",
                    prefix.dimmed(),
                    stmt.statement_preview,
                    cost_info.dimmed()
                );
            }

            for warning in &stmt.warnings {
                println!("    {} {}", "!".yellow(), warning.yellow());
            }
        }
        println!();
    }
}

/// Print conflict report.
pub fn print_conflict_report(report: &waypoint_core::ConflictReport) {
    if !report.has_conflicts {
        println!(
            "{}",
            format!(
                "No migration conflicts detected against '{}'.",
                report.base_branch
            )
            .green()
            .bold()
        );
        return;
    }

    println!(
        "{}",
        format!(
            "Migration conflicts detected against '{}': {} conflict(s)",
            report.base_branch,
            report.conflicts.len()
        )
        .red()
        .bold()
    );
    println!();

    for conflict in &report.conflicts {
        let icon = match conflict.conflict_type {
            waypoint_core::commands::check_conflicts::ConflictType::VersionCollision => {
                "!!".red().bold()
            }
            waypoint_core::commands::check_conflicts::ConflictType::SemanticConflict => {
                "!~".yellow().bold()
            }
        };
        println!(
            "  {} {} — {}",
            icon, conflict.conflict_type, conflict.description
        );
        for file in &conflict.files {
            println!("    {} {}", "→".dimmed(), file);
        }
    }
}

/// Print multi-database result.
pub fn print_multi_result(result: &waypoint_core::multi::MultiResult) {
    for r in &result.results {
        let icon = if r.success {
            "✓".green()
        } else {
            "✗".red()
        };
        println!("  {} [{}] {}", icon, r.name, r.message);
    }

    if result.all_succeeded {
        println!(
            "{}",
            format!(
                "All {} database(s) migrated successfully.",
                result.results.len()
            )
            .green()
            .bold()
        );
    } else {
        let failed = result.results.iter().filter(|r| !r.success).count();
        println!("{}", format!("{} database(s) failed.", failed).red().bold());
    }
}

/// Print multi-database info.
pub fn print_multi_info(all_info: &HashMap<String, Vec<MigrationInfo>>) {
    for (name, infos) in all_info {
        println!("{}", format!("=== {} ===", name).bold());
        print_info_table(infos);
        println!();
    }
}

/// Print a safety analysis report for a single migration.
pub fn print_safety_report(report: &waypoint_core::SafetyReport) {
    let verdict_str = match report.overall_verdict {
        waypoint_core::safety::SafetyVerdict::Safe => "SAFE".green().bold(),
        waypoint_core::safety::SafetyVerdict::Caution => "CAUTION".yellow().bold(),
        waypoint_core::safety::SafetyVerdict::Danger => "DANGER".red().bold(),
    };

    println!(
        "  {} [{}] {}",
        verdict_str, report.script, report.overall_verdict
    );

    for stmt in &report.statements {
        let icon = match stmt.verdict {
            waypoint_core::safety::SafetyVerdict::Safe => "✓".green(),
            waypoint_core::safety::SafetyVerdict::Caution => "!".yellow(),
            waypoint_core::safety::SafetyVerdict::Danger => "✗".red(),
        };
        let table_info = stmt
            .affected_table
            .as_ref()
            .map(|t| {
                let size = stmt
                    .estimated_rows
                    .map(|r| format!(" (~{} rows)", r))
                    .unwrap_or_default();
                format!(" on {}{}", t, size)
            })
            .unwrap_or_default();

        println!(
            "    {} {} — {}{}",
            icon, stmt.statement_preview, stmt.lock_level, table_info
        );

        if stmt.data_loss {
            println!(
                "      {} {}",
                "⚠".red(),
                "Data loss: operation is irreversible".red()
            );
        }

        for suggestion in &stmt.suggestions {
            println!("      {} {}", "→".dimmed(), suggestion.dimmed());
        }
    }
}

/// Print the overall safety verdict.
pub fn print_safety_overall(verdict: waypoint_core::safety::SafetyVerdict) {
    let msg = match verdict {
        waypoint_core::safety::SafetyVerdict::Safe => {
            "Overall: SAFE — all migrations can proceed safely."
                .green()
                .bold()
        }
        waypoint_core::safety::SafetyVerdict::Caution => {
            "Overall: CAUTION — some migrations require attention."
                .yellow()
                .bold()
        }
        waypoint_core::safety::SafetyVerdict::Danger => {
            "Overall: DANGER — some migrations are high risk. Use --force to override."
                .red()
                .bold()
        }
    };
    println!("\n{}", msg);
}

/// Print advisor report.
pub fn print_advisor_report(report: &waypoint_core::AdvisorReport) {
    if report.advisories.is_empty() {
        println!(
            "{}",
            format!("Schema '{}' looks good. No advisories.", report.schema)
                .green()
                .bold()
        );
        return;
    }

    println!(
        "{}",
        format!(
            "Schema '{}': {} advisory(ies) ({} warning, {} suggestion, {} info)",
            report.schema,
            report.advisories.len(),
            report.warning_count,
            report.suggestion_count,
            report.info_count
        )
        .bold()
    );
    println!();

    for advisory in &report.advisories {
        let severity = match advisory.severity {
            waypoint_core::advisor::AdvisorySeverity::Warning => {
                format!("[{}]", advisory.rule_id).red().bold().to_string()
            }
            waypoint_core::advisor::AdvisorySeverity::Suggestion => {
                format!("[{}]", advisory.rule_id)
                    .yellow()
                    .bold()
                    .to_string()
            }
            waypoint_core::advisor::AdvisorySeverity::Info => {
                format!("[{}]", advisory.rule_id).blue().to_string()
            }
        };

        println!(
            "  {} {} — {} ({})",
            severity, advisory.object, advisory.explanation, advisory.category
        );

        if let Some(ref fix) = advisory.fix_sql {
            println!("    {} {}", "fix:".dimmed(), fix.dimmed());
        }
    }
}

/// Print simulation report.
pub fn print_simulation_report(report: &waypoint_core::SimulationReport) {
    if report.passed {
        println!(
            "{}",
            format!(
                "Simulation passed: {} migration(s) applied successfully in temp schema.",
                report.migrations_simulated
            )
            .green()
            .bold()
        );
    } else {
        println!(
            "{}",
            format!(
                "Simulation FAILED: {} error(s) in temp schema.",
                report.errors.len()
            )
            .red()
            .bold()
        );
        for error in &report.errors {
            println!("  {} {} — {}", "✗".red(), error.script, error.error);
        }
    }

    if !report.warnings.is_empty() {
        println!(
            "{}",
            format!(
                "Simulation warnings ({}): some source objects could not be replicated into the temp schema.",
                report.warnings.len()
            )
            .yellow()
        );
        for w in &report.warnings {
            println!("  {} {}", "!".yellow(), w);
        }
    }
}
