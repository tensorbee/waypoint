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
        println!("{}", "Schema is up to date. No migration necessary.".green());
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
        println!("{}", "Successfully validated all applied migrations.".green().bold());
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

/// Print items dropped by clean.
pub fn print_clean_result(dropped: &[String]) {
    if dropped.is_empty() {
        println!("{}", "Nothing to clean.".green());
        return;
    }

    println!("{}", format!("Successfully cleaned. Dropped {} object(s):", dropped.len()).green().bold());
    for item in dropped {
        println!("  {} {}", "✗".red(), item);
    }
}
