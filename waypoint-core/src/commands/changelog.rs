//! Auto-generate changelog from migration DDL.
//!
//! Parses migration files and extracts DDL operations to produce
//! a structured changelog in markdown, plain text, or JSON format.

use std::path::PathBuf;

use serde::Serialize;

use crate::error::Result;
use crate::migration::{MigrationKind, MigrationVersion, scan_migrations};
use crate::sql_parser::{DdlOperation, extract_ddl_operations};

/// Supported output formats for the changelog.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum ChangelogFormat {
    /// Render changelog as a Markdown document.
    Markdown,
    /// Render changelog as plain text.
    PlainText,
    /// Render changelog as structured JSON.
    Json,
}

impl ChangelogFormat {
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "markdown" | "md" => ChangelogFormat::Markdown,
            "json" => ChangelogFormat::Json,
            _ => ChangelogFormat::PlainText,
        }
    }
}

/// Changes for a single migration version.
#[derive(Debug, Clone, Serialize)]
pub struct VersionChanges {
    /// Version string, or None for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description from the migration filename.
    pub description: String,
    /// Filename of the migration script.
    pub script: String,
    /// DDL operations extracted from the migration SQL.
    pub changes: Vec<DdlOperation>,
}

/// Complete changelog report.
#[derive(Debug, Serialize)]
pub struct ChangelogReport {
    /// Per-version change entries in version order.
    pub versions: Vec<VersionChanges>,
    /// Total number of DDL changes across all versions.
    pub total_changes: usize,
}

/// Execute the changelog command.
pub fn execute(
    locations: &[PathBuf],
    from: Option<&str>,
    to: Option<&str>,
) -> Result<ChangelogReport> {
    let migrations = scan_migrations(locations)?;

    let from_version = from.map(MigrationVersion::parse).transpose()?;
    let to_version = to.map(MigrationVersion::parse).transpose()?;

    let mut versions = Vec::new();
    let mut total_changes = 0;

    for migration in &migrations {
        // Skip undo migrations
        if migration.is_undo() {
            continue;
        }

        // Apply version range filter
        if let Some(ref fv) = from_version
            && let Some(mv) = migration.version()
            && mv < fv
        {
            continue;
        }
        if let Some(ref tv) = to_version
            && let Some(mv) = migration.version()
            && mv > tv
        {
            continue;
        }

        let changes = extract_ddl_operations(&migration.sql);
        total_changes += changes.len();

        let version = match &migration.kind {
            MigrationKind::Versioned(v) => Some(v.raw.clone()),
            _ => None,
        };

        versions.push(VersionChanges {
            version,
            description: migration.description.clone(),
            script: migration.script.clone(),
            changes,
        });
    }

    Ok(ChangelogReport {
        versions,
        total_changes,
    })
}

/// Render changelog as markdown.
pub fn render_markdown(report: &ChangelogReport) -> String {
    let mut output = String::from("# Changelog\n\n");

    for vc in &report.versions {
        let header = match &vc.version {
            Some(v) => format!("## V{} — {}", v, vc.description),
            None => format!("## (Repeatable) — {}", vc.description),
        };
        output.push_str(&header);
        output.push('\n');
        output.push_str(&format!("_Source: {}_\n\n", vc.script));

        if vc.changes.is_empty() {
            output.push_str("- No DDL changes detected\n");
        } else {
            for change in &vc.changes {
                output.push_str(&format!("- {}\n", change));
            }
        }
        output.push('\n');
    }

    output.push_str(&format!(
        "---\n_Total: {} change(s) across {} migration(s)_\n",
        report.total_changes,
        report.versions.len()
    ));

    output
}

/// Render changelog as plain text.
pub fn render_plain(report: &ChangelogReport) -> String {
    let mut output = String::from("CHANGELOG\n=========\n\n");

    for vc in &report.versions {
        let header = match &vc.version {
            Some(v) => format!("V{} - {}", v, vc.description),
            None => format!("(Repeatable) - {}", vc.description),
        };
        output.push_str(&header);
        output.push('\n');
        output.push_str(&format!("  Source: {}\n", vc.script));

        if vc.changes.is_empty() {
            output.push_str("  No DDL changes detected\n");
        } else {
            for change in &vc.changes {
                output.push_str(&format!("  * {}\n", change));
            }
        }
        output.push('\n');
    }

    output.push_str(&format!(
        "Total: {} change(s) across {} migration(s)\n",
        report.total_changes,
        report.versions.len()
    ));

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_changelog_basic() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("V1__Create_users.sql"),
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
        )
        .unwrap();
        fs::write(
            dir.path().join("V2__Add_email.sql"),
            "ALTER TABLE users ADD COLUMN email VARCHAR(255);",
        )
        .unwrap();

        let report = execute(&[dir.path().to_path_buf()], None, None).unwrap();
        assert_eq!(report.versions.len(), 2);
        assert!(report.total_changes >= 2);
    }

    #[test]
    fn test_changelog_version_range() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("V1__First.sql"),
            "CREATE TABLE t1 (id SERIAL);",
        )
        .unwrap();
        fs::write(
            dir.path().join("V2__Second.sql"),
            "CREATE TABLE t2 (id SERIAL);",
        )
        .unwrap();
        fs::write(
            dir.path().join("V3__Third.sql"),
            "CREATE TABLE t3 (id SERIAL);",
        )
        .unwrap();

        let report = execute(&[dir.path().to_path_buf()], Some("2"), Some("2")).unwrap();
        assert_eq!(report.versions.len(), 1);
        assert_eq!(report.versions[0].version.as_deref(), Some("2"));
    }

    #[test]
    fn test_render_markdown() {
        let report = ChangelogReport {
            versions: vec![VersionChanges {
                version: Some("1".to_string()),
                description: "Create users".to_string(),
                script: "V1__Create_users.sql".to_string(),
                changes: vec![DdlOperation::CreateTable {
                    table: "users".to_string(),
                    if_not_exists: false,
                }],
            }],
            total_changes: 1,
        };
        let md = render_markdown(&report);
        assert!(md.contains("# Changelog"));
        assert!(md.contains("## V1"));
        assert!(md.contains("CREATE TABLE users"));
    }
}
