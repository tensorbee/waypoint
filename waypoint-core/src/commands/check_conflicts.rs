//! Branch-aware migration conflict detection.
//!
//! Detects version collisions and semantic conflicts between
//! git branches without requiring a database connection.

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Command;

use serde::Serialize;

use crate::error::{Result, WaypointError};
use crate::migration::parse_migration_filename;
use crate::sql_parser::extract_ddl_operations;

/// Type of conflict detected.
#[derive(Debug, Clone, Serialize)]
pub enum ConflictType {
    /// Two branches define the same migration version number.
    VersionCollision,
    /// Two branches modify the same database object (table or column).
    SemanticConflict,
}

impl std::fmt::Display for ConflictType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictType::VersionCollision => write!(f, "Version Collision"),
            ConflictType::SemanticConflict => write!(f, "Semantic Conflict"),
        }
    }
}

/// A single conflict finding.
#[derive(Debug, Clone, Serialize)]
pub struct Conflict {
    /// Category of the detected conflict.
    pub conflict_type: ConflictType,
    /// Human-readable description of the conflict.
    pub description: String,
    /// Migration file paths involved in the conflict.
    pub files: Vec<String>,
}

/// Conflict detection report.
#[derive(Debug, Serialize)]
pub struct ConflictReport {
    /// All conflicts detected between branches.
    pub conflicts: Vec<Conflict>,
    /// Whether any conflicts were detected.
    pub has_conflicts: bool,
    /// Name of the base branch compared against.
    pub base_branch: String,
}

/// Execute the check-conflicts command.
pub fn execute(locations: &[PathBuf], base_branch: &str) -> Result<ConflictReport> {
    // Get files added on current branch
    let current_files = git_added_files(base_branch, "HEAD")?;
    // Get files added on base branch
    let base_files = git_added_files("HEAD", base_branch)?;

    // Filter to migration files in configured locations
    let current_migrations = filter_migration_files(&current_files, locations);
    let base_migrations = filter_migration_files(&base_files, locations);

    let mut conflicts = Vec::new();

    // Check for version collisions
    let current_versions = extract_versions(&current_migrations);
    let base_versions = extract_versions(&base_migrations);

    for (version, current_file) in &current_versions {
        if let Some(base_file) = base_versions.get(version) {
            conflicts.push(Conflict {
                conflict_type: ConflictType::VersionCollision,
                description: format!(
                    "Version V{} exists on both branches with different files",
                    version
                ),
                files: vec![current_file.clone(), base_file.clone()],
            });
        }
    }

    // Check for semantic conflicts (both modify same table/column)
    for current_file in &current_migrations {
        for base_file in &base_migrations {
            if let Some(conflict) = check_semantic_conflict(current_file, base_file) {
                conflicts.push(conflict);
            }
        }
    }

    let has_conflicts = !conflicts.is_empty();

    Ok(ConflictReport {
        conflicts,
        has_conflicts,
        base_branch: base_branch.to_string(),
    })
}

fn git_added_files(from: &str, to: &str) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args([
            "diff",
            "--name-only",
            "--diff-filter=A",
            &format!("{}...{}", from, to),
        ])
        .output()
        .map_err(|e| WaypointError::GitError(format!("Failed to run git: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(WaypointError::GitError(format!(
            "git diff failed: {}",
            stderr
        )));
    }

    let files = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(String::from)
        .collect();

    Ok(files)
}

/// Keep only files inside the configured migration locations.
///
/// This used to fall back to "any filename starting with V or R" when a file
/// was outside every configured location, which silently overrode
/// `[migrations] locations`. A `V1__example.sql` in a docs or fixtures
/// directory was then treated as a real migration, and two branches touching
/// such examples produced a conflict that blocks a git hook with exit 11.
///
/// Files that look like migrations but sit outside the configured locations are
/// reported rather than either silently included or silently dropped — that
/// combination usually means `locations` is wrong.
fn filter_migration_files(files: &[String], locations: &[PathBuf]) -> Vec<String> {
    let mut kept = Vec::new();
    let mut stray = Vec::new();

    for f in files {
        let path = PathBuf::from(f);
        if locations.iter().any(|loc| path.starts_with(loc)) {
            kept.push(f.clone());
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| (n.starts_with('V') || n.starts_with('R')) && n.ends_with(".sql"))
        {
            stray.push(f.clone());
        }
    }

    if !stray.is_empty() {
        log::warn!(
            "Ignoring {} file(s) that look like migrations but are outside \
             `[migrations] locations`: {}. Add their directory to `locations` if \
             they should be checked.",
            stray.len(),
            stray.join(", ")
        );
    }

    kept
}

fn extract_versions(files: &[String]) -> std::collections::HashMap<String, String> {
    let mut versions = std::collections::HashMap::new();
    for file in files {
        let filename = PathBuf::from(file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        if let Ok((crate::migration::MigrationKind::Versioned(v), _)) =
            parse_migration_filename(&filename)
        {
            versions.insert(v.raw, file.clone());
        }
    }
    versions
}

fn check_semantic_conflict(file_a: &str, file_b: &str) -> Option<Conflict> {
    let sql_a = std::fs::read_to_string(file_a).ok()?;
    let sql_b = std::fs::read_to_string(file_b).ok()?;

    let ops_a = extract_ddl_operations(&sql_a);
    let ops_b = extract_ddl_operations(&sql_b);

    // Check for overlapping table+column operations
    let targets_a: HashSet<String> = ops_a
        .iter()
        .filter_map(|op| match op {
            crate::sql_parser::DdlOperation::AlterTableAddColumn { table, column, .. } => {
                Some(format!("{}.{}", table, column))
            }
            crate::sql_parser::DdlOperation::AlterTableDropColumn { table, column } => {
                Some(format!("{}.{}", table, column))
            }
            crate::sql_parser::DdlOperation::AlterTableAlterColumn { table, column } => {
                Some(format!("{}.{}", table, column))
            }
            crate::sql_parser::DdlOperation::CreateTable { table, .. } => Some(table.clone()),
            _ => None,
        })
        .collect();

    let targets_b: HashSet<String> = ops_b
        .iter()
        .filter_map(|op| match op {
            crate::sql_parser::DdlOperation::AlterTableAddColumn { table, column, .. } => {
                Some(format!("{}.{}", table, column))
            }
            crate::sql_parser::DdlOperation::AlterTableDropColumn { table, column } => {
                Some(format!("{}.{}", table, column))
            }
            crate::sql_parser::DdlOperation::AlterTableAlterColumn { table, column } => {
                Some(format!("{}.{}", table, column))
            }
            crate::sql_parser::DdlOperation::CreateTable { table, .. } => Some(table.clone()),
            _ => None,
        })
        .collect();

    // Sorted: `HashSet::intersection` yields in hash order, so the description
    // listed the same objects differently on every run.
    let mut overlaps: Vec<String> = targets_a.intersection(&targets_b).cloned().collect();
    overlaps.sort();

    if overlaps.is_empty() {
        None
    } else {
        Some(Conflict {
            conflict_type: ConflictType::SemanticConflict,
            description: format!(
                "Both files modify the same object(s): {}",
                overlaps.join(", ")
            ),
            files: vec![file_a.to_string(), file_b.to_string()],
        })
    }
}
