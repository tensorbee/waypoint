//! Migration file parsing, scanning, and types.
//!
//! Supports versioned (`V{version}__{desc}.sql`) and repeatable (`R__{desc}.sql`) migrations.

use std::cmp::Ordering;
use std::fmt;
use std::sync::LazyLock;

use regex::Regex;

use crate::checksum::calculate_checksum;
use crate::error::{Result, WaypointError};
use crate::hooks;

static VERSIONED_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^V([\d._]+)__(.+)$").unwrap());
static REPEATABLE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^R__(.+)$").unwrap());

/// A parsed migration version, supporting dotted numeric segments (e.g., "1.2.3").
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MigrationVersion {
    pub segments: Vec<u64>,
    pub raw: String,
}

impl MigrationVersion {
    /// Parse a version string like `"1.2.3"` or `"1_2"` into segments.
    pub fn parse(raw: &str) -> Result<Self> {
        if raw.is_empty() {
            return Err(WaypointError::MigrationParseError(
                "Version string is empty".to_string(),
            ));
        }

        // Support both "." and "_" as segment separators
        let segments: std::result::Result<Vec<u64>, _> =
            raw.split(['.', '_']).map(|s| s.parse::<u64>()).collect();

        let segments = segments.map_err(|e| {
            WaypointError::MigrationParseError(format!(
                "Invalid version segment in '{}': {}",
                raw, e
            ))
        })?;

        Ok(MigrationVersion {
            segments,
            raw: raw.to_string(),
        })
    }
}

impl Ord for MigrationVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        let max_len = self.segments.len().max(other.segments.len());
        for i in 0..max_len {
            let a = self.segments.get(i).copied().unwrap_or(0);
            let b = other.segments.get(i).copied().unwrap_or(0);
            match a.cmp(&b) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        Ordering::Equal
    }
}

impl PartialOrd for MigrationVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for MigrationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

/// The type of a migration (for display/serialization).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationType {
    /// V{version}__{description}.sql
    Versioned,
    /// R__{description}.sql
    Repeatable,
}

impl fmt::Display for MigrationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationType::Versioned => write!(f, "SQL"),
            MigrationType::Repeatable => write!(f, "SQL_REPEATABLE"),
        }
    }
}

/// Type-safe encoding of the migration variant.
///
/// Versioned migrations always have a version; repeatable migrations never do.
/// This eliminates the `Option<MigrationVersion>` + `MigrationType` redundancy.
#[derive(Debug, Clone)]
pub enum MigrationKind {
    Versioned(MigrationVersion),
    Repeatable,
}

/// A migration file discovered on disk.
#[derive(Debug, Clone)]
pub struct ResolvedMigration {
    pub kind: MigrationKind,
    pub description: String,
    pub script: String,
    pub checksum: i32,
    pub sql: String,
}

impl ResolvedMigration {
    /// Get the version if this is a versioned migration.
    pub fn version(&self) -> Option<&MigrationVersion> {
        match &self.kind {
            MigrationKind::Versioned(v) => Some(v),
            MigrationKind::Repeatable => None,
        }
    }

    /// Get the migration type for display/serialization.
    pub fn migration_type(&self) -> MigrationType {
        match &self.kind {
            MigrationKind::Versioned(_) => MigrationType::Versioned,
            MigrationKind::Repeatable => MigrationType::Repeatable,
        }
    }

    /// Whether this is a versioned migration.
    pub fn is_versioned(&self) -> bool {
        matches!(&self.kind, MigrationKind::Versioned(_))
    }
}

/// Parse a migration filename into its components.
///
/// Expected patterns:
///   V{version}__{description}.sql  — versioned migration
///   R__{description}.sql           — repeatable migration
pub fn parse_migration_filename(filename: &str) -> Result<(MigrationKind, String)> {
    // Strip .sql extension
    let stem = filename.strip_suffix(".sql").ok_or_else(|| {
        WaypointError::MigrationParseError(format!(
            "Migration file '{}' does not have .sql extension",
            filename
        ))
    })?;

    if let Some(caps) = VERSIONED_RE.captures(stem) {
        let version_str = caps.get(1).unwrap().as_str();
        let description = caps.get(2).unwrap().as_str().replace('_', " ");
        let version = MigrationVersion::parse(version_str)?;
        Ok((MigrationKind::Versioned(version), description))
    } else if let Some(caps) = REPEATABLE_RE.captures(stem) {
        let description = caps.get(1).unwrap().as_str().replace('_', " ");
        Ok((MigrationKind::Repeatable, description))
    } else {
        Err(WaypointError::MigrationParseError(format!(
            "Migration file '{}' does not match V{{version}}__{{description}}.sql or R__{{description}}.sql pattern",
            filename
        )))
    }
}

/// Scan migration locations for SQL files and parse them into ResolvedMigrations.
pub fn scan_migrations(locations: &[std::path::PathBuf]) -> Result<Vec<ResolvedMigration>> {
    let mut migrations = Vec::new();

    for location in locations {
        if !location.exists() {
            tracing::warn!("Migration location does not exist: {}", location.display());
            continue;
        }

        let entries = std::fs::read_dir(location).map_err(|e| {
            WaypointError::IoError(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to read migration directory '{}': {}",
                    location.display(),
                    e
                ),
            ))
        })?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Skip non-SQL files
            if !filename.ends_with(".sql") {
                continue;
            }

            // Skip hook callback files
            if hooks::is_hook_file(&filename) {
                continue;
            }

            // Skip files that don't start with V or R
            if !filename.starts_with('V') && !filename.starts_with('R') {
                continue;
            }

            let (kind, description) = parse_migration_filename(&filename)?;
            let sql = std::fs::read_to_string(&path)?;
            let checksum = calculate_checksum(&sql);

            migrations.push(ResolvedMigration {
                kind,
                description,
                script: filename,
                checksum,
                sql,
            });
        }
    }

    // Sort: versioned by version, repeatable by description
    migrations.sort_by(|a, b| match (&a.kind, &b.kind) {
        (MigrationKind::Versioned(va), MigrationKind::Versioned(vb)) => va.cmp(vb),
        (MigrationKind::Versioned(_), MigrationKind::Repeatable) => Ordering::Less,
        (MigrationKind::Repeatable, MigrationKind::Versioned(_)) => Ordering::Greater,
        (MigrationKind::Repeatable, MigrationKind::Repeatable) => a.description.cmp(&b.description),
    });

    Ok(migrations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_parsing() {
        let v = MigrationVersion::parse("1").unwrap();
        assert_eq!(v.segments, vec![1]);

        let v = MigrationVersion::parse("1.2.3").unwrap();
        assert_eq!(v.segments, vec![1, 2, 3]);

        let v = MigrationVersion::parse("1_2_3").unwrap();
        assert_eq!(v.segments, vec![1, 2, 3]);
    }

    #[test]
    fn test_version_ordering() {
        let v1 = MigrationVersion::parse("1").unwrap();
        let v2 = MigrationVersion::parse("2").unwrap();
        let v1_9 = MigrationVersion::parse("1.9").unwrap();
        let v1_10 = MigrationVersion::parse("1.10").unwrap();
        let v1_2 = MigrationVersion::parse("1.2").unwrap();
        let v1_2_0 = MigrationVersion::parse("1.2.0").unwrap();

        assert!(v1 < v2);
        assert!(v1_9 < v1_10); // Numeric, not string comparison
        assert!(v1_2 < v1_9);
        assert_eq!(v1_2.cmp(&v1_2_0), Ordering::Equal); // Trailing zeros are equal
    }

    #[test]
    fn test_version_parse_error() {
        assert!(MigrationVersion::parse("").is_err());
        assert!(MigrationVersion::parse("abc").is_err());
    }

    #[test]
    fn test_parse_versioned_filename() {
        let (kind, desc) = parse_migration_filename("V1__Create_users.sql").unwrap();
        match kind {
            MigrationKind::Versioned(v) => assert_eq!(v.segments, vec![1]),
            _ => panic!("Expected Versioned"),
        }
        assert_eq!(desc, "Create users");
    }

    #[test]
    fn test_parse_versioned_dotted_version() {
        let (kind, desc) = parse_migration_filename("V1.2.3__Add_column.sql").unwrap();
        match kind {
            MigrationKind::Versioned(v) => assert_eq!(v.segments, vec![1, 2, 3]),
            _ => panic!("Expected Versioned"),
        }
        assert_eq!(desc, "Add column");
    }

    #[test]
    fn test_parse_repeatable_filename() {
        let (kind, desc) = parse_migration_filename("R__Create_user_view.sql").unwrap();
        assert!(matches!(kind, MigrationKind::Repeatable));
        assert_eq!(desc, "Create user view");
    }

    #[test]
    fn test_parse_invalid_filename() {
        assert!(parse_migration_filename("random.sql").is_err());
        assert!(parse_migration_filename("V1_missing_separator.sql").is_err());
        assert!(parse_migration_filename("V1__no_ext").is_err());
    }
}
