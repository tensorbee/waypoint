//! Apply pending migrations to the database.
//!
//! This module owns the engine-agnostic public types ([`MigrateReport`],
//! [`MigrateDetail`]) and a handful of shared helpers used by both
//! engine-specific implementations. The actual `execute*` entry points
//! live in [`crate::engines::postgres::migrate`] and
//! [`crate::engines::mysql::migrate`] and are re-exported here so that
//! downstream callers (and the library `Waypoint` façade) can keep using
//! the historical paths under `crate::commands::migrate::*`.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::directive::MigrationDirectives;
use crate::error::{Result, WaypointError};
use crate::migration::{MigrationVersion, ResolvedMigration};

// ── Re-exports of the engine-specific entry points ──────────────────────────
//
// `multi.rs` and `lib.rs` reference these paths today. Keeping the names
// where they used to live preserves the public API and back-compat.

#[allow(deprecated)]
#[cfg(feature = "mysql")]
pub use crate::engines::mysql::migrate::execute as execute_mysql;
#[cfg(feature = "mysql")]
pub use crate::engines::mysql::migrate::execute_with_options as execute_mysql_with_options;
#[allow(deprecated)]
#[cfg(feature = "postgres")]
pub use crate::engines::postgres::migrate::execute;
#[cfg(feature = "postgres")]
pub use crate::engines::postgres::migrate::execute_with_options;

// ── Engine-agnostic public types ────────────────────────────────────────────

/// Report returned after a migrate operation.
#[derive(Debug, Serialize)]
pub struct MigrateReport {
    /// Number of migrations that were applied in this run.
    pub migrations_applied: usize,
    /// Total execution time of all migrations in milliseconds.
    pub total_time_ms: i32,
    /// Per-migration details for each applied migration.
    pub details: Vec<MigrateDetail>,
    /// Number of lifecycle hooks that were executed.
    pub hooks_executed: usize,
    /// Total execution time of all hooks in milliseconds.
    pub hooks_time_ms: i32,
    /// Migrations that were pending but skipped by a `require` guard under
    /// `guards.on_require_fail = "skip"`.
    ///
    /// Previously a skip left no trace in the report at all — only an `INFO`
    /// log line, which both `--json` and `--quiet` suppress. A pipeline reading
    /// `migrations_applied` had no way to tell a deliberate skip from there
    /// being nothing to do.
    #[serde(default)]
    pub skipped: Vec<SkippedMigration>,
}

/// A pending migration that a `require` guard skipped.
#[derive(Debug, Serialize)]
pub struct SkippedMigration {
    /// Version string, or `None` for a repeatable migration.
    pub version: Option<String>,
    /// Filename of the migration that was skipped.
    pub script: String,
    /// The `require` expression that was not satisfied.
    pub expression: String,
}

/// Details of a single applied migration within a migrate run.
#[derive(Debug, Serialize)]
pub struct MigrateDetail {
    /// Version string, or None for repeatable migrations.
    pub version: Option<String>,
    /// Human-readable description from the migration filename.
    pub description: String,
    /// Filename of the migration script.
    pub script: String,
    /// Execution time of this migration in milliseconds.
    pub execution_time_ms: i32,
}

// ── Shared helpers used by both engine paths ────────────────────────────────

/// Result of evaluating require-guard preconditions for a single migration.
pub(crate) enum GuardAction {
    /// All preconditions passed; proceed with the migration.
    Continue,
    /// A precondition failed with on_require_fail=Skip; skip this migration.
    ///
    /// Carries the expression that failed so the run can *report* the skip.
    /// It used to carry nothing, and the only trace was an `INFO` log line —
    /// which `--json` and `--quiet` both suppress.
    Skip(String),
    /// A precondition failed fatally; abort with the given error.
    Error(WaypointError),
}

/// Turn one `require` guard evaluation into a [`GuardAction`].
///
/// The two engine paths differ only in *how* they evaluate the expression
/// (`guard::evaluate` over `&Client` vs `guard::evaluate_db` over `&DbClient`).
/// Everything after that — the on-fail policy, the logging, the error shape —
/// is identical, and lives here so the engines cannot drift apart.
///
/// `outcome` is the parse-then-evaluate result: `Ok(bool)` for a guard that
/// evaluated, `Err` for a parse or evaluation failure.
pub(crate) fn classify_require(
    outcome: Result<bool>,
    expr_str: &str,
    script: &str,
    on_require_fail: &crate::guard::OnRequireFail,
) -> GuardAction {
    match outcome {
        Ok(true) => GuardAction::Continue,
        Ok(false) => match on_require_fail {
            crate::guard::OnRequireFail::Skip => {
                log::info!(
                    "Guard require failed, skipping migration; script={}, expr={}",
                    script,
                    expr_str
                );
                GuardAction::Skip(expr_str.to_string())
            }
            crate::guard::OnRequireFail::Warn => {
                log::warn!(
                    "Guard require failed (continuing); script={}, expr={}",
                    script,
                    expr_str
                );
                GuardAction::Continue
            }
            crate::guard::OnRequireFail::Error => GuardAction::Error(WaypointError::GuardFailed {
                kind: "require".to_string(),
                script: script.to_string(),
                expression: expr_str.to_string(),
            }),
        },
        Err(e) => {
            log::warn!(
                "Guard evaluation error; script={}, expr={}, error={}",
                script,
                expr_str,
                e
            );
            GuardAction::Error(WaypointError::GuardFailed {
                kind: "require".to_string(),
                script: script.to_string(),
                expression: format!("{} ({})", expr_str, describe_guard_error(&e)),
            })
        }
    }
}

/// Turn one `ensure` guard evaluation into a result.
pub(crate) fn classify_ensure(outcome: Result<bool>, expr_str: &str, script: &str) -> Result<()> {
    match outcome {
        Ok(true) => Ok(()),
        Ok(false) => Err(WaypointError::GuardFailed {
            kind: "ensure".to_string(),
            script: script.to_string(),
            expression: expr_str.to_string(),
        }),
        Err(e) => Err(WaypointError::GuardFailed {
            kind: "ensure".to_string(),
            script: script.to_string(),
            expression: format!("{} ({})", expr_str, describe_guard_error(&e)),
        }),
    }
}

/// Build the error for a guard expression that failed to parse.
///
/// Kept next to [`classify_require`] / [`classify_ensure`] so all three
/// produce the same `GuardFailed` shape.
pub(crate) fn guard_parse_error(
    kind: &str,
    script: &str,
    expr_str: &str,
    e: &WaypointError,
) -> WaypointError {
    WaypointError::GuardFailed {
        kind: kind.to_string(),
        script: script.to_string(),
        expression: format!("{} (parse error: {})", expr_str, e),
    }
}

/// Format an evaluation failure for inclusion in a `GuardFailed` expression.
fn describe_guard_error(e: &WaypointError) -> String {
    format!("evaluation error: {}", e)
}

/// Inputs that decide which migrations are still pending.
///
/// Shared by both engine paths so that PostgreSQL and MySQL cannot drift apart
/// on baseline/target/out-of-order/environment semantics.
pub(crate) struct PendingCriteria<'a> {
    /// Versions currently applied (respecting undo).
    pub effective_versions: &'a HashSet<String>,
    /// Baseline version from history, if any — anything at or below is skipped.
    pub baseline_version: Option<&'a MigrationVersion>,
    /// Upper bound requested by the caller, if any.
    pub target: Option<&'a MigrationVersion>,
    /// Highest effectively-applied version, for the out-of-order check.
    pub highest_applied: Option<&'a MigrationVersion>,
    /// Applied repeatable script name -> recorded checksum.
    pub applied_scripts: &'a HashMap<String, Option<i32>>,
    /// Environment name to filter `-- waypoint:env` directives against.
    pub current_env: Option<&'a str>,
    /// Whether applying a version below `highest_applied` is permitted.
    pub out_of_order: bool,
    /// Whether `-- waypoint:depends` directives decide apply order.
    ///
    /// When false, pending migrations run in ascending version order. When
    /// true, they run in a topological order derived from the dependency
    /// graph, which still degrades to version order when no migration declares
    /// a dependency.
    pub dependency_ordering: bool,
}

/// The pending work for one migrate run.
#[derive(Debug)]
pub(crate) struct PendingSelection<'a> {
    /// Pending versioned migrations, in ascending version order.
    pub versioned: Vec<&'a ResolvedMigration>,
    /// Pending repeatable migrations (new, or checksum changed).
    pub repeatables: Vec<&'a ResolvedMigration>,
}

/// Select the migrations that still need to run.
///
/// Returns [`WaypointError::OutOfOrder`] when a pending version sorts below the
/// highest applied one and `out_of_order` is disabled. Erroring — rather than
/// silently skipping — is deliberate: a skipped migration that the report
/// counts as a clean run is the worst possible outcome.
pub(crate) fn select_pending<'a>(
    resolved: &'a [ResolvedMigration],
    criteria: &PendingCriteria<'_>,
) -> Result<PendingSelection<'a>> {
    let mut versioned: Vec<&ResolvedMigration> = Vec::new();

    for migration in resolved.iter().filter(|m| m.is_versioned()) {
        if !should_run_in_environment(&migration.directives, criteria.current_env) {
            continue;
        }
        // `is_versioned()` guarantees a version is present.
        let version = match migration.version() {
            Some(v) => v,
            None => continue,
        };

        if criteria.effective_versions.contains(&version.raw) {
            continue;
        }
        if let Some(baseline) = criteria.baseline_version
            && version <= baseline
        {
            log::debug!("Skipping {} (below baseline)", migration.script);
            continue;
        }
        if let Some(target) = criteria.target
            && version > target
        {
            log::debug!("Skipping {} (above target {})", migration.script, target);
            continue;
        }
        if !criteria.out_of_order
            && let Some(highest) = criteria.highest_applied
            && version < highest
        {
            return Err(WaypointError::OutOfOrder {
                version: version.raw.clone(),
                highest: highest.raw.clone(),
            });
        }

        versioned.push(migration);
    }

    if criteria.dependency_ordering {
        order_by_dependencies(resolved, &mut versioned)?;
    } else {
        versioned.sort_by(|a, b| a.version().cmp(&b.version()));
    }

    let repeatables: Vec<&ResolvedMigration> = resolved
        .iter()
        .filter(|m| !m.is_versioned() && !m.is_undo())
        .filter(|m| should_run_in_environment(&m.directives, criteria.current_env))
        .filter(|m| match criteria.applied_scripts.get(&m.script) {
            None => true,
            Some(applied) => *applied != Some(m.checksum),
        })
        .collect();

    Ok(PendingSelection {
        versioned,
        repeatables,
    })
}

/// Reorder `pending` into a topological order honouring `-- waypoint:depends`.
///
/// The graph is built over *all* resolved migrations, not just the pending
/// ones, so a `depends` on an already-applied version still resolves instead of
/// reporting a missing dependency. Pending migrations are then emitted in the
/// order the sort produced.
fn order_by_dependencies(
    resolved: &[ResolvedMigration],
    pending: &mut Vec<&ResolvedMigration>,
) -> Result<()> {
    let all: Vec<&ResolvedMigration> = resolved.iter().collect();
    // `implicit_chain = true` keeps plain version order for any migration that
    // declares no dependencies, so turning this on is behaviour-preserving for
    // projects that never use the directive.
    let graph = crate::dependency::DependencyGraph::build(&all, true)?;
    let order = graph.topological_sort()?;

    let rank: HashMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(i, v)| (v.as_str(), i))
        .collect();

    pending.sort_by_key(|m| {
        m.version()
            .and_then(|v| rank.get(v.raw.as_str()).copied())
            // A migration absent from the graph sorts last rather than
            // panicking; it cannot happen for versioned migrations, which the
            // graph always contains.
            .unwrap_or(usize::MAX)
    });
    Ok(())
}

/// Check if a migration should run in the current environment.
///
/// Returns true if:
/// - The migration has no env directives (runs everywhere)
/// - No environment is configured (runs everything)
/// - The migration's env list includes the current environment
pub(crate) fn should_run_in_environment(
    directives: &MigrationDirectives,
    current_env: Option<&str>,
) -> bool {
    if directives.env.is_empty() {
        return true;
    }
    let env = match current_env {
        Some(e) => e,
        None => return true,
    };
    directives.env.iter().any(|e| e.eq_ignore_ascii_case(env))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_run_in_environment_no_directives() {
        let directives = MigrationDirectives::default();
        assert!(should_run_in_environment(&directives, Some("production")));
        assert!(should_run_in_environment(&directives, None));
    }

    #[test]
    fn test_should_run_in_environment_matches() {
        let directives = MigrationDirectives {
            env: vec!["production".to_string(), "staging".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, Some("production")));
        assert!(should_run_in_environment(&directives, Some("staging")));
        assert!(!should_run_in_environment(&directives, Some("dev")));
    }

    #[test]
    fn test_should_run_in_environment_case_insensitive() {
        let directives = MigrationDirectives {
            env: vec!["PROD".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, Some("prod")));
        assert!(should_run_in_environment(&directives, Some("PROD")));
        assert!(should_run_in_environment(&directives, Some("Prod")));
        assert!(!should_run_in_environment(&directives, Some("dev")));
    }

    #[test]
    fn test_should_run_in_environment_no_env_configured() {
        let directives = MigrationDirectives {
            env: vec!["prod".to_string()],
            ..Default::default()
        };
        assert!(should_run_in_environment(&directives, None));
    }

    use crate::migration::{MigrationKind, MigrationVersion};

    fn mig(name: &str, depends: &[&str]) -> ResolvedMigration {
        let (kind, description) = crate::migration::parse_migration_filename(name).unwrap();
        ResolvedMigration {
            kind,
            description,
            script: name.to_string(),
            checksum: 1,
            sql: String::new(),
            directives: MigrationDirectives {
                depends: depends.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
        }
    }

    fn criteria<'a>(
        applied: &'a HashSet<String>,
        scripts: &'a HashMap<String, Option<i32>>,
        highest: Option<&'a MigrationVersion>,
    ) -> PendingCriteria<'a> {
        PendingCriteria {
            effective_versions: applied,
            baseline_version: None,
            target: None,
            highest_applied: highest,
            applied_scripts: scripts,
            current_env: None,
            out_of_order: false,
            dependency_ordering: false,
        }
    }

    #[test]
    fn select_pending_orders_by_version() {
        let migs = vec![mig("V10__Ten.sql", &[]), mig("V2__Two.sql", &[])];
        let applied = HashSet::new();
        let scripts = HashMap::new();
        let out = select_pending(&migs, &criteria(&applied, &scripts, None)).unwrap();
        let order: Vec<&str> = out.versioned.iter().map(|m| m.script.as_str()).collect();
        assert_eq!(order, vec!["V2__Two.sql", "V10__Ten.sql"]);
    }

    #[test]
    fn select_pending_errors_on_out_of_order() {
        // V1 pending while V5 is already applied, with out_of_order disabled:
        // must be an error, never a silent skip.
        let migs = vec![mig("V1__One.sql", &[])];
        let mut applied = HashSet::new();
        applied.insert("5".to_string());
        let scripts = HashMap::new();
        let highest = MigrationVersion::parse("5").unwrap();
        let err = select_pending(&migs, &criteria(&applied, &scripts, Some(&highest))).unwrap_err();
        assert!(
            matches!(err, WaypointError::OutOfOrder { .. }),
            "expected OutOfOrder, got {err:?}"
        );
    }

    #[test]
    fn select_pending_allows_out_of_order_when_enabled() {
        let migs = vec![mig("V1__One.sql", &[])];
        let mut applied = HashSet::new();
        applied.insert("5".to_string());
        let scripts = HashMap::new();
        let highest = MigrationVersion::parse("5").unwrap();
        let mut c = criteria(&applied, &scripts, Some(&highest));
        c.out_of_order = true;
        let out = select_pending(&migs, &c).unwrap();
        assert_eq!(out.versioned.len(), 1);
    }

    #[test]
    fn select_pending_honours_depends_directive() {
        // V2 declares a dependency on V3, so it must run *after* it even though
        // its version sorts lower.
        let migs = vec![
            mig("V1__One.sql", &[]),
            mig("V2__Two.sql", &["3"]),
            mig("V3__Three.sql", &[]),
        ];
        let applied = HashSet::new();
        let scripts = HashMap::new();
        let mut c = criteria(&applied, &scripts, None);
        c.dependency_ordering = true;
        let out = select_pending(&migs, &c).unwrap();
        let order: Vec<&str> = out.versioned.iter().map(|m| m.script.as_str()).collect();
        assert_eq!(
            order,
            vec!["V1__One.sql", "V3__Three.sql", "V2__Two.sql"],
            "V2 depends on V3 so it must follow it"
        );
    }

    #[test]
    fn select_pending_dependency_ordering_is_version_order_without_directives() {
        let migs = vec![
            mig("V1__One.sql", &[]),
            mig("V2__Two.sql", &[]),
            mig("V3__Three.sql", &[]),
        ];
        let applied = HashSet::new();
        let scripts = HashMap::new();
        let mut c = criteria(&applied, &scripts, None);
        c.dependency_ordering = true;
        let out = select_pending(&migs, &c).unwrap();
        let order: Vec<&str> = out.versioned.iter().map(|m| m.script.as_str()).collect();
        assert_eq!(
            order,
            vec!["V1__One.sql", "V2__Two.sql", "V3__Three.sql"],
            "no directives means dependency ordering degrades to version order"
        );
    }

    #[test]
    fn select_pending_repeatable_reruns_on_checksum_change() {
        let mut r = mig("V1__One.sql", &[]);
        r.kind = MigrationKind::Repeatable;
        r.script = "R__View.sql".to_string();
        r.checksum = 99;
        let migs = vec![r];
        let applied = HashSet::new();

        let mut scripts = HashMap::new();
        scripts.insert("R__View.sql".to_string(), Some(99));
        let out = select_pending(&migs, &criteria(&applied, &scripts, None)).unwrap();
        assert!(
            out.repeatables.is_empty(),
            "unchanged checksum must not re-run"
        );

        let mut scripts = HashMap::new();
        scripts.insert("R__View.sql".to_string(), Some(1));
        let out = select_pending(&migs, &criteria(&applied, &scripts, None)).unwrap();
        assert_eq!(out.repeatables.len(), 1, "changed checksum must re-run");
    }
}
