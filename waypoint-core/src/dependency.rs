//! Migration dependency graph with topological sort.
//!
//! Supports `-- waypoint:depends V3,V5` directives for non-linear
//! migration ordering using Kahn's algorithm.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{Result, WaypointError};
use crate::migration::ResolvedMigration;

/// Whether `from` depends on `target`, directly or through other versions.
///
/// Plain DFS over the `depends-on` edges; migration counts are small enough
/// that the repeated traversal during graph construction is not worth caching.
fn depends_transitively(
    edges: &HashMap<String, HashSet<String>>,
    from: &str,
    target: &str,
) -> bool {
    let mut stack = vec![from];
    let mut seen: HashSet<&str> = HashSet::new();
    while let Some(node) = stack.pop() {
        if node == target {
            return true;
        }
        if !seen.insert(node) {
            continue;
        }
        if let Some(deps) = edges.get(node) {
            stack.extend(deps.iter().map(|s| s.as_str()));
        }
    }
    false
}

/// A directed acyclic graph of migration dependencies.
pub struct DependencyGraph {
    /// version -> set of versions it depends on
    edges: HashMap<String, HashSet<String>>,
    /// version -> set of versions that depend on it
    reverse_edges: HashMap<String, HashSet<String>>,
    /// All known versions
    all_versions: Vec<String>,
}

impl DependencyGraph {
    /// Build a dependency graph from resolved migrations.
    ///
    /// If `implicit_chain` is true, each versioned migration implicitly depends
    /// on the previous version in sort order (backward-compatible default).
    pub fn build(migrations: &[&ResolvedMigration], implicit_chain: bool) -> Result<Self> {
        let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
        let mut reverse_edges: HashMap<String, HashSet<String>> = HashMap::new();
        let mut all_versions: Vec<String> = Vec::new();

        // Collect all versioned migrations sorted by version
        let mut versioned: Vec<&ResolvedMigration> = migrations
            .iter()
            .filter(|m| m.is_versioned())
            .copied()
            .collect();
        versioned.sort_by(|a, b| a.version().unwrap().cmp(b.version().unwrap()));

        for m in &versioned {
            let version = m.version().unwrap().raw.clone();
            edges.entry(version.clone()).or_default();
            reverse_edges.entry(version.clone()).or_default();
            all_versions.push(version);
        }

        // Add explicit dependencies from directives
        for m in &versioned {
            let version = &m.version().unwrap().raw;
            for dep in &m.directives.depends {
                if !edges.contains_key(dep) {
                    return Err(WaypointError::MissingDependency {
                        version: version.clone(),
                        dependency: dep.clone(),
                    });
                }
                edges.get_mut(version.as_str()).unwrap().insert(dep.clone());
                reverse_edges
                    .get_mut(dep.as_str())
                    .unwrap()
                    .insert(version.clone());
            }
        }

        // Add implicit chain dependencies (each version depends on previous).
        //
        // An implicit edge must never contradict an explicit one. If `previous`
        // already depends — directly or transitively — on `current`, then
        // adding `current -> previous` closes a cycle, and the graph would be
        // rejected even though the migrations are perfectly orderable. That
        // happens whenever a lower version declares `depends` on a higher one:
        //
        //   V2 `-- waypoint:depends 3`   explicit: 2 -> 3
        //   V3 has no directives         implicit: 3 -> 2   ← false cycle
        //
        // Skipping the implicit edge in that case leaves the explicit
        // dependency to do the ordering, which is what the author asked for.
        if implicit_chain {
            for i in 1..all_versions.len() {
                let current = all_versions[i].clone();
                let previous = all_versions[i - 1].clone();

                let has_explicit_deps = edges.get(&current).is_some_and(|deps| !deps.is_empty());
                if has_explicit_deps {
                    continue;
                }
                if depends_transitively(&edges, &previous, &current) {
                    log::debug!(
                        "Skipping implicit dependency {} -> {}: {} already depends on {}",
                        current,
                        previous,
                        previous,
                        current
                    );
                    continue;
                }

                edges
                    .entry(current.clone())
                    .or_default()
                    .insert(previous.clone());
                reverse_edges.entry(previous).or_default().insert(current);
            }
        }

        Ok(DependencyGraph {
            edges,
            reverse_edges,
            all_versions,
        })
    }

    /// Produce a topologically sorted order of versions using Kahn's algorithm.
    ///
    /// Uses borrowed `&str` references internally to avoid cloning during
    /// the sort; only clones into owned `String`s for the output.
    pub fn topological_sort(&self) -> Result<Vec<String>> {
        // Compute in-degree for each node using borrowed keys
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        for v in &self.all_versions {
            in_degree.insert(v, self.edges.get(v).map_or(0, |deps| deps.len()));
        }

        // Start with nodes that have no dependencies
        let mut queue: VecDeque<&str> = VecDeque::new();
        for v in &self.all_versions {
            if *in_degree.get(v.as_str()).unwrap_or(&0) == 0 {
                queue.push_back(v);
            }
        }

        let mut sorted = Vec::new();

        while let Some(node) = queue.pop_front() {
            sorted.push(node.to_string());

            // For each node that depends on this one, decrement in-degree
            if let Some(dependents) = self.reverse_edges.get(node) {
                for dep in dependents {
                    let deg = in_degree.get_mut(dep.as_str()).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep);
                    }
                }
            }
        }

        if sorted.len() != self.all_versions.len() {
            // Trace an actual cycle path — convert in_degree to owned keys for trace_cycle
            let owned_in_degree: HashMap<String, usize> = in_degree
                .iter()
                .map(|(&k, &v)| (k.to_string(), v))
                .collect();
            let cycle_path = self.trace_cycle(&owned_in_degree);
            return Err(WaypointError::DependencyCycle { path: cycle_path });
        }

        Ok(sorted)
    }

    /// Trace an actual cycle path for error reporting.
    fn trace_cycle(&self, in_degree: &HashMap<String, usize>) -> String {
        // Start from any node still in the cycle
        let start = self
            .all_versions
            .iter()
            .find(|v| *in_degree.get(*v).unwrap_or(&0) > 0);

        let Some(start) = start else {
            return "unknown cycle".to_string();
        };

        // Follow dependency edges to trace the cycle
        let mut path = vec![start.clone()];
        let mut current = start.clone();
        let mut visited = std::collections::HashSet::new();
        visited.insert(current.clone());

        loop {
            // Find a dependency of `current` that is also in the cycle
            let next = self
                .edges
                .get(&current)
                .and_then(|deps| deps.iter().find(|d| *in_degree.get(*d).unwrap_or(&0) > 0));

            match next {
                Some(n) => {
                    if !visited.insert(n.clone()) {
                        // We've come back to a visited node — complete the cycle
                        path.push(n.clone());
                        // Trim path to start from the cycle entry point
                        if let Some(pos) = path.iter().position(|v| v == n) {
                            let cycle: Vec<String> = path[pos..].to_vec();
                            return cycle.join(" -> ");
                        }
                        return path.join(" -> ");
                    }
                    path.push(n.clone());
                    current = n.clone();
                }
                None => {
                    // Fallback: list all nodes in cycle
                    let in_cycle: Vec<String> = self
                        .all_versions
                        .iter()
                        .filter(|v| *in_degree.get(*v).unwrap_or(&0) > 0)
                        .cloned()
                        .collect();
                    return format!("cycle involving: {}", in_cycle.join(", "));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directive::MigrationDirectives;
    use crate::migration::{MigrationKind, MigrationVersion, ResolvedMigration};

    fn make_migration(version: &str, depends: Vec<&str>) -> ResolvedMigration {
        ResolvedMigration {
            kind: MigrationKind::Versioned(MigrationVersion::parse(version).unwrap()),
            description: format!("V{}", version),
            script: format!("V{}__test.sql", version),
            checksum: 0,
            sql: String::new(),
            directives: MigrationDirectives {
                depends: depends.into_iter().map(String::from).collect(),
                env: vec![],
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_simple_chain() {
        let m1 = make_migration("1", vec![]);
        let m2 = make_migration("2", vec![]);
        let m3 = make_migration("3", vec![]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2, &m3];

        let graph = DependencyGraph::build(&migrations, true).unwrap();
        let order = graph.topological_sort().unwrap();
        assert_eq!(order, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_explicit_dependency() {
        let m1 = make_migration("1", vec![]);
        let m2 = make_migration("2", vec![]);
        let m3 = make_migration("3", vec!["1"]); // V3 depends on V1, skipping V2
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2, &m3];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        let order = graph.topological_sort().unwrap();
        // V1 must come before V3, V2 has no deps so can be anywhere
        let pos1 = order.iter().position(|v| v == "1").unwrap();
        let pos3 = order.iter().position(|v| v == "3").unwrap();
        assert!(pos1 < pos3);
    }

    #[test]
    fn test_cycle_detection() {
        let m1 = make_migration("1", vec!["2"]);
        let m2 = make_migration("2", vec!["1"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        assert!(graph.topological_sort().is_err());
    }

    #[test]
    fn test_missing_dependency() {
        let m1 = make_migration("1", vec!["99"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1];

        assert!(DependencyGraph::build(&migrations, false).is_err());
    }

    #[test]
    fn test_cycle_error_shows_path() {
        let m1 = make_migration("1", vec!["3"]);
        let m2 = make_migration("2", vec!["1"]);
        let m3 = make_migration("3", vec!["2"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2, &m3];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        let err = graph.topological_sort().unwrap_err();
        let msg = err.to_string();
        // The error should contain cycle path information
        assert!(msg.contains("->"), "Cycle error should show path: {}", msg);
    }

    #[test]
    fn test_empty_migrations() {
        let migrations: Vec<&ResolvedMigration> = vec![];
        let graph = DependencyGraph::build(&migrations, true).unwrap();
        let order = graph.topological_sort().unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_single_migration() {
        let m1 = make_migration("1", vec![]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1];
        let graph = DependencyGraph::build(&migrations, true).unwrap();
        let order = graph.topological_sort().unwrap();
        assert_eq!(order, vec!["1"]);
    }

    #[test]
    fn test_diamond_dependency() {
        let m1 = make_migration("1", vec![]);
        let m2 = make_migration("2", vec!["1"]);
        let m3 = make_migration("3", vec!["1"]);
        let m4 = make_migration("4", vec!["2", "3"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1, &m2, &m3, &m4];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        let order = graph.topological_sort().unwrap();

        // V1 must be first, V4 must be last
        assert_eq!(order[0], "1");
        assert_eq!(order[3], "4");
    }

    #[test]
    fn test_self_referencing_cycle() {
        let m1 = make_migration("1", vec!["1"]);
        let migrations: Vec<&ResolvedMigration> = vec![&m1];

        let graph = DependencyGraph::build(&migrations, false).unwrap();
        assert!(graph.topological_sort().is_err());
    }
}
