//! Parse `-- waypoint:*` comment directives from SQL file headers.
//!
//! Directives appear as SQL comments at the top of migration files:
//! ```sql
//! -- waypoint:env dev,staging
//! -- waypoint:depends V3,V5
//! CREATE TABLE ...
//! ```

/// Parsed directives from a migration file header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationDirectives {
    /// Dependencies: `-- waypoint:depends V3,V5` (V prefix is stripped)
    pub depends: Vec<String>,
    /// Environment tags: `-- waypoint:env dev,staging`
    pub env: Vec<String>,
    /// Preconditions: `-- waypoint:require table_exists("users")`
    pub require: Vec<String>,
    /// Postconditions: `-- waypoint:ensure column_exists("users", "email")`
    pub ensure: Vec<String>,
    /// Safety override: `-- waypoint:safety-override` bypasses DANGER blocks
    pub safety_override: bool,
}

/// Scope of an inline lint suppression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum LintIgnoreScope {
    /// `-- waypoint:lint-ignore` — applies to the next statement only.
    NextStatement,
    /// `-- waypoint:lint-ignore-file` — applies to the whole file.
    File,
}

impl std::fmt::Display for LintIgnoreScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LintIgnoreScope::NextStatement => write!(f, "statement"),
            LintIgnoreScope::File => write!(f, "file"),
        }
    }
}

/// An inline `-- waypoint:lint-ignore[-file]` directive.
///
/// ```sql
/// -- waypoint:lint-ignore E001 reason="backfilled by the ceremony writer"
/// ALTER TABLE t ADD COLUMN c int NOT NULL;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintIgnoreDirective {
    /// Whether this suppresses the next statement or the whole file.
    pub scope: LintIgnoreScope,
    /// Rule IDs named by the directive, uppercased. Never empty for a valid
    /// directive; an empty list means the directive named no rules.
    pub rules: Vec<String>,
    /// The mandatory `reason=...` value, if one was supplied.
    pub reason: Option<String>,
    /// 1-based line number of the directive.
    pub line: usize,
    /// Byte offset of the start of the directive's line.
    pub offset: usize,
}

/// Parse every `-- waypoint:lint-ignore[-file]` directive in a migration file.
///
/// Unlike the header directives, these may appear anywhere in the file, but
/// only on lines that contain nothing but the comment — a trailing comment on
/// a line of SQL is ignored, because its scope would be ambiguous.
pub fn parse_lint_ignores(sql: &str) -> Vec<LintIgnoreDirective> {
    let mut out = Vec::new();
    let mut offset = 0usize;

    // split('\n') rather than lines() so byte offsets stay exact on CRLF input.
    for (idx, line) in sql.split('\n').enumerate() {
        let line_offset = offset;
        offset += line.len() + 1;

        let trimmed = line.trim();
        let Some(body) = trimmed.strip_prefix("--") else {
            continue;
        };
        let body = body.trim();

        let (scope, rest) =
            if let Some(rest) = strip_directive_prefix(body, "waypoint:lint-ignore-file") {
                (LintIgnoreScope::File, rest)
            } else if let Some(rest) = strip_directive_prefix(body, "waypoint:lint-ignore") {
                (LintIgnoreScope::NextStatement, rest)
            } else {
                continue;
            };

        let (rules_part, reason) = split_reason(rest);
        let rules = rules_part
            .split([',', ' ', '\t'])
            .map(|r| r.trim())
            .filter(|r| !r.is_empty())
            .map(|r| r.to_uppercase())
            .collect();

        out.push(LintIgnoreDirective {
            scope,
            rules,
            reason,
            line: idx + 1,
            offset: line_offset,
        });
    }

    out
}

/// Split a directive tail into its rule list and its `reason=` value.
///
/// The reason runs to the end of the line and may be quoted with `"` or `'`.
fn split_reason(rest: &str) -> (&str, Option<String>) {
    let lower = rest.to_lowercase();
    let Some(pos) = lower.find("reason") else {
        return (rest, None);
    };
    // Require `reason` to be a standalone word followed by `=` or `:`.
    let after = rest[pos + "reason".len()..].trim_start();
    let Some(value) = after.strip_prefix('=').or_else(|| after.strip_prefix(':')) else {
        return (rest, None);
    };
    let value = value.trim();
    let value = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
        .trim();

    let reason = if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    };
    (&rest[..pos], reason)
}

/// Strip a directive prefix, ensuring the prefix is followed by whitespace or end of string.
/// This prevents prefix collisions like "waypoint:env" matching "waypoint:environment".
fn strip_directive_prefix<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    if let Some(rest) = line.strip_prefix(prefix) {
        if rest.is_empty() || rest.starts_with(char::is_whitespace) {
            Some(rest.trim())
        } else {
            None
        }
    } else {
        None
    }
}

/// Parse `-- waypoint:*` directives from SQL content.
///
/// Only parses comment lines (`--`) at the top of the file.
/// Stops at the first non-empty, non-comment line.
pub fn parse_directives(sql: &str) -> MigrationDirectives {
    let mut directives = MigrationDirectives::default();

    for line in sql.lines() {
        let trimmed = line.trim();

        // Skip empty lines at the top
        if trimmed.is_empty() {
            continue;
        }

        // Only process SQL comment lines
        if !trimmed.starts_with("--") {
            break;
        }

        let comment_body = trimmed.strip_prefix("--").unwrap().trim();

        if let Some(value) = strip_directive_prefix(comment_body, "waypoint:depends") {
            for item in value.split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    // Strip optional V prefix
                    let version = item.strip_prefix('V').unwrap_or(item);
                    directives.depends.push(version.to_string());
                }
            }
        } else if let Some(value) = strip_directive_prefix(comment_body, "waypoint:env") {
            for item in value.split(',') {
                let item = item.trim();
                if !item.is_empty() {
                    directives.env.push(item.to_string());
                }
            }
        } else if let Some(value) = strip_directive_prefix(comment_body, "waypoint:require") {
            if !value.is_empty() {
                directives.require.push(value.to_string());
            }
        } else if let Some(value) = strip_directive_prefix(comment_body, "waypoint:ensure") {
            if !value.is_empty() {
                directives.ensure.push(value.to_string());
            }
        } else if comment_body.trim() == "waypoint:safety-override" {
            directives.safety_override = true;
        } else if let Some(unknown) = unrecognised_directive(comment_body) {
            // A misspelled directive used to be indistinguishable from an
            // ordinary comment. `-- waypoint:requires table_exists("x")` — the
            // plural is an easy slip — silently dropped the precondition, and
            // the migration then ran without the guard the author wrote.
            log::warn!(
                "Unrecognised directive '-- waypoint:{}' — this line is being treated as an \
                 ordinary comment and has no effect. Known directives: depends, env, require, \
                 ensure, safety-override, lint-ignore, lint-ignore-file.",
                unknown
            );
        }
    }

    directives
}

/// The directive name in `comment_body`, if it looks like a `waypoint:`
/// directive but is not one we know.
///
/// Returns `None` for ordinary comments and for the `lint-ignore` family, which
/// [`parse_lint_ignores`] handles in its own pass over the file.
fn unrecognised_directive(comment_body: &str) -> Option<&str> {
    let name = comment_body.strip_prefix("waypoint:")?;
    let head = name
        .split_whitespace()
        .next()
        .unwrap_or(name)
        .trim_end_matches(':');
    if head.is_empty() || matches!(head, "lint-ignore" | "lint-ignore-file") {
        return None;
    }
    Some(head)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_env_directive() {
        let sql = "-- waypoint:env dev,staging\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev", "staging"]);
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_parse_depends_directive() {
        let sql = "-- waypoint:depends V3,V5\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.depends, vec!["3", "5"]);
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_depends_without_v_prefix() {
        let sql = "-- waypoint:depends 3,5\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.depends, vec!["3", "5"]);
    }

    #[test]
    fn test_parse_multiple_directives() {
        let sql = "-- waypoint:env dev\n-- waypoint:depends V1,V2\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev"]);
        assert_eq!(d.depends, vec!["1", "2"]);
    }

    #[test]
    fn test_stops_at_non_comment_line() {
        let sql = "-- waypoint:env dev\nCREATE TABLE foo();\n-- waypoint:env prod\n";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev"]);
    }

    #[test]
    fn test_empty_sql() {
        let d = parse_directives("");
        assert!(d.env.is_empty());
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_no_directives() {
        let sql = "-- Regular comment\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(d.env.is_empty());
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_skips_leading_blank_lines() {
        let sql = "\n\n-- waypoint:env prod\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["prod"]);
    }

    #[test]
    fn test_whitespace_in_values() {
        let sql = "-- waypoint:env  dev , staging , prod \nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.env, vec!["dev", "staging", "prod"]);
    }

    #[test]
    fn test_no_env_runs_everywhere() {
        let d = MigrationDirectives::default();
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_require_directive() {
        let sql = "-- waypoint:require table_exists(\"users\")\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.require, vec!["table_exists(\"users\")"]);
    }

    #[test]
    fn test_parse_ensure_directive() {
        let sql = "-- waypoint:ensure column_exists(\"users\", \"email\")\nALTER TABLE users ADD COLUMN email TEXT;";
        let d = parse_directives(sql);
        assert_eq!(d.ensure, vec!["column_exists(\"users\", \"email\")"]);
    }

    #[test]
    fn test_parse_multiple_guards() {
        let sql = "-- waypoint:require table_exists(\"users\")\n-- waypoint:require NOT column_exists(\"users\", \"email\")\n-- waypoint:ensure column_exists(\"users\", \"email\")\nALTER TABLE users ADD COLUMN email TEXT;";
        let d = parse_directives(sql);
        assert_eq!(d.require.len(), 2);
        assert_eq!(d.ensure.len(), 1);
    }

    #[test]
    fn test_parse_lint_ignore_next_statement() {
        let sql = "-- waypoint:lint-ignore E001 reason=\"empty table at deploy time\"\nALTER TABLE t ADD COLUMN a int NOT NULL;";
        let d = parse_lint_ignores(sql);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].scope, LintIgnoreScope::NextStatement);
        assert_eq!(d[0].rules, vec!["E001"]);
        assert_eq!(d[0].reason.as_deref(), Some("empty table at deploy time"));
        assert_eq!(d[0].line, 1);
        assert_eq!(d[0].offset, 0);
    }

    #[test]
    fn test_parse_lint_ignore_file_scope_and_multiple_rules() {
        let sql = "-- header\n-- waypoint:lint-ignore-file E001,W004 reason=legacy migration\nDROP TABLE t;";
        let d = parse_lint_ignores(sql);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].scope, LintIgnoreScope::File);
        assert_eq!(d[0].rules, vec!["E001", "W004"]);
        assert_eq!(d[0].reason.as_deref(), Some("legacy migration"));
        assert_eq!(d[0].line, 2);
    }

    #[test]
    fn test_parse_lint_ignore_without_reason() {
        let d = parse_lint_ignores("-- waypoint:lint-ignore E001\nSELECT 1;");
        assert_eq!(d.len(), 1);
        assert!(d[0].reason.is_none());
        assert_eq!(d[0].rules, vec!["E001"]);
    }

    #[test]
    fn test_parse_lint_ignore_without_rules() {
        let d = parse_lint_ignores("-- waypoint:lint-ignore reason=because\nSELECT 1;");
        assert_eq!(d.len(), 1);
        assert!(d[0].rules.is_empty());
        assert_eq!(d[0].reason.as_deref(), Some("because"));
    }

    #[test]
    fn test_parse_lint_ignore_offsets_are_exact() {
        let sql = "SELECT 1;\n-- waypoint:lint-ignore E001 reason=x\nSELECT 2;";
        let d = parse_lint_ignores(sql);
        assert_eq!(d[0].line, 2);
        assert_eq!(&sql[d[0].offset..d[0].offset + 2], "--");
    }

    #[test]
    fn test_trailing_comment_is_not_a_directive() {
        // Scope would be ambiguous, so only comment-only lines count.
        let d = parse_lint_ignores("SELECT 1; -- waypoint:lint-ignore E001 reason=x\n");
        assert!(d.is_empty());
    }

    #[test]
    fn test_lint_ignore_prefix_does_not_collide() {
        let d = parse_lint_ignores("-- waypoint:lint-ignore-file E001 reason=x\n");
        assert_eq!(d[0].scope, LintIgnoreScope::File);
        assert_eq!(d[0].rules, vec!["E001"]);
    }

    #[test]
    fn test_parse_safety_override() {
        let sql = "-- waypoint:safety-override\nALTER TABLE large_table ADD COLUMN foo TEXT;";
        let d = parse_directives(sql);
        assert!(d.safety_override);
    }

    #[test]
    fn test_safety_override_default_false() {
        let sql = "CREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(!d.safety_override);
    }

    #[test]
    fn test_env_prefix_does_not_match_ensure() {
        let sql = "-- waypoint:ensure column_exists(\"users\", \"email\")\nALTER TABLE users ADD COLUMN email TEXT;";
        let d = parse_directives(sql);
        // Should be parsed as ensure, not env
        assert!(d.env.is_empty());
        assert_eq!(d.ensure.len(), 1);
    }

    #[test]
    fn test_directive_prefix_boundary() {
        // "waypoint:environment" should NOT match "waypoint:env"
        let sql = "-- waypoint:environment prod\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        // Should NOT be parsed as env directive since "waypoint:environment" != "waypoint:env"
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_empty_depends() {
        let sql = "-- waypoint:depends\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(d.depends.is_empty());
    }

    #[test]
    fn test_parse_empty_env() {
        let sql = "-- waypoint:env\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert!(d.env.is_empty());
    }

    #[test]
    fn test_parse_require_with_special_chars() {
        let sql = "-- waypoint:require table_exists(\"my-table\")\nCREATE TABLE foo();";
        let d = parse_directives(sql);
        assert_eq!(d.require, vec!["table_exists(\"my-table\")"]);
    }

    #[test]
    fn test_unrecognised_directive_detects_typos_but_not_ordinary_comments() {
        // Typos in directive names used to be silently indistinguishable from
        // a plain comment, so a mistyped `require` dropped the precondition.
        assert_eq!(
            unrecognised_directive("waypoint:requires foo()"),
            Some("requires")
        );
        assert_eq!(
            unrecognised_directive("waypoint:saftey-override"),
            Some("saftey-override")
        );
        assert_eq!(
            unrecognised_directive("waypoint:ensures x"),
            Some("ensures")
        );

        // Known directives and ordinary comments are not flagged.
        assert_eq!(
            unrecognised_directive("waypoint:lint-ignore E001 reason=\"x\""),
            None
        );
        assert_eq!(
            unrecognised_directive("waypoint:lint-ignore-file E001 reason=\"x\""),
            None
        );
        assert_eq!(unrecognised_directive("just a normal comment"), None);
        assert_eq!(unrecognised_directive("waypoint is a tool"), None);
    }

    #[test]
    fn test_known_directives_still_parse_and_are_not_warned_about() {
        let sql = "-- waypoint:require table_exists(\"a\")\n\
                   -- waypoint:ensure table_exists(\"b\")\n\
                   -- waypoint:env prod\n\
                   -- waypoint:depends V1\n\
                   -- waypoint:safety-override\n\
                   SELECT 1;";
        let d = parse_directives(sql);
        assert_eq!(d.require.len(), 1);
        assert_eq!(d.ensure.len(), 1);
        assert_eq!(d.env, vec!["prod"]);
        assert_eq!(d.depends, vec!["1"]);
        assert!(d.safety_override);
        // None of these should look unrecognised.
        for body in [
            "waypoint:require x",
            "waypoint:ensure x",
            "waypoint:env prod",
            "waypoint:depends V1",
            "waypoint:safety-override",
        ] {
            let head = unrecognised_directive(body);
            assert!(
                matches!(
                    head,
                    Some("require" | "ensure" | "env" | "depends" | "safety-override")
                ),
                "known directive {body:?} classified as {head:?}"
            );
        }
    }
}
