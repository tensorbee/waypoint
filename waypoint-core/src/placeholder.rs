//! Placeholder replacement in SQL (`${key}` syntax).

use std::collections::HashMap;
use std::sync::LazyLock;

use regex_lite::Regex;

use crate::error::{Result, WaypointError};

/// Compiled regex for matching `${key}` placeholders.
static PLACEHOLDER_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\$\{([^}]+)\}").unwrap());

/// Replace all `${key}` placeholders in the given SQL string.
///
/// Lookup is case-insensitive. If a placeholder key is not found in the map,
/// an error is returned listing available placeholders.
///
/// Placeholders inside dollar-quoted blocks (`$$...$$` or `$tag$...$tag$`) are
/// left untouched, since dollar-quoted content is literal SQL.
pub fn replace_placeholders(sql: &str, placeholders: &HashMap<String, String>) -> Result<String> {
    let re = &*PLACEHOLDER_RE;

    // Build a lowercase lookup map
    let lower_map: HashMap<String, &String> = placeholders
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect();

    // Find all dollar-quoted regions to skip
    let dollar_regions = find_dollar_quoted_regions(sql);

    let mut result = String::with_capacity(sql.len());
    let mut last_end = 0;

    for caps in re.captures_iter(sql) {
        let full_match = caps.get(0).unwrap();
        let key = caps.get(1).unwrap().as_str();

        // Skip matches inside dollar-quoted regions
        if dollar_regions
            .iter()
            .any(|&(start, end)| full_match.start() >= start && full_match.end() <= end)
        {
            continue;
        }

        let key_lower = key.to_lowercase();

        result.push_str(&sql[last_end..full_match.start()]);

        if let Some(value) = lower_map.get(&key_lower) {
            result.push_str(value);
        } else {
            // Sorted: `HashMap::keys` yields in hash order, so the same error
            // listed the available placeholders differently on every run.
            let mut available: Vec<&str> = placeholders.keys().map(|k| k.as_str()).collect();
            available.sort_unstable();
            return Err(WaypointError::PlaceholderNotFound {
                key: key.to_string(),
                available: if available.is_empty() {
                    "(none)".to_string()
                } else {
                    available.join(", ")
                },
            });
        }

        last_end = full_match.end();
    }

    result.push_str(&sql[last_end..]);
    Ok(result)
}

/// Find byte ranges of dollar-quoted regions in SQL.
/// Returns a vec of (start, end) byte offsets for each `$tag$...$tag$` region.
fn find_dollar_quoted_regions(sql: &str) -> Vec<(usize, usize)> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut regions = Vec::new();
    let mut i = 0;

    while i < len {
        match bytes[i] {
            // Skip string literals
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            // Skip single-line comments
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            // Skip block comments
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                let mut depth = 1;
                while i < len && depth > 0 {
                    if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                        depth += 1;
                        i += 2;
                    } else if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            // Dollar-quoted string
            b'$' => {
                let region_start = i;
                let tag_start = i;
                i += 1;
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if i < len && bytes[i] == b'$' {
                    let tag = &sql[tag_start..=i];
                    i += 1;
                    // Find closing tag
                    loop {
                        if i >= len {
                            break;
                        }
                        if bytes[i] == b'$' {
                            let remaining = &sql[i..];
                            if remaining.starts_with(tag) {
                                i += tag.len();
                                regions.push((region_start, i));
                                break;
                            }
                        }
                        i += 1;
                    }
                }
                continue;
            }
            _ => {
                i += 1;
            }
        }
    }

    regions
}

/// Build the full placeholder map including built-in waypoint placeholders.
pub fn build_placeholders(
    user_placeholders: &HashMap<String, String>,
    schema: &str,
    user: &str,
    database: &str,
    filename: &str,
) -> HashMap<String, String> {
    let mut map = user_placeholders.clone();

    map.insert("waypoint:schema".to_string(), schema.to_string());
    map.insert("waypoint:user".to_string(), user.to_string());
    map.insert("waypoint:database".to_string(), database.to_string());
    map.insert(
        "waypoint:timestamp".to_string(),
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    );
    map.insert("waypoint:filename".to_string(), filename.to_string());

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_placeholders() {
        let mut placeholders = HashMap::new();
        placeholders.insert("schema".to_string(), "public".to_string());
        placeholders.insert("table".to_string(), "users".to_string());

        let sql = "CREATE TABLE ${schema}.${table} (id SERIAL);";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert_eq!(result, "CREATE TABLE public.users (id SERIAL);");
    }

    #[test]
    fn test_replace_placeholders_case_insensitive() {
        let mut placeholders = HashMap::new();
        placeholders.insert("Schema".to_string(), "public".to_string());

        let sql = "SELECT * FROM ${schema}.users;";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert_eq!(result, "SELECT * FROM public.users;");
    }

    #[test]
    fn test_replace_placeholders_missing_key() {
        let placeholders = HashMap::new();
        let sql = "SELECT * FROM ${missing}.users;";
        let result = replace_placeholders(sql, &placeholders);
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_no_placeholders() {
        let placeholders = HashMap::new();
        let sql = "SELECT 1;";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert_eq!(result, "SELECT 1;");
    }

    #[test]
    fn test_replace_placeholders_skips_dollar_quoted() {
        let mut placeholders = HashMap::new();
        placeholders.insert("name".to_string(), "world".to_string());

        // ${name} inside dollar-quoted block should NOT be replaced
        let sql = "SELECT $$ ${name} $$;";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert!(result.contains("$$ ${name} $$"));
    }

    #[test]
    fn test_replace_placeholders_outside_dollar_quote() {
        let mut placeholders = HashMap::new();
        placeholders.insert("schema".to_string(), "public".to_string());

        let sql = "CREATE TABLE ${schema}.users (id SERIAL); CREATE FUNCTION foo() AS $$ SELECT 1; $$ LANGUAGE sql;";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert!(result.starts_with("CREATE TABLE public.users"));
    }

    #[test]
    fn test_build_placeholders_includes_builtins() {
        let user = HashMap::new();
        let map = build_placeholders(&user, "public", "admin", "mydb", "V1__test.sql");

        assert_eq!(map.get("waypoint:schema").unwrap(), "public");
        assert_eq!(map.get("waypoint:user").unwrap(), "admin");
        assert_eq!(map.get("waypoint:database").unwrap(), "mydb");
        assert_eq!(map.get("waypoint:filename").unwrap(), "V1__test.sql");
        assert!(map.contains_key("waypoint:timestamp"));
    }

    #[test]
    fn test_replace_multiple_same_placeholder() {
        let mut placeholders = HashMap::new();
        placeholders.insert("name".to_string(), "users".to_string());

        let sql = "SELECT * FROM ${name} WHERE ${name}.id = 1;";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE users.id = 1;");
    }

    #[test]
    fn test_replace_placeholder_at_start() {
        let mut placeholders = HashMap::new();
        placeholders.insert("tbl".to_string(), "users".to_string());

        let sql = "${tbl} IS a table";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert_eq!(result, "users IS a table");
    }

    #[test]
    fn test_find_dollar_quoted_regions_tagged() {
        let mut placeholders = HashMap::new();
        placeholders.insert("name".to_string(), "world".to_string());

        let sql = "SELECT $func$ ${name} $func$; SELECT '${name}';";
        let result = replace_placeholders(sql, &placeholders).unwrap();
        assert!(result.contains("$func$ ${name} $func$"));
        assert!(result.contains("'world'"));
    }
}
