use std::collections::HashMap;

use regex::Regex;

use crate::error::{Result, WaypointError};

/// Replace all `${key}` placeholders in the given SQL string.
///
/// Lookup is case-insensitive. If a placeholder key is not found in the map,
/// an error is returned listing available placeholders.
pub fn replace_placeholders(sql: &str, placeholders: &HashMap<String, String>) -> Result<String> {
    let re = Regex::new(r"\$\{([^}]+)\}").unwrap();

    // Build a lowercase lookup map
    let lower_map: HashMap<String, &String> = placeholders
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect();

    let mut result = String::with_capacity(sql.len());
    let mut last_end = 0;

    for caps in re.captures_iter(sql) {
        let full_match = caps.get(0).unwrap();
        let key = caps.get(1).unwrap().as_str();
        let key_lower = key.to_lowercase();

        result.push_str(&sql[last_end..full_match.start()]);

        if let Some(value) = lower_map.get(&key_lower) {
            result.push_str(value);
        } else {
            let available: Vec<&str> = placeholders.keys().map(|k| k.as_str()).collect();
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
    fn test_build_placeholders_includes_builtins() {
        let user = HashMap::new();
        let map = build_placeholders(&user, "public", "admin", "mydb", "V1__test.sql");

        assert_eq!(map.get("waypoint:schema").unwrap(), "public");
        assert_eq!(map.get("waypoint:user").unwrap(), "admin");
        assert_eq!(map.get("waypoint:database").unwrap(), "mydb");
        assert_eq!(map.get("waypoint:filename").unwrap(), "V1__test.sql");
        assert!(map.contains_key("waypoint:timestamp"));
    }
}
