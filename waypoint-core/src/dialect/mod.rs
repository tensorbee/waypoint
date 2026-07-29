//! Database dialect abstraction.
//!
//! Waypoint targets multiple SQL engines. Dialect-specific behavior — identifier
//! quoting, history-table DDL, lock-level mapping for DDL operations, statement
//! splitter rules, and so on — is funneled through the [`DatabaseDialect`] trait
//! so that the rest of the codebase can be engine-agnostic where possible and
//! explicit about engine-specific paths where not.
//!
//! Connection-dependent operations live on [`crate::db::DbClient`] which dispatches
//! based on its variant (Postgres / MySQL).

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

/// Identifier of which dialect a connection or piece of code targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DialectKind {
    /// PostgreSQL 12+
    #[default]
    Postgres,
    /// MySQL 8.0+
    Mysql,
}

impl std::fmt::Display for DialectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for DialectKind {
    type Err = crate::error::WaypointError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(DialectKind::Postgres),
            "mysql" | "mariadb" => Ok(DialectKind::Mysql),
            other => Err(crate::error::WaypointError::ConfigError(format!(
                "Invalid database engine '{}'. Use 'postgres' or 'mysql'.",
                other
            ))),
        }
    }
}

impl DialectKind {
    /// Canonical lowercase name (`"postgres"` / `"mysql"`).
    pub fn name(&self) -> &'static str {
        match self {
            DialectKind::Postgres => "postgres",
            DialectKind::Mysql => "mysql",
        }
    }

    /// Detect dialect from a connection URL scheme.
    ///
    /// Recognises `postgres://`, `postgresql://`, `mysql://`. Returns `None` for
    /// key=value style PG strings or unknown schemes — caller may need to fall
    /// back to an explicit `dialect = "..."` config field.
    pub fn from_url(url: &str) -> Option<Self> {
        let lower = url.trim_start().to_lowercase();
        if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
            Some(DialectKind::Postgres)
        } else if lower.starts_with("mysql://") {
            Some(DialectKind::Mysql)
        } else {
            None
        }
    }
}

/// Describes how migrations should be split, locked, and tracked on a given engine.
///
/// All methods are pure — they operate on strings or return DDL templates and do
/// not touch a database connection. Connection-dependent operations live on
/// [`crate::db::DbClient`].
pub trait DatabaseDialect: Send + Sync {
    /// Which dialect this is.
    fn kind(&self) -> DialectKind;

    /// Quote a SQL identifier for safe inclusion in dynamic SQL.
    ///
    /// PostgreSQL uses double-quotes (`"name"`), MySQL uses backticks (`\`name\``).
    /// Doubles any embedded quote character to escape it.
    fn quote_ident(&self, name: &str) -> String;

    /// Produce a fully-qualified table reference (`schema.table`).
    ///
    /// In MySQL the "schema" is the database; in PostgreSQL it's a schema namespace.
    /// Both use the same `qualifier.identifier` syntax in DDL, just with different
    /// quoting characters — handled by [`Self::quote_ident`].
    fn qualified_table(&self, schema: &str, table: &str) -> String {
        format!("{}.{}", self.quote_ident(schema), self.quote_ident(table))
    }

    /// DDL to (idempotently) create the schema-history table.
    ///
    /// Returns one or more `;`-separated statements. Caller is responsible for
    /// executing them via the appropriate driver. Schema, table, and index names
    /// are quoted with [`Self::quote_ident`].
    ///
    /// PostgreSQL uses `TIMESTAMPTZ`; MySQL uses `TIMESTAMP` (UTC by convention).
    /// Both store the same logical columns.
    fn history_table_ddl(&self, schema: &str, table: &str) -> String;

    /// Whether the engine supports atomic rollback of DDL inside a transaction.
    ///
    /// PostgreSQL: `true`. MySQL: `false` (most DDL implicitly commits).
    /// Used to gate `--transaction` batch mode — when this returns `false`,
    /// callers should refuse the `batch_transaction` config or return a clear
    /// error rather than silently no-op.
    fn supports_transactional_ddl(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_url_recognises_postgres() {
        assert_eq!(
            DialectKind::from_url("postgres://u:p@h/d"),
            Some(DialectKind::Postgres)
        );
        assert_eq!(
            DialectKind::from_url("postgresql://u:p@h/d"),
            Some(DialectKind::Postgres)
        );
        assert_eq!(
            DialectKind::from_url("POSTGRES://u:p@h/d"),
            Some(DialectKind::Postgres)
        );
    }

    #[test]
    fn from_url_recognises_mysql() {
        assert_eq!(
            DialectKind::from_url("mysql://u:p@h/d"),
            Some(DialectKind::Mysql)
        );
        assert_eq!(
            DialectKind::from_url("  mysql://h/d"),
            Some(DialectKind::Mysql)
        );
    }

    #[test]
    fn from_url_returns_none_for_kv_or_unknown() {
        assert_eq!(DialectKind::from_url("host=localhost user=postgres"), None);
        assert_eq!(DialectKind::from_url("sqlite://x"), None);
        assert_eq!(DialectKind::from_url(""), None);
    }
}
