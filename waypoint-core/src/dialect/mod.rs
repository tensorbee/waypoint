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

use crate::error::Result;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

/// Identifier of which dialect a connection or piece of code targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DialectKind {
    /// PostgreSQL 12+
    Postgres,
    /// MySQL 8.0+
    Mysql,
}

impl DialectKind {
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

    /// Quote a string literal for safe inclusion in dynamic SQL.
    ///
    /// Used in places where parameter binding is unavailable (e.g. statements that
    /// can't run inside a prepared statement). Doubles single quotes.
    fn quote_literal(&self, value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

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

    /// SQL placeholder syntax for the `n`-th parameter (1-indexed).
    ///
    /// PostgreSQL: `$1`, `$2`. MySQL: `?` (unindexed).
    fn placeholder(&self, n: usize) -> String;

    /// Statement to set the per-connection statement timeout in seconds.
    ///
    /// PostgreSQL: `SET statement_timeout = '<n>s'`.
    /// MySQL: `SET SESSION MAX_EXECUTION_TIME = <n_ms>` (millisecond units, SELECT-only).
    /// Returns `None` when the engine has no equivalent at this granularity.
    fn statement_timeout_sql(&self, secs: u32) -> Option<String>;

    /// Whether the engine supports atomic rollback of DDL inside a transaction.
    ///
    /// PostgreSQL: `true`. MySQL: `false` (most DDL implicitly commits).
    /// Used to gate `--transaction` batch mode and `ensure`-guards-in-transaction.
    fn supports_transactional_ddl(&self) -> bool;

    /// Whether the engine supports advisory locking with the semantics Waypoint
    /// expects (session-scoped, mutually exclusive, keyed by an integer).
    ///
    /// PostgreSQL: `pg_advisory_lock`. MySQL: `GET_LOCK` (named, session-scoped).
    /// Both are usable; the difference is the key type (i64 vs string).
    fn supports_advisory_locks(&self) -> bool {
        true
    }
}

/// Construct the dialect for a given kind. Returns an error if the corresponding
/// Cargo feature is not enabled.
pub fn dialect_for(kind: DialectKind) -> Result<Box<dyn DatabaseDialect>> {
    match kind {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => Ok(Box::new(postgres::PostgresDialect)),
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(crate::error::WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => Ok(Box::new(mysql::MysqlDialect)),
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(crate::error::WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
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
