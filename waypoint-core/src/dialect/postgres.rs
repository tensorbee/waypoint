//! PostgreSQL dialect implementation.

use super::{DatabaseDialect, DialectKind};

/// PostgreSQL 12+ dialect.
pub struct PostgresDialect;

impl DatabaseDialect for PostgresDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Postgres
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn history_table_ddl(&self, schema: &str, table: &str) -> String {
        let fq = self.qualified_table(schema, table);
        let success_idx = self.quote_ident(&format!("{}_s_idx", table));
        let version_idx = self.quote_ident(&format!("{}_v_idx", table));
        format!(
            r#"
CREATE TABLE IF NOT EXISTS {fq} (
    installed_rank INTEGER PRIMARY KEY,
    version        VARCHAR(50),
    description    VARCHAR(200) NOT NULL,
    type           VARCHAR(20) NOT NULL,
    script         VARCHAR(1000) NOT NULL,
    checksum       INTEGER,
    installed_by   VARCHAR(100) NOT NULL,
    installed_on   TIMESTAMPTZ NOT NULL DEFAULT now(),
    execution_time INTEGER NOT NULL,
    success        BOOLEAN NOT NULL,
    reversal_sql   TEXT
);

CREATE INDEX IF NOT EXISTS {success_idx} ON {fq} (success);
CREATE INDEX IF NOT EXISTS {version_idx} ON {fq} (version);
"#
        )
    }

    fn placeholder(&self, n: usize) -> String {
        format!("${}", n)
    }

    fn statement_timeout_sql(&self, secs: u32) -> Option<String> {
        if secs == 0 {
            None
        } else {
            Some(format!("SET statement_timeout = '{}s'", secs))
        }
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifier_with_double_quotes() {
        let d = PostgresDialect;
        assert_eq!(d.quote_ident("users"), r#""users""#);
        assert_eq!(d.quote_ident(r#"my"table"#), r#""my""table""#);
    }

    #[test]
    fn placeholder_is_dollar_indexed() {
        let d = PostgresDialect;
        assert_eq!(d.placeholder(1), "$1");
        assert_eq!(d.placeholder(7), "$7");
    }

    #[test]
    fn statement_timeout_uses_seconds() {
        let d = PostgresDialect;
        assert_eq!(
            d.statement_timeout_sql(30),
            Some("SET statement_timeout = '30s'".into())
        );
        assert_eq!(d.statement_timeout_sql(0), None);
    }

    #[test]
    fn history_ddl_contains_required_columns() {
        let d = PostgresDialect;
        let ddl = d.history_table_ddl("public", "waypoint_schema_history");
        for col in [
            "installed_rank",
            "version",
            "description",
            "type",
            "script",
            "checksum",
            "installed_by",
            "installed_on",
            "execution_time",
            "success",
            "reversal_sql",
        ] {
            assert!(ddl.contains(col), "DDL missing column {}", col);
        }
        assert!(ddl.contains("TIMESTAMPTZ"));
    }

    #[test]
    fn supports_transactional_ddl() {
        assert!(PostgresDialect.supports_transactional_ddl());
    }
}
