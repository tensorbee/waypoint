//! MySQL 8.0+ dialect implementation.

use super::{DatabaseDialect, DialectKind};

/// MySQL 8.0+ dialect.
///
/// Note: MySQL does not support transactional DDL — most DDL statements
/// implicitly commit. This means batch-transaction mode and
/// "ensure-guards-rollback-on-failure" behavior are weaker than on PostgreSQL.
/// See [`DatabaseDialect::supports_transactional_ddl`].
pub struct MysqlDialect;

impl DatabaseDialect for MysqlDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Mysql
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn history_table_ddl(&self, schema: &str, table: &str) -> String {
        let fq = self.qualified_table(schema, table);
        let success_idx = self.quote_ident(&format!("{}_s_idx", table));
        let version_idx = self.quote_ident(&format!("{}_v_idx", table));
        // MySQL: TIMESTAMP (UTC) instead of TIMESTAMPTZ; ENGINE=InnoDB for txn support
        // on data-table operations; utf8mb4 to match modern defaults; no `IF NOT EXISTS`
        // on `CREATE INDEX` (not supported pre-8.0.29 reliably) — use plain CREATE INDEX
        // wrapped in a procedure-style guard would be too heavy here. Instead we rely
        // on the table-level IF NOT EXISTS plus a separate idempotent index check at
        // create time: MySQL silently errors with ER_DUP_KEYNAME if the index exists.
        // We emit them as separate statements so the caller can ignore that specific
        // duplicate-key error.
        format!(
            r#"
CREATE TABLE IF NOT EXISTS {fq} (
    installed_rank INT PRIMARY KEY,
    version        VARCHAR(50),
    description    VARCHAR(200) NOT NULL,
    type           VARCHAR(20) NOT NULL,
    script         VARCHAR(1000) NOT NULL,
    checksum       INT,
    installed_by   VARCHAR(100) NOT NULL,
    installed_on   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    execution_time INT NOT NULL,
    success        BOOLEAN NOT NULL,
    reversal_sql   LONGTEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE INDEX {success_idx} ON {fq} (success);
CREATE INDEX {version_idx} ON {fq} (version);
"#
        )
    }

    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }

    fn statement_timeout_sql(&self, secs: u32) -> Option<String> {
        if secs == 0 {
            None
        } else {
            // MAX_EXECUTION_TIME is in milliseconds and applies to SELECT only.
            // It's the closest equivalent we have at the session level.
            Some(format!(
                "SET SESSION MAX_EXECUTION_TIME = {}",
                (secs as u64) * 1000
            ))
        }
    }

    fn supports_transactional_ddl(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifier_with_backticks() {
        let d = MysqlDialect;
        assert_eq!(d.quote_ident("users"), "`users`");
        assert_eq!(d.quote_ident("my`table"), "`my``table`");
    }

    #[test]
    fn placeholder_is_question_mark_regardless_of_index() {
        let d = MysqlDialect;
        assert_eq!(d.placeholder(1), "?");
        assert_eq!(d.placeholder(7), "?");
    }

    #[test]
    fn statement_timeout_uses_milliseconds() {
        let d = MysqlDialect;
        assert_eq!(
            d.statement_timeout_sql(30),
            Some("SET SESSION MAX_EXECUTION_TIME = 30000".into())
        );
        assert_eq!(d.statement_timeout_sql(0), None);
    }

    #[test]
    fn history_ddl_uses_innodb_and_timestamp() {
        let d = MysqlDialect;
        let ddl = d.history_table_ddl("devdb", "waypoint_schema_history");
        assert!(ddl.contains("ENGINE=InnoDB"));
        assert!(ddl.contains("TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"));
        assert!(!ddl.contains("TIMESTAMPTZ"));
        assert!(ddl.contains("utf8mb4"));
    }

    #[test]
    fn does_not_support_transactional_ddl() {
        assert!(!MysqlDialect.supports_transactional_ddl());
    }
}
