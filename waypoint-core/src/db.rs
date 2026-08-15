//! Database connection, TLS support, advisory locking, and transaction execution.
//!
//! The functions in this module that take `&tokio_postgres::Client` are gated
//! behind the `postgres` feature and are the original PostgreSQL-only entry points.
//! New code paths should use [`DbClient`] which abstracts over the configured
//! backend (PostgreSQL or MySQL).

use crate::config::SslMode;
use crate::dialect::{DatabaseDialect, DialectKind};
use crate::error::{Result, WaypointError};
use std::path::PathBuf;

#[cfg(feature = "postgres")]
use fastrand;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

/// Transport-level connection settings: how to reach the server and how much
/// to trust it.
///
/// Introduced to stop the connect helpers growing another positional argument
/// — [`connect_with_full_config`] already took six, and TLS trust needs two
/// more. Build one with [`TransportConfig::from_database_config`].
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// TLS mode; see [`SslMode`] for the ladder.
    pub ssl_mode: SslMode,
    /// PEM file of CA certificates that replaces the built-in trust store.
    pub ssl_root_cert: Option<PathBuf>,
    /// Connection attempts to retry before giving up.
    pub retries: u32,
    /// Per-attempt connection timeout in seconds (0 disables).
    pub connect_timeout_secs: u32,
    /// `statement_timeout` to set once connected (0 leaves it alone).
    pub statement_timeout_secs: u32,
    /// TCP keepalive interval in seconds (0 disables).
    pub keepalive_secs: u32,
}

impl Default for TransportConfig {
    fn default() -> Self {
        // Mirrors `DatabaseConfig::default` so the two cannot drift apart.
        Self {
            ssl_mode: SslMode::Prefer,
            ssl_root_cert: None,
            retries: 0,
            connect_timeout_secs: 30,
            statement_timeout_secs: 0,
            keepalive_secs: 120,
        }
    }
}

impl TransportConfig {
    /// Extract the transport settings from a loaded `[database]` config.
    pub fn from_database_config(db: &crate::config::DatabaseConfig) -> Self {
        Self {
            ssl_mode: db.ssl_mode,
            ssl_root_cert: db.ssl_root_cert.clone(),
            retries: db.connect_retries,
            connect_timeout_secs: db.connect_timeout_secs,
            statement_timeout_secs: db.statement_timeout_secs,
            keepalive_secs: db.keepalive_secs,
        }
    }
}

/// Build a unique name for a throwaway schema or database.
///
/// `simulate` and `drift` each create a sandbox, work in it, and then drop it
/// unconditionally — including when their own `CREATE` failed. That makes the
/// name safety-critical rather than cosmetic: if two concurrent runs pick the
/// same one, the loser's `CREATE` fails and its cleanup then drops the sandbox
/// the *winner* is still using.
///
/// Both used to derive the name from a clock alone — milliseconds for
/// `simulate`, whole seconds for `drift` — so a collision needed only two runs
/// starting in the same tick. The process id and a random suffix make the name
/// unique across concurrent processes as well as within one.
///
/// Stays inside PostgreSQL's 63-byte and MySQL's 64-byte identifier limits for
/// the prefixes used here.
pub fn sandbox_name(prefix: &str) -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!(
        "{}_{}_{:x}_{:08x}",
        prefix,
        millis,
        std::process::id(),
        fastrand::u32(..)
    )
}

/// Quote a value as a SQL string literal.
///
/// Doubles any embedded single quote, which is the escape both PostgreSQL and
/// MySQL accept. Use this for anything that is *data* inside generated SQL —
/// enum labels, for instance — as opposed to an object name, which wants
/// [`quote_ident`].
///
/// Generated DDL used to interpolate enum labels with a bare `format!("'{}'")`,
/// so a label containing an apostrophe produced
/// `CREATE TYPE "mood" AS ENUM ('fine', 'it's bad')` — broken SQL in the
/// snapshot, which `restore` then skipped with only a warning.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Quote a SQL identifier to prevent SQL injection.
///
/// Doubles any embedded double-quotes and wraps in double-quotes — this is the
/// PostgreSQL convention. For MySQL identifier quoting use the dialect's
/// [`DatabaseDialect::quote_ident`].
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a SQL identifier the MySQL way: backticks, with embedded backticks
/// doubled.
///
/// The MySQL command paths build DDL from names read out of
/// `information_schema`. Those are server-provided rather than user-supplied,
/// but an identifier containing a backtick would still produce broken SQL, and
/// quoting uniformly means no call site has to reason about which names are
/// "safe". Mirrors [`quote_ident`], which does the same for PostgreSQL.
pub fn quote_ident_mysql(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Validate that a SQL identifier contains only safe characters.
///
/// Returns an error for names with characters outside `[a-zA-Z0-9_]`.
/// Even with quoting (defense in depth), we reject suspicious identifiers early.
pub fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(WaypointError::ConfigError(
            "Identifier cannot be empty".to_string(),
        ));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(WaypointError::ConfigError(format!(
            "Identifier '{}' contains invalid characters. Only [a-zA-Z0-9_] are allowed.",
            name
        )));
    }
    Ok(())
}

/// Engine-specific database connection wrapper.
///
/// Constructed by [`Waypoint::new`](crate::Waypoint::new) (which auto-detects
/// the engine from the connection URL) or by [`DbClient::with_postgres`] /
/// [`DbClient::with_mysql`] for callers that already have a connection.
///
/// Most internal command code currently still operates on a raw
/// `tokio_postgres::Client` obtained via [`Self::as_postgres`]. As MySQL support
/// rolls out command-by-command, those call sites move to dialect-aware code.
pub enum DbClient {
    /// PostgreSQL connection.
    #[cfg(feature = "postgres")]
    Postgres(Client),
    /// MySQL connection pool. We use a pool because `mysql_async::Conn` requires
    /// `&mut self` for queries, which would force every command to take
    /// `&mut DbClient` — disruptive to the existing API. The pool exposes a
    /// `&self` checkout API.
    #[cfg(feature = "mysql")]
    Mysql(mysql_async::Pool),
}

impl DbClient {
    /// Wrap an existing PostgreSQL client.
    #[cfg(feature = "postgres")]
    pub fn with_postgres(client: Client) -> Self {
        DbClient::Postgres(client)
    }

    /// Wrap an existing MySQL pool.
    #[cfg(feature = "mysql")]
    pub fn with_mysql(pool: mysql_async::Pool) -> Self {
        DbClient::Mysql(pool)
    }

    /// Identify which dialect this connection is for.
    pub fn dialect_kind(&self) -> DialectKind {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(_) => DialectKind::Postgres,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(_) => DialectKind::Mysql,
        }
    }

    /// Borrow the dialect helper for this connection.
    ///
    /// Both `PostgresDialect` and `MysqlDialect` are zero-sized, so this returns
    /// a static reference rather than allocating a new `Box` per call.
    pub fn dialect(&self) -> &'static dyn DatabaseDialect {
        #[cfg(feature = "postgres")]
        static PG: crate::dialect::postgres::PostgresDialect =
            crate::dialect::postgres::PostgresDialect;
        #[cfg(feature = "mysql")]
        static MY: crate::dialect::mysql::MysqlDialect = crate::dialect::mysql::MysqlDialect;
        match self.dialect_kind() {
            #[cfg(feature = "postgres")]
            DialectKind::Postgres => &PG,
            #[cfg(not(feature = "postgres"))]
            DialectKind::Postgres => {
                panic!("PostgreSQL connection without `postgres` feature compiled in")
            }
            #[cfg(feature = "mysql")]
            DialectKind::Mysql => &MY,
            #[cfg(not(feature = "mysql"))]
            DialectKind::Mysql => {
                panic!("MySQL connection without `mysql` feature compiled in")
            }
        }
    }

    /// Borrow the inner PostgreSQL client. Returns an error if this DbClient
    /// is not a PostgreSQL connection — used as a transitional bridge for
    /// command code that hasn't been ported to dialect-aware operation yet.
    #[cfg(feature = "postgres")]
    pub fn as_postgres(&self) -> Result<&Client> {
        match self {
            DbClient::Postgres(c) => Ok(c),
            #[cfg(feature = "mysql")]
            DbClient::Mysql(_) => Err(WaypointError::ConfigError(
                "This operation is not yet implemented for MySQL".into(),
            )),
        }
    }

    /// Borrow the inner MySQL pool. Returns an error if this DbClient is not
    /// a MySQL connection.
    #[cfg(feature = "mysql")]
    pub fn as_mysql(&self) -> Result<&mysql_async::Pool> {
        match self {
            DbClient::Mysql(p) => Ok(p),
            #[cfg(feature = "postgres")]
            DbClient::Postgres(_) => Err(WaypointError::ConfigError(
                "This operation requires a MySQL connection".into(),
            )),
        }
    }

    /// Verify the database connection is still alive with a minimal round-trip.
    pub async fn check_connection(&self) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => check_connection(c).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let mut conn =
                    pool.get_conn()
                        .await
                        .map_err(|e| WaypointError::ConnectionLost {
                            operation: "health check".into(),
                            detail: e.to_string(),
                        })?;
                conn.query_drop("DO 0")
                    .await
                    .map_err(|e| WaypointError::ConnectionLost {
                        operation: "health check".into(),
                        detail: e.to_string(),
                    })?;
                Ok(())
            }
        }
    }

    /// Acquire a session-scoped advisory lock keyed by the history-table name.
    ///
    /// PostgreSQL: `pg_advisory_lock(<i64>)` derived from a CRC32 of the table name.
    /// MySQL: `GET_LOCK('waypoint_<table>', -1)` (named, indefinite-wait).
    pub async fn acquire_lock(&self, schema: &str, table_name: &str) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => acquire_advisory_lock(c, schema, table_name).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let key = mysql_lock_key(schema, table_name);
                let mut conn = pool.get_conn().await?;
                let acquired: Option<i64> = conn
                    .exec_first("SELECT GET_LOCK(?, -1)", (key.clone(),))
                    .await?;
                match acquired {
                    Some(1) => {
                        park_lock_conn(pool, &key, conn);
                        Ok(())
                    }
                    _ => Err(WaypointError::LockError(format!(
                        "Failed to acquire MySQL named lock {}",
                        key
                    ))),
                }
            }
        }
    }

    /// Try to acquire the advisory lock, polling until acquired or timeout expires.
    pub async fn acquire_lock_with_timeout(
        &self,
        schema: &str,
        table_name: &str,
        timeout_secs: u32,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => {
                acquire_advisory_lock_with_timeout(c, schema, table_name, timeout_secs).await
            }
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let key = mysql_lock_key(schema, table_name);
                let mut conn = pool.get_conn().await?;
                let acquired: Option<i64> = conn
                    .exec_first("SELECT GET_LOCK(?, ?)", (key.clone(), timeout_secs as i64))
                    .await?;
                match acquired {
                    Some(1) => {
                        park_lock_conn(pool, &key, conn);
                        Ok(())
                    }
                    Some(0) => Err(WaypointError::LockError(format!(
                        "Timed out waiting for MySQL named lock {} after {}s",
                        key, timeout_secs
                    ))),
                    _ => Err(WaypointError::LockError(format!(
                        "Failed to acquire MySQL named lock {} (NULL result)",
                        key
                    ))),
                }
            }
        }
    }

    /// Release the advisory lock acquired via [`Self::acquire_lock`].
    pub async fn release_lock(&self, schema: &str, table_name: &str) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => release_advisory_lock(c, schema, table_name).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let key = mysql_lock_key(schema, table_name);
                // Release on the *same* session that acquired it. A different
                // connection's RELEASE_LOCK is a silent no-op (returns 0) and
                // would leak the lock until the server reaps the session.
                let mut conn = match unpark_lock_conn(pool, &key) {
                    Some(conn) => conn,
                    None => {
                        return Err(WaypointError::LockError(format!(
                            "No pinned connection holds MySQL named lock {} — \
                             release_lock called without a matching acquire_lock",
                            key
                        )));
                    }
                };
                let released = conn
                    .exec_first::<Option<i64>, _, _>("SELECT RELEASE_LOCK(?)", (key.clone(),))
                    .await;
                // Return the connection to the pool either way; dropping it
                // here also drops the lock, so a failed RELEASE_LOCK is not
                // fatal — the session reset on return clears it.
                drop(conn);
                match released {
                    Ok(Some(Some(1))) => Ok(()),
                    Ok(_) => {
                        log::warn!(
                            "RELEASE_LOCK({}) did not report success; the lock is released \
                             regardless because the holding session was returned to the pool",
                            key
                        );
                        Ok(())
                    }
                    Err(e) => Err(WaypointError::MysqlError(e)),
                }
            }
        }
    }

    /// Get the current database user/account.
    pub async fn current_user(&self) -> Result<String> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => get_current_user(c).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let mut conn = pool.get_conn().await?;
                let user: Option<String> = conn.query_first("SELECT CURRENT_USER()").await?;
                user.ok_or_else(|| {
                    WaypointError::ConfigError("CURRENT_USER() returned no rows".into())
                })
            }
        }
    }

    /// Get the current database name.
    pub async fn current_database(&self) -> Result<String> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => get_current_database(c).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let mut conn = pool.get_conn().await?;
                // DATABASE() returns NULL when no schema is selected on the connection
                let db: Option<Option<String>> = conn.query_first("SELECT DATABASE()").await?;
                match db.flatten() {
                    Some(name) => Ok(name),
                    None => Err(WaypointError::ConfigError(
                        "MySQL connection has no current database (none selected in URL)".into(),
                    )),
                }
            }
        }
    }

    /// Resolve the schema/database name to use for the history table.
    ///
    /// On PostgreSQL the configured value is used as-is. On MySQL there is no
    /// schema concept distinct from the database; if the configured value is
    /// the PG-default `"public"`, we fall back to the connection's current
    /// database so a PG-shaped config keeps working when pointed at MySQL.
    pub async fn resolve_schema(&self, configured: &str) -> Result<String> {
        match self.dialect_kind() {
            DialectKind::Postgres => Ok(configured.to_string()),
            DialectKind::Mysql => {
                if configured == "public" {
                    self.current_database().await
                } else {
                    Ok(configured.to_string())
                }
            }
        }
    }

    /// Run one or more `;`-separated SQL statements without an explicit transaction.
    ///
    /// On PostgreSQL this is a single `batch_execute` call. On MySQL it splits
    /// the batch into individual statements via
    /// [`crate::sql_parser::split_mysql_statements`] (mysql_async's underlying
    /// protocol doesn't accept multiple statements unless the connection is
    /// built with `CLIENT_MULTI_STATEMENTS`, which we deliberately avoid).
    /// Returns elapsed time in milliseconds.
    pub async fn execute_raw(&self, sql: &str) -> Result<i32> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => execute_raw(c, sql).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let start = std::time::Instant::now();
                let mut conn = pool.get_conn().await?;
                for stmt in crate::sql_parser::split_mysql_statements(sql) {
                    conn.query_drop(&stmt).await?;
                }
                Ok(start.elapsed().as_millis() as i32)
            }
        }
    }

    /// Run SQL inside a transaction where the engine supports DDL rollback.
    ///
    /// On PostgreSQL this issues `BEGIN` / `COMMIT` (with `ROLLBACK` on failure)
    /// around `batch_execute`. On MySQL most DDL implicitly commits, so a
    /// transaction wrapper provides no rollback guarantee for DDL — we issue
    /// the statements without a wrapper and surface failures as they arise.
    /// Callers needing strict batch atomicity should consult
    /// [`DatabaseDialect::supports_transactional_ddl`] before invoking.
    pub async fn execute_in_transaction(&self, sql: &str) -> Result<i32> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => execute_in_transaction(c, sql).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(_) => self.execute_raw(sql).await,
        }
    }
}

/// Connect to whichever backend the URL scheme indicates.
///
/// The single place that maps a connection string to a [`DbClient`]. Engine is
/// taken from the URL scheme (`postgres://` / `postgresql://` → PostgreSQL,
/// `mysql://` → MySQL); anything else — notably libpq `key=value` strings —
/// falls back to `config.database.engine`, which defaults to PostgreSQL.
///
/// PostgreSQL connections pick up the full `[database]` transport config
/// (SSL mode, retries, timeouts, keepalive).
pub async fn connect_for_url(
    conn_string: &str,
    #[cfg_attr(
        not(any(feature = "postgres", feature = "mysql")),
        allow(unused_variables)
    )]
    config: &crate::config::WaypointConfig,
) -> Result<DbClient> {
    let kind = DialectKind::from_url(conn_string).unwrap_or(config.database.engine);
    match kind {
        #[cfg(feature = "postgres")]
        DialectKind::Postgres => {
            let transport = TransportConfig::from_database_config(&config.database);
            let client = connect_with_transport(conn_string, &transport).await?;
            Ok(DbClient::with_postgres(client))
        }
        #[cfg(not(feature = "postgres"))]
        DialectKind::Postgres => Err(WaypointError::ConfigError(
            "PostgreSQL support is not compiled in (enable the `postgres` feature)".into(),
        )),
        #[cfg(feature = "mysql")]
        DialectKind::Mysql => {
            let pool = connect_mysql_pool(
                conn_string,
                config.database.ssl_mode,
                config.database.ssl_root_cert.as_deref(),
                config.database.statement_timeout_secs,
                config.database.keepalive_secs,
            )
            .await?;
            Ok(DbClient::with_mysql(pool))
        }
        #[cfg(not(feature = "mysql"))]
        DialectKind::Mysql => Err(WaypointError::ConfigError(
            "MySQL support is not compiled in (enable the `mysql` feature)".into(),
        )),
    }
}

/// Build a MySQL pool with TLS configured from the `[database]` settings.
///
/// Before 0.7.0 this was a bare `Pool::from_url`, which meant `ssl_mode` was
/// ignored entirely on MySQL and connections ran in plaintext unless the URL
/// itself carried `require_ssl=true`.
///
/// # The `prefer` probe
///
/// `mysql_async` has no opportunistic TLS: attaching `SslOpts` makes TLS
/// *mandatory*, and `Pool` is lazy, so a handshake failure would not surface
/// until the first query. To give `prefer` its libpq meaning we therefore
/// connect once eagerly and rebuild the pool without TLS if that fails. The
/// probe connection is returned to the pool rather than discarded, so the
/// successful path costs nothing extra. Every other mode stays lazy.
///
/// # `statement_timeout`
///
/// Applied as `SET SESSION MAX_EXECUTION_TIME`, which `docs/ENGINES.md` has
/// always documented but the code never issued — the setting was silently
/// ignored on MySQL. Note the scope the docs already state: MySQL's
/// `MAX_EXECUTION_TIME` bounds **read-only SELECTs only**, so unlike
/// PostgreSQL's `statement_timeout` it will not interrupt a long `ALTER TABLE`.
/// MySQL has no server-side equivalent that does.
///
/// # Transport settings that do not apply
///
/// `connect_timeout` has no counterpart in `mysql_async` 0.37, and
/// `connect_retries` has nothing to retry: `Pool` is lazy, so there is no
/// connect step here to wrap in a loop. Both are recorded as PostgreSQL-only in
/// `docs/ENGINES.md` rather than being silently dropped.
#[cfg(feature = "mysql")]
async fn connect_mysql_pool(
    conn_string: &str,
    ssl_mode: SslMode,
    ssl_root_cert: Option<&std::path::Path>,
    statement_timeout_secs: u32,
    keepalive_secs: u32,
) -> Result<mysql_async::Pool> {
    let base = mysql_async::Opts::from_url(conn_string)
        .map_err(|e| WaypointError::ConfigError(format!("Invalid MySQL connection URL: {}", e)))?;

    // `setup`, not `init`: the pool issues `COM_RESET_CONNECTION` when a `Conn`
    // is returned, which clears session variables. `init` runs only when the
    // connection is first opened, so the timeout survived exactly one checkout
    // and every later one silently ran unbounded. `setup` re-runs after each
    // reset.
    let mut builder = mysql_async::OptsBuilder::from_opts(base);

    if statement_timeout_secs > 0 {
        let millis = u64::from(statement_timeout_secs).saturating_mul(1000);
        log::debug!(
            "Setting MySQL MAX_EXECUTION_TIME={}ms (bounds SELECTs only; DDL is not interruptible \
             by it)",
            millis
        );
        builder = builder.setup(vec![format!("SET SESSION MAX_EXECUTION_TIME = {}", millis)]);
    }

    if keepalive_secs > 0 {
        builder = builder.tcp_keepalive(Some(std::time::Duration::from_secs(u64::from(
            keepalive_secs,
        ))));
    }

    let base = mysql_async::Opts::from(builder);

    // mysql_async writes the SSLRequest packet and then silently skips the
    // upgrade for socket connections, handing back a plaintext session that
    // reports success. Refuse instead of pretending the connection is
    // encrypted.
    if ssl_mode.requires_tls() && base.socket().is_some() {
        return Err(WaypointError::ConfigError(format!(
            "ssl_mode = '{}' requires TLS, but this MySQL connection uses a Unix \
             socket, which the driver cannot secure. Use a TCP host:port, or set \
             ssl_mode = 'disable'.",
            ssl_mode
        )));
    }

    // A URL that already spells out its TLS wishes (`require_ssl`, `verify_ca`,
    // …) wins while ssl_mode is still at its default, mirroring how the
    // PostgreSQL path treats an embedded `sslmode=`.
    if ssl_mode == SslMode::Prefer && base.ssl_opts().is_some() {
        log::debug!(
            "Using the TLS options from the MySQL connection URL (ssl_mode is at its default)."
        );
        return Ok(mysql_async::Pool::new(base));
    }

    let Some(ssl_opts) = crate::tls::make_mysql_ssl_opts(ssl_mode, ssl_root_cert) else {
        // ssl_mode = disable.
        return Ok(mysql_async::Pool::new(base));
    };

    let secure = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(base.clone()).ssl_opts(Some(ssl_opts)),
    );

    if ssl_mode != SslMode::Prefer {
        return Ok(secure);
    }

    match secure.get_conn().await {
        Ok(conn) => {
            drop(conn);
            Ok(secure)
        }
        // Only retry in plaintext when the failure was actually about TLS.
        // Falling back on *any* error would mask a wrong password behind a
        // second, differently-failing attempt and double the authentication
        // attempts against the server.
        Err(e) if mysql_tls_unavailable(&e) => {
            log::warn!(
                "MySQL server does not support TLS ({}); continuing with an UNENCRYPTED \
                 connection because ssl_mode is 'prefer'. Set ssl_mode to 'require' or \
                 higher to refuse this.",
                e
            );
            let _ = secure.disconnect().await;
            Ok(mysql_async::Pool::new(base))
        }
        Err(e) => Err(WaypointError::MysqlError(e)),
    }
}

/// Did this MySQL connection fail because TLS was unavailable, as opposed to
/// for an unrelated reason like bad credentials or a refused connection?
///
/// Matched on the typed error rather than its `Display` text — the string
/// approach is exactly what leaves `verify-ca` broken inside mysql_async
/// itself (see `tls::make_mysql_ssl_opts`).
#[cfg(feature = "mysql")]
fn mysql_tls_unavailable(e: &mysql_async::Error) -> bool {
    matches!(
        e,
        mysql_async::Error::Driver(mysql_async::DriverError::NoClientSslFlagFromServer)
    ) || matches!(e, mysql_async::Error::Io(mysql_async::IoError::Tls(_)))
}

/// Compute the MySQL named-lock key for a history table in a given database.
///
/// # Scoping
///
/// MySQL `GET_LOCK` names live in a **server-global** namespace, unlike
/// PostgreSQL advisory locks which are scoped to the current database. Keying
/// on the table name alone therefore made every database on a shared MySQL
/// server contend for one lock: migrating `app_staging` blocked a concurrent
/// migration of `app_prod`, even though they share nothing. Including the
/// database name restores per-database scoping and matches the PostgreSQL
/// behaviour.
///
/// # Length
///
/// `GET_LOCK` names are capped at 64 characters on MySQL 8.0+. Plain
/// truncation would let two distinct long `db.table` pairs collapse onto one
/// key — silently over-serialising, or worse, letting a caller release a lock
/// it does not hold. Over-long keys fall back to a CRC32 of the full name,
/// which is stable across versions and platforms.
#[cfg(feature = "mysql")]
fn mysql_lock_key(schema: &str, table_name: &str) -> String {
    let full = format!("waypoint_{}_{}", schema, table_name);
    if full.len() <= 64 {
        full
    } else {
        format!("waypoint_{:08x}", crc32fast::hash(full.as_bytes()))
    }
}

/// Registry of pinned connections that currently hold a MySQL named lock.
///
/// `GET_LOCK` is **session**-scoped, and `mysql_async`'s pool defaults to
/// `reset_connection = true`, which issues `COM_RESET_CONNECTION` when a
/// `Conn` is returned to the pool. `COM_RESET_CONNECTION` explicitly releases
/// locks acquired with `GET_LOCK()`. So acquiring the lock on a borrowed
/// connection and dropping it back into the pool releases the lock
/// immediately — the migration lock would provide no exclusion at all.
///
/// We therefore keep the acquiring `Conn` checked *out* of the pool for the
/// whole lock lifetime, parked here, and release the lock on that same
/// connection. A second acquire in the same process cannot get this
/// connection back (it is not in the pool), so it takes a fresh session and
/// blocks on `GET_LOCK` exactly as a separate process would.
///
/// Keyed by server identity + lock name so that a mixed-engine or
/// multi-database run targeting two MySQL servers with the same history-table
/// name keeps its locks distinct.
#[cfg(feature = "mysql")]
type MysqlLockRegistry = std::collections::HashMap<(usize, String), mysql_async::Conn>;

#[cfg(feature = "mysql")]
static MYSQL_LOCK_CONNS: std::sync::LazyLock<std::sync::Mutex<MysqlLockRegistry>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Identity of the pool a lock was taken on, for registry keying.
///
/// The address of the `Pool` inside its owning [`DbClient`] scopes the entry to
/// that specific client instance, which is what we want: two `DbClient`s
/// pointing at different MySQL servers must not share a registry slot even
/// when they use the same history-table name.
///
/// `acquire_lock` / `release_lock` are always called through the same
/// `&DbClient` borrow (acquire, do work, release), so the value provably
/// cannot move in between and the address is stable across the pair. If a
/// caller were to move the owning `DbClient` while holding a lock, the parked
/// connection would be orphaned and the lock would persist until the server
/// reaps the session — degraded, but never silently unlocked.
#[cfg(feature = "mysql")]
fn mysql_pool_ident(pool: &mysql_async::Pool) -> usize {
    pool as *const mysql_async::Pool as usize
}

/// Park the lock-holding connection in the registry.
#[cfg(feature = "mysql")]
fn park_lock_conn(pool: &mysql_async::Pool, key: &str, conn: mysql_async::Conn) {
    let registry_key = (mysql_pool_ident(pool), key.to_string());
    match MYSQL_LOCK_CONNS.lock() {
        Ok(mut guard) => {
            guard.insert(registry_key, conn);
        }
        Err(poisoned) => {
            // A panic elsewhere poisoned the registry. Recover rather than
            // propagate: losing the parked connection would leak the lock
            // until the server times the session out.
            poisoned.into_inner().insert(registry_key, conn);
        }
    }
}

/// Reclaim the lock-holding connection from the registry, if present.
#[cfg(feature = "mysql")]
fn unpark_lock_conn(pool: &mysql_async::Pool, key: &str) -> Option<mysql_async::Conn> {
    let registry_key = (mysql_pool_ident(pool), key.to_string());
    match MYSQL_LOCK_CONNS.lock() {
        Ok(mut guard) => guard.remove(&registry_key),
        Err(poisoned) => poisoned.into_inner().remove(&registry_key),
    }
}

// ── PostgreSQL-specific connection helpers (legacy entry points) ──────────────

/// Translate waypoint's [`SslMode`] into tokio-postgres's own three-value mode.
///
/// tokio-postgres has no concept of `verify-ca` / `verify-full` — it only
/// decides whether TLS is *attempted* or *demanded*. All three of our
/// mandatory modes therefore map to `Require`, and the strength of the
/// verification is expressed entirely in the rustls verifier built by
/// [`crate::tls::make_rustls_config`].
///
/// Setting this at all is what makes `require` actually require TLS: without
/// it tokio-postgres defaults to `Prefer` and silently accepts a server that
/// refuses SSL.
#[cfg(feature = "postgres")]
fn to_pg_ssl_mode(mode: SslMode) -> tokio_postgres::config::SslMode {
    match mode {
        SslMode::Disable => tokio_postgres::config::SslMode::Disable,
        SslMode::Prefer => tokio_postgres::config::SslMode::Prefer,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {
            tokio_postgres::config::SslMode::Require
        }
    }
}

/// Check if a postgres error is a permanent authentication failure that should not be retried.
#[cfg(feature = "postgres")]
fn is_permanent_error(e: &tokio_postgres::Error) -> bool {
    if let Some(db_err) = e.as_db_error() {
        let code = db_err.code().code();
        // 28P01 = invalid_password, 28000 = invalid_authorization_specification
        return code == "28P01" || code == "28000";
    }
    false
}

/// Inject TCP keepalive parameters into a connection string if not already present.
///
/// For URL-style strings (`postgres://...`), appends `?keepalives=1&keepalives_idle=N`
/// (or `&` if `?` already exists). For key=value style, appends ` keepalives=1 keepalives_idle=N`.
/// Returns the string unchanged if `keepalive_secs == 0` or keepalive params already exist.
pub fn inject_keepalive(conn_string: &str, keepalive_secs: u32) -> String {
    if keepalive_secs == 0 {
        return conn_string.to_string();
    }
    let lower = conn_string.to_lowercase();
    if lower.contains("keepalives") {
        return conn_string.to_string();
    }
    let params = format!("keepalives=1&keepalives_idle={}", keepalive_secs);
    if conn_string.starts_with("postgres://") || conn_string.starts_with("postgresql://") {
        if conn_string.contains('?') {
            format!("{}&{}", conn_string, params)
        } else {
            format!("{}?{}", conn_string, params)
        }
    } else {
        // Key=value style
        format!(
            "{} keepalives=1 keepalives_idle={}",
            conn_string, keepalive_secs
        )
    }
}

/// Spawn the background connection driver task.
///
/// Both TLS and non-TLS connections produce a future that resolves when the
/// connection terminates.  This helper accepts any such future and runs it
/// on the tokio runtime, logging errors.
#[cfg(feature = "postgres")]
fn spawn_connection_task<F>(connection: F)
where
    F: std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>
        + Send
        + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("Database connection error: {}", e);
        }
    });
}

/// Make one connection attempt against an already-prepared config.
///
/// `pg_config` carries the enforced `ssl_mode`, and `tls_config` is `None`
/// only for [`SslMode::Disable`]. Both are built once by the caller so a CA
/// file is not re-read on every retry.
///
/// There is deliberately no outer plaintext retry for `prefer`. Now that the
/// mode is pushed into `tokio_postgres::Config`, tokio-postgres performs the
/// `prefer` downgrade *in band* — it sends the SSLRequest, and on the server's
/// `N` reply continues on the same socket unencrypted. The old code instead
/// caught every error from the TLS attempt and opened a second connection,
/// which doubled the authentication attempts against the server (enough to
/// trip lockout policies) and reported "falling back to plaintext" for
/// failures that had nothing to do with TLS, such as a refused connection or a
/// bad password.
#[cfg(feature = "postgres")]
async fn connect_once(
    pg_config: &tokio_postgres::Config,
    tls_config: Option<&rustls::ClientConfig>,
    connect_timeout_secs: u32,
) -> std::result::Result<Client, tokio_postgres::Error> {
    let connect_fut = async {
        match tls_config {
            None => {
                let (client, connection) = pg_config.connect(tokio_postgres::NoTls).await?;
                spawn_connection_task(connection);
                Ok(client)
            }
            Some(tls_config) => {
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config.clone());
                let (client, connection) = pg_config.connect(tls).await?;
                spawn_connection_task(connection);
                Ok(client)
            }
        }
    };

    if connect_timeout_secs > 0 {
        match tokio::time::timeout(
            std::time::Duration::from_secs(connect_timeout_secs as u64),
            connect_fut,
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(tokio_postgres::Error::__private_api_timeout()),
        }
    } else {
        connect_fut.await
    }
}

/// Connect to the database using the provided connection string.
///
/// Spawns the connection task on the tokio runtime.
#[cfg(feature = "postgres")]
#[deprecated(
    since = "0.7.0",
    note = "Use connect_with_transport, which supports the full sslmode ladder and a custom CA. Will be removed in 1.0."
)]
pub async fn connect(conn_string: &str) -> Result<Client> {
    connect_with_transport(conn_string, &TransportConfig::default()).await
}

/// Connect to the database, retrying up to `retries` times with exponential backoff + jitter.
///
/// Each retry waits `min(2^attempt, 30) + rand(0..1000ms)` before the next attempt.
/// Permanent errors (authentication failures) are not retried.
#[cfg(feature = "postgres")]
#[deprecated(
    since = "0.7.0",
    note = "Use connect_with_transport, which supports the full sslmode ladder and a custom CA. Will be removed in 1.0."
)]
pub async fn connect_with_config(
    conn_string: &str,
    ssl_mode: &SslMode,
    retries: u32,
    connect_timeout_secs: u32,
    statement_timeout_secs: u32,
) -> Result<Client> {
    connect_with_transport(
        conn_string,
        &TransportConfig {
            ssl_mode: *ssl_mode,
            retries,
            connect_timeout_secs,
            statement_timeout_secs,
            ..TransportConfig::default()
        },
    )
    .await
}

/// Connect to the database with all configuration options including TCP keepalive.
#[cfg(feature = "postgres")]
#[deprecated(
    since = "0.7.0",
    note = "Use connect_with_transport — this signature cannot express ssl_root_cert. Will be removed in 1.0."
)]
pub async fn connect_with_full_config(
    conn_string: &str,
    ssl_mode: &SslMode,
    retries: u32,
    connect_timeout_secs: u32,
    statement_timeout_secs: u32,
    keepalive_secs: u32,
) -> Result<Client> {
    connect_with_transport(
        conn_string,
        &TransportConfig {
            ssl_mode: *ssl_mode,
            ssl_root_cert: None,
            retries,
            connect_timeout_secs,
            statement_timeout_secs,
            keepalive_secs,
        },
    )
    .await
}

/// Connect to PostgreSQL with retry, honouring the full TLS trust configuration.
///
/// Unlike the older helpers this actually *enforces* the requested
/// [`SslMode`]: the mode is pushed into `tokio_postgres::Config`, so a server
/// that refuses SSL is rejected under `require` and above rather than being
/// silently downgraded to plaintext.
#[cfg(feature = "postgres")]
pub async fn connect_with_transport(
    conn_string: &str,
    transport: &TransportConfig,
) -> Result<Client> {
    let conn_string = inject_keepalive(conn_string, transport.keepalive_secs);

    // Take libpq's `sslmode=` / `sslrootcert=` out of the string before
    // tokio-postgres sees it — its parser rejects `verify-ca` / `verify-full`
    // outright, and rejects `sslrootcert` as an unknown option.
    let (conn_string, embedded) = crate::tls::parse_url_sslmode(&conn_string);
    let ssl_mode = crate::tls::reconcile_ssl_mode(transport.ssl_mode, embedded.mode);
    let ssl_root_cert =
        crate::tls::reconcile_root_cert(transport.ssl_root_cert.as_deref(), embedded.root_cert);

    let mut pg_config: tokio_postgres::Config = conn_string.parse().map_err(|e| {
        WaypointError::ConfigError(format!("Invalid PostgreSQL connection string: {}", e))
    })?;
    pg_config.ssl_mode(to_pg_ssl_mode(ssl_mode));

    // Built once, outside the retry loop, so the CA file is read at most once
    // and a bad path fails immediately instead of after every backoff.
    let tls_config = match ssl_mode {
        SslMode::Disable => None,
        _ => Some(crate::tls::make_rustls_config(
            ssl_mode,
            ssl_root_cert.as_deref(),
        )?),
    };

    let retries = transport.retries;
    let mut last_err = None;

    for attempt in 0..=retries {
        if attempt > 0 {
            let base_delay = std::cmp::min(1u64 << attempt, 30);
            let jitter_ms = fastrand::u64(0..1000);
            let delay = std::time::Duration::from_secs(base_delay)
                + std::time::Duration::from_millis(jitter_ms);
            log::info!(
                "Connection attempt failed, retrying; attempt={}, max_attempts={}, delay_ms={}",
                attempt + 1,
                retries + 1,
                delay.as_millis() as u64
            );
            tokio::time::sleep(delay).await;
        }

        match connect_once(
            &pg_config,
            tls_config.as_ref(),
            transport.connect_timeout_secs,
        )
        .await
        {
            Ok(client) => {
                if attempt > 0 {
                    log::info!(
                        "Connected successfully after retry; attempt={}, max_attempts={}",
                        attempt + 1,
                        retries + 1
                    );
                }

                // Set statement timeout if configured
                if transport.statement_timeout_secs > 0 {
                    let timeout_sql = format!(
                        "SET statement_timeout = '{}s'",
                        transport.statement_timeout_secs
                    );
                    client.batch_execute(&timeout_sql).await?;
                }

                return Ok(client);
            }
            Err(e) => {
                // Don't retry permanent errors (e.g. bad credentials)
                if is_permanent_error(&e) {
                    log::error!("Permanent connection error, not retrying: {}", e);
                    return Err(WaypointError::DatabaseError(e));
                }
                last_err = Some(e);
            }
        }
    }

    Err(WaypointError::DatabaseError(last_err.unwrap()))
}

/// Acquire a PostgreSQL advisory lock based on the history table name.
///
/// This prevents concurrent migration runs from interfering with each other.
#[cfg(feature = "postgres")]
pub async fn acquire_advisory_lock(client: &Client, schema: &str, table_name: &str) -> Result<()> {
    let lock_id = advisory_lock_id(schema, table_name);
    log::info!(
        "Acquiring advisory lock; lock_id={}, table={}",
        lock_id,
        table_name
    );

    client
        .execute("SELECT pg_advisory_lock($1)", &[&lock_id])
        .await
        .map_err(|e| WaypointError::LockError(format!("Failed to acquire advisory lock: {}", e)))?;

    Ok(())
}

/// Try to acquire a PostgreSQL advisory lock with a timeout.
///
/// Uses `pg_try_advisory_lock()` in a polling loop with configurable timeout.
/// Returns Ok(()) if lock acquired, or a LockError if the timeout expires.
#[cfg(feature = "postgres")]
pub async fn acquire_advisory_lock_with_timeout(
    client: &Client,
    schema: &str,
    table_name: &str,
    timeout_secs: u32,
) -> Result<()> {
    let lock_id = advisory_lock_id(schema, table_name);
    log::info!(
        "Trying to acquire advisory lock with timeout; lock_id={}, table={}, timeout_secs={}",
        lock_id,
        table_name,
        timeout_secs
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs as u64);

    loop {
        let row = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_id])
            .await
            .map_err(|e| WaypointError::LockError(format!("Failed to try advisory lock: {}", e)))?;

        let acquired: bool = row.get(0);
        if acquired {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            return Err(WaypointError::LockError(format!(
                "Timed out waiting for advisory lock after {}s (table: {}). Another migration may be running.",
                timeout_secs, table_name
            )));
        }

        // Wait 500ms before retrying
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Release the PostgreSQL advisory lock.
#[cfg(feature = "postgres")]
pub async fn release_advisory_lock(client: &Client, schema: &str, table_name: &str) -> Result<()> {
    let lock_id = advisory_lock_id(schema, table_name);
    log::info!(
        "Releasing advisory lock; lock_id={}, table={}",
        lock_id,
        table_name
    );

    client
        .execute("SELECT pg_advisory_unlock($1)", &[&lock_id])
        .await
        .map_err(|e| WaypointError::LockError(format!("Failed to release advisory lock: {}", e)))?;

    Ok(())
}

/// Compute a stable i64 lock ID from the schema and table name using CRC32.
///
/// # Scoping
///
/// PostgreSQL advisory locks live in a per-*database* namespace, so two
/// databases never collide. Two **schemas** in the same database did: the key
/// used to be a hash of the table name alone, and every schema names its
/// history table the same thing. Schema-per-tenant and schema-per-service
/// layouts therefore serialised every migration in the database behind one
/// lock. It was never unsafe — over-locking cannot corrupt anything — but it
/// queued work that has nothing to do with each other.
///
/// Including the schema mirrors [`mysql_lock_key`], which has always been
/// scoped this way and whose test says why.
///
/// # Upgrading
///
/// **The key changed in 0.8.0.** A waypoint before 0.8.0 and a waypoint from
/// 0.8.0 onwards compute different ids for the same history table, so they do
/// **not** exclude each other. Finish rolling out the new version before
/// relying on the lock again — do not run migrations from a mixed fleet
/// against one database.
///
/// Uses CRC32 instead of DefaultHasher for cross-version stability —
/// DefaultHasher is not guaranteed to produce the same output across
/// Rust compiler versions.
pub fn advisory_lock_id(schema: &str, table_name: &str) -> i64 {
    // `\0` as the separator: it cannot appear in a PostgreSQL identifier, so
    // ("a", "b_c") and ("a_b", "c") cannot hash to the same key.
    let key = format!("{}\0{}", schema, table_name);
    crc32fast::hash(key.as_bytes()) as i64
}

/// Get the current database user.
#[cfg(feature = "postgres")]
pub async fn get_current_user(client: &Client) -> Result<String> {
    let row = client.query_one("SELECT current_user", &[]).await?;
    Ok(row.get::<_, String>(0))
}

/// Get the current database name.
#[cfg(feature = "postgres")]
pub async fn get_current_database(client: &Client) -> Result<String> {
    let row = client.query_one("SELECT current_database()", &[]).await?;
    Ok(row.get::<_, String>(0))
}

/// Execute a SQL string within a transaction using SQL-level BEGIN/COMMIT.
/// Returns the execution time in milliseconds.
#[cfg(feature = "postgres")]
pub async fn execute_in_transaction(client: &Client, sql: &str) -> Result<i32> {
    let start = std::time::Instant::now();

    client.batch_execute("BEGIN").await?;

    match client.batch_execute(sql).await {
        Ok(()) => {
            client.batch_execute("COMMIT").await?;
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                log::warn!("Failed to rollback transaction: {}", rollback_err);
            }
            return Err(WaypointError::DatabaseError(e));
        }
    }

    let elapsed = start.elapsed().as_millis() as i32;
    Ok(elapsed)
}

/// Execute SQL without a transaction wrapper (for statements that can't run in a transaction).
#[cfg(feature = "postgres")]
pub async fn execute_raw(client: &Client, sql: &str) -> Result<i32> {
    let start = std::time::Instant::now();
    client.batch_execute(sql).await?;
    let elapsed = start.elapsed().as_millis() as i32;
    Ok(elapsed)
}

/// Check if an error is a transient connection error that may be retried.
///
/// Detects PostgreSQL server shutdown codes, connection exception codes,
/// closed connections, and common network error message patterns.
pub fn is_transient_error(e: &WaypointError) -> bool {
    match e {
        #[cfg(feature = "postgres")]
        WaypointError::DatabaseError(pg_err) => {
            // Check if the connection is closed
            if pg_err.is_closed() {
                return true;
            }
            // Check PostgreSQL error codes
            if let Some(db_err) = pg_err.as_db_error() {
                let code = db_err.code().code();
                // 57P01 = admin_shutdown, 57P02 = crash_shutdown, 57P03 = cannot_connect_now
                // 08000 = connection_exception, 08003 = connection_does_not_exist,
                // 08006 = connection_failure
                return matches!(
                    code,
                    "57P01" | "57P02" | "57P03" | "08000" | "08003" | "08006"
                );
            }
            // Check error message patterns for connection-related issues
            let msg = pg_err.to_string().to_lowercase();
            msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection closed")
                || msg.contains("unexpected eof")
        }
        #[cfg(feature = "mysql")]
        WaypointError::MysqlError(my_err) => {
            // mysql_async surfaces server-shutdown / connection-reset as IO or
            // driver errors. Do a coarse string match for now; we'll refine when
            // we wire production retry logic for MySQL in Phase 1.
            let msg = my_err.to_string().to_lowercase();
            msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("connection closed")
                || msg.contains("server has gone away")
                || msg.contains("lost connection")
                || msg.contains("io error")
        }
        WaypointError::ConnectionLost { .. } => true,
        _ => false,
    }
}

/// Verify the database connection is still alive with a minimal round-trip.
#[cfg(feature = "postgres")]
pub async fn check_connection(client: &Client) -> Result<()> {
    client
        .simple_query("")
        .await
        .map_err(|e| WaypointError::ConnectionLost {
            operation: "health check".to_string(),
            detail: e.to_string(),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── inject_keepalive tests ──

    #[test]
    fn test_inject_keepalive_url_style() {
        let result = inject_keepalive("postgres://user:pass@localhost/db", 120);
        assert_eq!(
            result,
            "postgres://user:pass@localhost/db?keepalives=1&keepalives_idle=120"
        );
    }

    #[test]
    fn test_inject_keepalive_url_with_existing_params() {
        let result = inject_keepalive("postgres://user:pass@localhost/db?sslmode=require", 60);
        assert_eq!(
            result,
            "postgres://user:pass@localhost/db?sslmode=require&keepalives=1&keepalives_idle=60"
        );
    }

    #[test]
    fn test_inject_keepalive_kv_style() {
        let result = inject_keepalive("host=localhost port=5432 user=admin dbname=mydb", 90);
        assert_eq!(
            result,
            "host=localhost port=5432 user=admin dbname=mydb keepalives=1 keepalives_idle=90"
        );
    }

    #[test]
    fn test_inject_keepalive_zero_disables() {
        let result = inject_keepalive("postgres://user:pass@localhost/db", 0);
        assert_eq!(result, "postgres://user:pass@localhost/db");
    }

    #[test]
    fn test_inject_keepalive_already_present() {
        let result = inject_keepalive("postgres://user:pass@localhost/db?keepalives=1", 120);
        assert_eq!(result, "postgres://user:pass@localhost/db?keepalives=1");
    }

    // ── is_transient_error tests ──

    #[test]
    fn test_transient_error_connection_lost() {
        let err = WaypointError::ConnectionLost {
            operation: "test".to_string(),
            detail: "gone".to_string(),
        };
        assert!(is_transient_error(&err));
    }

    #[test]
    fn test_transient_error_config_is_not_transient() {
        let err = WaypointError::ConfigError("bad config".to_string());
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn test_transient_error_migration_failed_is_not_transient() {
        let err = WaypointError::MigrationFailed {
            script: "V1__test.sql".to_string(),
            reason: "syntax error".to_string(),
        };
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn test_advisory_lock_id_stability() {
        // The same schema+table always produces the same lock ID; a
        // release_lock would otherwise not match its acquire_lock.
        let id1 = advisory_lock_id("public", "waypoint_schema_history");
        let id2 = advisory_lock_id("public", "waypoint_schema_history");
        assert_eq!(id1, id2);
        // Different table names produce different lock IDs.
        let id3 = advisory_lock_id("public", "other_table");
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_advisory_lock_id_is_scoped_per_schema() {
        // PostgreSQL advisory locks are per-database, so two *schemas* in one
        // database used to share a key — every tenant's migration queued
        // behind every other tenant's for no reason.
        let a = advisory_lock_id("tenant_a", "waypoint_schema_history");
        let b = advisory_lock_id("tenant_b", "waypoint_schema_history");
        assert_ne!(a, b, "schemas in one database must not share a lock");
    }

    #[test]
    fn test_advisory_lock_id_separator_cannot_be_forged() {
        // A plain concatenation would make ("a", "b_c") and ("a_b", "c")
        // collide. The NUL separator cannot occur in an identifier.
        assert_ne!(
            advisory_lock_id("a", "b_c"),
            advisory_lock_id("a_b", "c"),
            "schema/table boundary must be unambiguous"
        );
    }

    #[test]
    fn test_transient_error_lock_error_is_not_transient() {
        let err = WaypointError::LockError("lock failed".to_string());
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn test_transient_error_io_error_is_not_transient() {
        let err = WaypointError::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("users").is_ok());
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("Table123").is_ok());
        assert!(validate_identifier("a").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("my-table").is_err());
        assert!(validate_identifier("my table").is_err());
        assert!(validate_identifier("table.name").is_err());
        assert!(validate_identifier("table;drop").is_err());
    }

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn test_quote_ident_embedded_quotes() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn test_inject_keepalive_postgresql_prefix() {
        let result = inject_keepalive("postgresql://user:pass@localhost/db", 120);
        assert_eq!(
            result,
            "postgresql://user:pass@localhost/db?keepalives=1&keepalives_idle=120"
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_lock_key_is_scoped_per_database() {
        // GET_LOCK names are server-global, so the same history table in two
        // databases must not collide — otherwise migrating one database blocks
        // migrating the other.
        let a = mysql_lock_key("app_prod", "waypoint_schema_history");
        let b = mysql_lock_key("app_staging", "waypoint_schema_history");
        assert_ne!(a, b);
        assert_eq!(a, "waypoint_app_prod_waypoint_schema_history");
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_lock_key_respects_the_64_char_limit() {
        let long_db = "d".repeat(60);
        let long_tbl = "t".repeat(60);
        let k = mysql_lock_key(&long_db, &long_tbl);
        assert!(
            k.len() <= 64,
            "GET_LOCK names are capped at 64: {}",
            k.len()
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_lock_key_does_not_collide_after_shortening() {
        // Two distinct over-long names must not fold onto the same key —
        // plain truncation would have made these identical.
        let prefix = "x".repeat(60);
        let a = mysql_lock_key(&prefix, "alpha");
        let b = mysql_lock_key(&prefix, "beta");
        assert!(a.len() <= 64 && b.len() <= 64);
        assert_ne!(a, b, "distinct tables collapsed onto one lock key");
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_lock_key_is_stable() {
        // The key has to be reproducible across processes and releases, or a
        // release_lock would not match its acquire_lock.
        assert_eq!(mysql_lock_key("db", "tbl"), mysql_lock_key("db", "tbl"));
    }

    #[test]
    fn test_sandbox_name_is_unique_across_rapid_calls() {
        // `simulate` and `drift` drop their sandbox unconditionally, so two
        // runs that pick the same name destroy each other's work. The names
        // used to be a bare clock reading — milliseconds for simulate, whole
        // seconds for drift — and a loop this tight produced duplicates.
        let names: std::collections::HashSet<String> =
            (0..2000).map(|_| sandbox_name("waypoint_sim")).collect();
        assert_eq!(
            names.len(),
            2000,
            "sandbox names collided within a single tight loop"
        );
    }

    #[test]
    fn test_sandbox_name_fits_identifier_limits() {
        // PostgreSQL truncates identifiers at 63 bytes and MySQL rejects
        // database names over 64. A truncated name would reintroduce exactly
        // the collision this helper exists to prevent.
        for prefix in ["waypoint_sim", "waypoint_drift_check"] {
            let name = sandbox_name(prefix);
            assert!(
                name.len() <= 63,
                "{} is {} bytes, over PostgreSQL's 63-byte limit",
                name,
                name.len()
            );
            assert!(name.starts_with(prefix));
        }
    }

    #[test]
    fn test_quote_literal_escapes_embedded_single_quotes() {
        // Generated enum DDL used to interpolate labels raw, producing
        // `ENUM ('fine', 'it's bad')` — a snapshot that will not restore.
        assert_eq!(quote_literal("fine"), "'fine'");
        assert_eq!(quote_literal("it's bad"), "'it''s bad'");
        // Two quotes in: each is doubled, then the delimiters are added.
        assert_eq!(quote_literal("''"), r"''''''");
        assert_eq!(quote_literal(""), r"''");
    }

    #[test]
    fn test_quote_literal_leaves_other_characters_alone() {
        // Backslashes are not escapes in a standard-conforming string literal,
        // so doubling them would corrupt the value.
        assert_eq!(quote_literal(r"back\slash"), r"'back\slash'");
        assert_eq!(quote_literal("multi\nline"), "'multi\nline'");
    }
}
