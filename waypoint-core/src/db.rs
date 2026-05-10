//! Database connection, TLS support, advisory locking, and transaction execution.
//!
//! The functions in this module that take `&tokio_postgres::Client` are gated
//! behind the `postgres` feature and are the original PostgreSQL-only entry points.
//! New code paths should use [`DbClient`] which abstracts over the configured
//! backend (PostgreSQL or MySQL).

use crate::dialect::{dialect_for, DatabaseDialect, DialectKind};
use crate::error::{Result, WaypointError};

#[cfg(feature = "postgres")]
use fastrand;

#[cfg(feature = "postgres")]
use tokio_postgres::Client;

#[cfg(feature = "postgres")]
use crate::config::SslMode;

/// Quote a SQL identifier to prevent SQL injection.
///
/// Doubles any embedded double-quotes and wraps in double-quotes — this is the
/// PostgreSQL convention. For MySQL identifier quoting use the dialect's
/// [`DatabaseDialect::quote_ident`].
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
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

    /// Construct the dialect helper for this connection.
    pub fn dialect(&self) -> Box<dyn DatabaseDialect> {
        // Both features are conditionally compiled, so this can't fail in practice
        // when the corresponding feature is enabled.
        dialect_for(self.dialect_kind()).expect("dialect for active connection feature")
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
    pub async fn acquire_lock(&self, table_name: &str) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => acquire_advisory_lock(c, table_name).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let key = mysql_lock_key(table_name);
                let mut conn = pool.get_conn().await?;
                let acquired: Option<i64> = conn
                    .exec_first("SELECT GET_LOCK(?, -1)", (key.clone(),))
                    .await?;
                match acquired {
                    Some(1) => Ok(()),
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
        table_name: &str,
        timeout_secs: u32,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => {
                acquire_advisory_lock_with_timeout(c, table_name, timeout_secs).await
            }
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let key = mysql_lock_key(table_name);
                let mut conn = pool.get_conn().await?;
                let acquired: Option<i64> = conn
                    .exec_first("SELECT GET_LOCK(?, ?)", (key.clone(), timeout_secs as i64))
                    .await?;
                match acquired {
                    Some(1) => Ok(()),
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
    pub async fn release_lock(&self, table_name: &str) -> Result<()> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => release_advisory_lock(c, table_name).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let key = mysql_lock_key(table_name);
                let mut conn = pool.get_conn().await?;
                conn.exec_drop("SELECT RELEASE_LOCK(?)", (key,)).await?;
                Ok(())
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
    /// the batch into individual statements (mysql_async's underlying protocol
    /// doesn't accept multiple statements unless the connection is built with
    /// `CLIENT_MULTI_STATEMENTS`, which we deliberately avoid). Returns elapsed
    /// time in milliseconds.
    pub async fn execute_raw(&self, sql: &str) -> Result<i32> {
        match self {
            #[cfg(feature = "postgres")]
            DbClient::Postgres(c) => execute_raw(c, sql).await,
            #[cfg(feature = "mysql")]
            DbClient::Mysql(pool) => {
                use mysql_async::prelude::*;
                let start = std::time::Instant::now();
                let mut conn = pool.get_conn().await?;
                for stmt in split_simple_statements(sql) {
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

/// Split a `;`-delimited SQL batch into individual statements for engines that
/// don't accept multi-statement batches over the wire (currently MySQL).
///
/// This is a deliberately simple splitter — it does NOT handle dollar-quoting
/// (PG-only), `DELIMITER //` blocks, or string-literal `;` characters with full
/// fidelity. For Phase 1 it covers the history-table DDL we generate; richer
/// MySQL splitting (DELIMITER awareness for stored procedures) lives in
/// [`crate::sql_parser`] and will be plumbed in when migrate is ported.
#[cfg(feature = "mysql")]
fn split_simple_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    for c in sql.chars() {
        match c {
            '\'' if !in_double && !in_backtick => {
                in_single = !in_single;
                buf.push(c);
            }
            '"' if !in_single && !in_backtick => {
                in_double = !in_double;
                buf.push(c);
            }
            '`' if !in_single && !in_double => {
                in_backtick = !in_backtick;
                buf.push(c);
            }
            ';' if !in_single && !in_double && !in_backtick => {
                let stmt = buf.trim();
                if !stmt.is_empty() {
                    out.push(stmt.to_string());
                }
                buf.clear();
            }
            _ => buf.push(c),
        }
    }
    let tail = buf.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

/// Compute the MySQL named-lock key for a given history table name.
///
/// MySQL `GET_LOCK` keys are arbitrary strings (truncated to 64 chars in 8.0+).
/// We prefix `waypoint_` to avoid clashes with application locks and keep the
/// key stable across versions.
#[cfg(feature = "mysql")]
fn mysql_lock_key(table_name: &str) -> String {
    let mut k = format!("waypoint_{}", table_name);
    if k.len() > 64 {
        k.truncate(64);
    }
    k
}

// ── PostgreSQL-specific connection helpers (legacy entry points) ──────────────

/// Build a rustls ClientConfig using the Mozilla CA bundle and ring crypto provider.
#[cfg(feature = "postgres")]
fn make_rustls_config() -> rustls::ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth()
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

/// Connect to the database using the provided connection string with TLS support.
///
/// Spawns the connection task on the tokio runtime.
#[cfg(feature = "postgres")]
async fn connect_once(
    conn_string: &str,
    ssl_mode: &SslMode,
    connect_timeout_secs: u32,
) -> std::result::Result<Client, tokio_postgres::Error> {
    let connect_fut = async {
        match ssl_mode {
            SslMode::Disable => {
                let (client, connection) =
                    tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await?;
                spawn_connection_task(connection);
                Ok(client)
            }
            SslMode::Require => {
                let tls_config = make_rustls_config();
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                let (client, connection) = tokio_postgres::connect(conn_string, tls).await?;
                spawn_connection_task(connection);
                Ok(client)
            }
            SslMode::Prefer => {
                // Try TLS first, fall back to plaintext
                let tls_config = make_rustls_config();
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                match tokio_postgres::connect(conn_string, tls).await {
                    Ok((client, connection)) => {
                        spawn_connection_task(connection);
                        Ok(client)
                    }
                    Err(_) => {
                        log::debug!("TLS connection failed, falling back to plaintext");
                        let (client, connection) =
                            tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await?;
                        spawn_connection_task(connection);
                        Ok(client)
                    }
                }
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
pub async fn connect(conn_string: &str) -> Result<Client> {
    connect_with_config(conn_string, &SslMode::Prefer, 0, 30, 0).await
}

/// Connect to the database, retrying up to `retries` times with exponential backoff + jitter.
///
/// Each retry waits `min(2^attempt, 30) + rand(0..1000ms)` before the next attempt.
/// Permanent errors (authentication failures) are not retried.
#[cfg(feature = "postgres")]
pub async fn connect_with_config(
    conn_string: &str,
    ssl_mode: &SslMode,
    retries: u32,
    connect_timeout_secs: u32,
    statement_timeout_secs: u32,
) -> Result<Client> {
    connect_with_full_config(
        conn_string,
        ssl_mode,
        retries,
        connect_timeout_secs,
        statement_timeout_secs,
        120,
    )
    .await
}

/// Connect to the database with all configuration options including TCP keepalive.
#[cfg(feature = "postgres")]
pub async fn connect_with_full_config(
    conn_string: &str,
    ssl_mode: &SslMode,
    retries: u32,
    connect_timeout_secs: u32,
    statement_timeout_secs: u32,
    keepalive_secs: u32,
) -> Result<Client> {
    let conn_string = inject_keepalive(conn_string, keepalive_secs);
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

        match connect_once(&conn_string, ssl_mode, connect_timeout_secs).await {
            Ok(client) => {
                if attempt > 0 {
                    log::info!(
                        "Connected successfully after retry; attempt={}, max_attempts={}",
                        attempt + 1,
                        retries + 1
                    );
                }

                // Set statement timeout if configured
                if statement_timeout_secs > 0 {
                    let timeout_sql =
                        format!("SET statement_timeout = '{}s'", statement_timeout_secs);
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
pub async fn acquire_advisory_lock(client: &Client, table_name: &str) -> Result<()> {
    let lock_id = advisory_lock_id(table_name);
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
    table_name: &str,
    timeout_secs: u32,
) -> Result<()> {
    let lock_id = advisory_lock_id(table_name);
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
pub async fn release_advisory_lock(client: &Client, table_name: &str) -> Result<()> {
    let lock_id = advisory_lock_id(table_name);
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

/// Compute a stable i64 lock ID from the table name using CRC32.
///
/// Uses CRC32 instead of DefaultHasher for cross-version stability —
/// DefaultHasher is not guaranteed to produce the same output across
/// Rust compiler versions.
pub fn advisory_lock_id(table_name: &str) -> i64 {
    crc32fast::hash(table_name.as_bytes()) as i64
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
        // Ensure the same table name always produces the same lock ID
        let id1 = advisory_lock_id("waypoint_schema_history");
        let id2 = advisory_lock_id("waypoint_schema_history");
        assert_eq!(id1, id2);
        // Different table names should produce different lock IDs
        let id3 = advisory_lock_id("other_table");
        assert_ne!(id1, id3);
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
}
