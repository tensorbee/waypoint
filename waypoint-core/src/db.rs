//! Database connection, TLS support, advisory locking, and transaction execution.

use rand::Rng;
use tokio_postgres::Client;

use crate::config::SslMode;
use crate::error::{Result, WaypointError};

/// Quote a SQL identifier to prevent SQL injection.
///
/// Doubles any embedded double-quotes and wraps in double-quotes.
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

/// Build a rustls ClientConfig using the Mozilla CA bundle.
fn make_rustls_config() -> rustls::ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Check if a postgres error is a permanent authentication failure that should not be retried.
fn is_permanent_error(e: &tokio_postgres::Error) -> bool {
    if let Some(db_err) = e.as_db_error() {
        let code = db_err.code().code();
        // 28P01 = invalid_password, 28000 = invalid_authorization_specification
        return code == "28P01" || code == "28000";
    }
    false
}

/// Connect to the database using the provided connection string with TLS support.
///
/// Spawns the connection task on the tokio runtime.
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
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        tracing::error!(error = %e, "Database connection error");
                    }
                });
                Ok(client)
            }
            SslMode::Require => {
                let tls_config = make_rustls_config();
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                let (client, connection) = tokio_postgres::connect(conn_string, tls).await?;
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        tracing::error!(error = %e, "Database connection error");
                    }
                });
                Ok(client)
            }
            SslMode::Prefer => {
                // Try TLS first, fall back to plaintext
                let tls_config = make_rustls_config();
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                match tokio_postgres::connect(conn_string, tls).await {
                    Ok((client, connection)) => {
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!(error = %e, "Database connection error");
                            }
                        });
                        Ok(client)
                    }
                    Err(_) => {
                        tracing::debug!("TLS connection failed, falling back to plaintext");
                        let (client, connection) =
                            tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await?;
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!(error = %e, "Database connection error");
                            }
                        });
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
pub async fn connect(conn_string: &str) -> Result<Client> {
    connect_with_config(conn_string, &SslMode::Prefer, 0, 30, 0).await
}

/// Connect to the database, retrying up to `retries` times with exponential backoff + jitter.
///
/// Each retry waits `min(2^attempt, 30) + rand(0..1000ms)` before the next attempt.
/// Permanent errors (authentication failures) are not retried.
pub async fn connect_with_config(
    conn_string: &str,
    ssl_mode: &SslMode,
    retries: u32,
    connect_timeout_secs: u32,
    statement_timeout_secs: u32,
) -> Result<Client> {
    let mut last_err = None;

    for attempt in 0..=retries {
        if attempt > 0 {
            let base_delay = std::cmp::min(1u64 << attempt, 30);
            let jitter_ms = rand::thread_rng().gen_range(0..1000);
            let delay = std::time::Duration::from_secs(base_delay)
                + std::time::Duration::from_millis(jitter_ms);
            tracing::info!(
                attempt = attempt + 1,
                max_attempts = retries + 1,
                delay_ms = delay.as_millis() as u64,
                "Connection attempt failed, retrying"
            );
            tokio::time::sleep(delay).await;
        }

        match connect_once(conn_string, ssl_mode, connect_timeout_secs).await {
            Ok(client) => {
                if attempt > 0 {
                    tracing::info!(
                        attempt = attempt + 1,
                        max_attempts = retries + 1,
                        "Connected successfully after retry"
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
                    tracing::error!(error = %e, "Permanent connection error, not retrying");
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
pub async fn acquire_advisory_lock(client: &Client, table_name: &str) -> Result<()> {
    let lock_id = advisory_lock_id(table_name);
    tracing::info!(lock_id = lock_id, table = %table_name, "Acquiring advisory lock");

    client
        .execute(&format!("SELECT pg_advisory_lock({})", lock_id), &[])
        .await
        .map_err(|e| WaypointError::LockError(format!("Failed to acquire advisory lock: {}", e)))?;

    Ok(())
}

/// Release the PostgreSQL advisory lock.
pub async fn release_advisory_lock(client: &Client, table_name: &str) -> Result<()> {
    let lock_id = advisory_lock_id(table_name);
    tracing::info!(lock_id = lock_id, table = %table_name, "Releasing advisory lock");

    client
        .execute(&format!("SELECT pg_advisory_unlock({})", lock_id), &[])
        .await
        .map_err(|e| WaypointError::LockError(format!("Failed to release advisory lock: {}", e)))?;

    Ok(())
}

/// Compute a stable i64 lock ID from the table name using CRC32.
///
/// Uses CRC32 instead of DefaultHasher for cross-version stability —
/// DefaultHasher is not guaranteed to produce the same output across
/// Rust compiler versions.
fn advisory_lock_id(table_name: &str) -> i64 {
    crc32fast::hash(table_name.as_bytes()) as i64
}

/// Get the current database user.
pub async fn get_current_user(client: &Client) -> Result<String> {
    let row = client.query_one("SELECT current_user", &[]).await?;
    Ok(row.get::<_, String>(0))
}

/// Get the current database name.
pub async fn get_current_database(client: &Client) -> Result<String> {
    let row = client.query_one("SELECT current_database()", &[]).await?;
    Ok(row.get::<_, String>(0))
}

/// Execute a SQL string within a transaction using SQL-level BEGIN/COMMIT.
/// Returns the execution time in milliseconds.
pub async fn execute_in_transaction(client: &Client, sql: &str) -> Result<i32> {
    let start = std::time::Instant::now();

    client.batch_execute("BEGIN").await?;

    match client.batch_execute(sql).await {
        Ok(()) => {
            client.batch_execute("COMMIT").await?;
        }
        Err(e) => {
            if let Err(rollback_err) = client.batch_execute("ROLLBACK").await {
                tracing::warn!(error = %rollback_err, "Failed to rollback transaction");
            }
            return Err(WaypointError::DatabaseError(e));
        }
    }

    let elapsed = start.elapsed().as_millis() as i32;
    Ok(elapsed)
}

/// Execute SQL without a transaction wrapper (for statements that can't run in a transaction).
pub async fn execute_raw(client: &Client, sql: &str) -> Result<i32> {
    let start = std::time::Instant::now();
    client.batch_execute(sql).await?;
    let elapsed = start.elapsed().as_millis() as i32;
    Ok(elapsed)
}
