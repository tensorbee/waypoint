use thiserror::Error;

/// Extract the full error message from a tokio_postgres::Error,
/// including the underlying DbError details that Display hides.
pub fn format_db_error(e: &tokio_postgres::Error) -> String {
    // The source chain contains the actual DbError with message/detail/hint
    if let Some(db_err) = e.as_db_error() {
        let mut msg = db_err.message().to_string();
        if let Some(detail) = db_err.detail() {
            msg.push_str(&format!("\n  Detail: {}", detail));
        }
        if let Some(hint) = db_err.hint() {
            msg.push_str(&format!("\n  Hint: {}", hint));
        }
        if let Some(position) = db_err.position() {
            msg.push_str(&format!("\n  Position: {:?}", position));
        }
        return msg;
    }
    // Fallback: walk the source chain
    let mut msg = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        msg.push_str(&format!(": {}", s));
        source = s.source();
    }
    msg
}

#[derive(Error, Debug)]
pub enum WaypointError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Database error: {}", format_db_error(.0))]
    DatabaseError(#[from] tokio_postgres::Error),

    #[error("Migration parse error: {0}")]
    MigrationParseError(String),

    #[error("Checksum mismatch for migration {script}: expected {expected}, found {found}")]
    ChecksumMismatch {
        script: String,
        expected: i32,
        found: i32,
    },

    #[error("Validation failed:\n{0}")]
    ValidationFailed(String),

    #[error("Migration failed for {script}: {reason}")]
    MigrationFailed { script: String, reason: String },

    #[error("Failed to acquire advisory lock: {0}")]
    LockError(String),

    #[error("Clean is disabled. Pass --allow-clean to enable it or set clean_enabled = true in config.")]
    CleanDisabled,

    #[error("Baseline already exists. The schema history table is not empty.")]
    BaselineExists,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Out-of-order migration not allowed: version {version} is below the highest applied version {highest}. Enable out_of_order to allow this.")]
    OutOfOrder { version: String, highest: String },

    #[error("Placeholder '{key}' not found. Available placeholders: {available}")]
    PlaceholderNotFound { key: String, available: String },

    #[error("Hook failed during {phase} ({script}): {reason}")]
    HookFailed {
        phase: String,
        script: String,
        reason: String,
    },
}

pub type Result<T> = std::result::Result<T, WaypointError>;
