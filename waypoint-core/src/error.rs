//! Error types for Waypoint operations.

use thiserror::Error;

/// Extract the full error message from a tokio_postgres::Error,
/// including the underlying DbError details that Display hides.
#[cfg(feature = "postgres")]
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
    // Append connection-loss context when the connection is closed
    if e.is_closed() {
        msg.push_str("\n  Note: The database connection was closed unexpectedly. This may indicate a network issue or server restart.");
    }
    msg
}

/// All error types that Waypoint operations can produce.
#[derive(Error, Debug)]
pub enum WaypointError {
    /// Invalid or missing configuration (TOML parse errors, missing required fields, etc.).
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// A database query or connection operation failed (PostgreSQL).
    #[cfg(feature = "postgres")]
    #[error("Database error: {}", format_db_error(.0))]
    DatabaseError(#[from] tokio_postgres::Error),

    /// A database query or connection operation failed (MySQL).
    #[cfg(feature = "mysql")]
    #[error("Database error: {0}")]
    MysqlError(#[from] mysql_async::Error),

    /// A migration filename could not be parsed into a valid migration.
    #[error("Migration parse error: {0}")]
    MigrationParseError(String),

    /// The on-disk checksum of a migration does not match the recorded checksum.
    #[error("Checksum mismatch for migration {script}: expected {expected}, found {found}")]
    ChecksumMismatch {
        script: String,
        expected: i32,
        found: i32,
    },

    /// One or more validation checks failed before migration could proceed.
    #[error("Validation failed:\n{0}")]
    ValidationFailed(String),

    /// A migration script failed to execute against the database.
    #[error("Migration failed for {script}: {reason}")]
    MigrationFailed { script: String, reason: String },

    /// Could not acquire the PostgreSQL advisory lock used to prevent concurrent migrations.
    #[error("Failed to acquire advisory lock: {0}")]
    LockError(String),

    /// The `clean` command was invoked but clean is not enabled in the configuration.
    #[error(
        "Clean is disabled. Pass --allow-clean to enable it or set clean_enabled = true in config."
    )]
    CleanDisabled,

    /// A baseline was requested but the schema history table already contains entries.
    #[error("Baseline already exists. The schema history table is not empty.")]
    BaselineExists,

    /// A filesystem I/O operation failed (reading migration files, config, etc.).
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// A migration version is lower than the highest applied version and out-of-order is disabled.
    #[error("Out-of-order migration not allowed: version {version} is below the highest applied version {highest}. Enable out_of_order to allow this.")]
    OutOfOrder { version: String, highest: String },

    /// A `${key}` placeholder in migration SQL has no corresponding value defined.
    #[error("Placeholder '{key}' not found. Available placeholders: {available}")]
    PlaceholderNotFound { key: String, available: String },

    /// A SQL callback hook script failed during execution.
    #[error("Hook failed during {phase} ({script}): {reason}")]
    HookFailed {
        phase: String,
        script: String,
        reason: String,
    },

    /// The self-update mechanism encountered an error.
    #[error("Self-update failed: {0}")]
    UpdateError(String),

    /// An undo migration script failed to execute against the database.
    #[error("Undo failed for {script}: {reason}")]
    UndoFailed { script: String, reason: String },

    /// No undo migration file was found for the requested version.
    #[error("No undo migration found for version {version}. Expected U{version}__*.sql file.")]
    UndoMissing { version: String },

    /// Lint analysis found one or more errors in migration SQL.
    #[error("Lint found {error_count} error(s): {details}")]
    LintFailed { error_count: usize, details: String },

    /// A schema diff operation failed.
    #[error("Diff failed: {reason}")]
    DiffFailed { reason: String },

    /// The live database schema differs from the expected snapshot.
    #[error("Schema drift detected: {count} difference(s): {details}")]
    DriftDetected { count: usize, details: String },

    /// A schema snapshot operation (save, load, or compare) failed.
    #[error("Snapshot error: {reason}")]
    SnapshotError { reason: String },

    /// A circular dependency was detected among migration `@depends` directives.
    #[error("Migration dependency cycle detected: {path}")]
    DependencyCycle { path: String },

    /// A migration declares a dependency on a version that does not exist on disk.
    #[error("Migration V{version} depends on V{dependency}, which does not exist")]
    MissingDependency { version: String, dependency: String },

    /// A migration directive comment is malformed or contains invalid values.
    #[error("Invalid directive in {script}: {reason}")]
    InvalidDirective { script: String, reason: String },

    /// A Git operation required for branch conflict detection failed.
    #[error("Git error: {0}")]
    GitError(String),

    /// Multiple branches introduced conflicting migration versions.
    #[error("Migration conflicts detected: {count} conflict(s): {details}")]
    ConflictsDetected { count: usize, details: String },

    /// A named database referenced in multi-database config was not found.
    #[error("Database '{name}' not found. Available: {available}")]
    DatabaseNotFound { name: String, available: String },

    /// A circular dependency was detected among multi-database `depends_on` declarations.
    #[error("Multi-database dependency cycle: {path}")]
    MultiDbDependencyCycle { path: String },

    /// A multi-database migration operation failed for a specific named database.
    #[error("Multi-database error for '{name}': {reason}")]
    MultiDbError { name: String, reason: String },

    /// One or more pre-flight safety checks failed before migration could proceed.
    #[error("Pre-flight checks failed: {checks}")]
    PreflightFailed { checks: String },

    /// A guard precondition or postcondition check failed.
    #[error("Guard {kind} failed for {script}: {expression}")]
    GuardFailed {
        kind: String,
        script: String,
        expression: String,
    },

    /// A migration was blocked by a DANGER safety verdict.
    #[error("Migration blocked for {script}: {reason}. Use --force to override.")]
    MigrationBlocked { script: String, reason: String },

    /// A schema advisor analysis encountered an error.
    #[error("Advisor error: {0}")]
    AdvisorError(String),

    /// A migration simulation failed.
    #[error("Simulation failed: {reason}")]
    SimulationFailed { reason: String },

    /// A migration contains statements that cannot run inside a transaction (e.g. CONCURRENTLY).
    #[error("Migration {script} contains non-transactional statement: {statement}. Remove --transaction or rewrite the migration.")]
    NonTransactionalStatement { script: String, statement: String },

    /// The database connection was lost during an operation.
    #[error("Connection lost during {operation}: {detail}")]
    ConnectionLost { operation: String, detail: String },
}

/// Convenience type alias for `Result<T, WaypointError>`.
pub type Result<T> = std::result::Result<T, WaypointError>;
