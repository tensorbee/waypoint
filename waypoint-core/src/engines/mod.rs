//! Per-engine implementations.
//!
//! Engine-specific code lives here, organized by engine. Each submodule
//! mirrors the top-level layout (e.g. `engines::postgres::history` parallels
//! the dispatcher in `crate::history`). The top-level files (e.g.
//! `history.rs`, `commands/migrate.rs`) keep the shared types and the
//! dialect-aware dispatchers; the per-engine submodules hold the actual
//! database-touching logic.
//!
//! Cargo features gate which submodules exist:
//! - `postgres` (default) → `engines::postgres` is present
//! - `mysql` (opt-in) → `engines::mysql` is present

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;
