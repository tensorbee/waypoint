# Waypoint

Lightweight, Flyway-inspired SQL migration tool built in Rust.

## Build & Test

```bash
cargo build          # Build both crates
cargo test           # Run all unit tests (no DB required)
cargo run -- --help  # Show CLI help
```

## Architecture

Cargo workspace with two crates:

- **waypoint-core** — Library crate with all migration logic. Public API is the `Waypoint` struct in `lib.rs`.
- **waypoint-cli** — Binary crate (`waypoint`) with clap-based CLI.

### Key modules (waypoint-core/src/)

- `config.rs` — Config loading: TOML + env vars + CLI overrides
- `migration.rs` — Migration types, filename parsing, file scanning
- `checksum.rs` — CRC32 checksum (line-by-line, Flyway-compatible)
- `placeholder.rs` — `${key}` placeholder replacement in SQL
- `history.rs` — Schema history table CRUD operations
- `db.rs` — Database connection, advisory locks, transaction execution
- `error.rs` — `WaypointError` enum with `thiserror`
- `commands/` — One module per command: migrate, info, validate, repair, baseline, clean

### Migration file naming

- Versioned: `V{version}__{description}.sql` (e.g., `V1.2__Add_users.sql`)
- Repeatable: `R__{description}.sql` (e.g., `R__Create_views.sql`)

## Config

Config resolution priority (highest wins):
1. CLI arguments
2. Environment variables (`WAYPOINT_DATABASE_URL`, etc.)
3. `waypoint.toml` (default path, override with `-c`)
4. Built-in defaults

## Integration testing

Integration tests require a running PostgreSQL instance. Set `TEST_DATABASE_URL` env var.
