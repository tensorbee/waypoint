# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                    # Build both crates
cargo test --lib               # Unit tests only (no DB required, 249 tests)
cargo test                     # All tests (integration tests need TEST_DATABASE_URL)
cargo clippy -- -D warnings    # Lint
cargo fmt --check              # Format check
cargo run -- --help            # Show CLI help
cargo run -- lint              # Run lint command (no DB needed)
```

Single test: `cargo test --lib test_name`

## Architecture

Cargo workspace with two crates:

- **waypoint-core** (`waypoint-core/`) — Library crate. Public API is `Waypoint` struct in `lib.rs`. All migration logic, schema introspection, and command implementations live here.
- **waypoint-cli** (`waypoint-cli/`) — Binary crate (`waypoint`). clap-based CLI with 19 subcommands, colored table output, self-update.

### Core modules (waypoint-core/src/)

| Module | Purpose |
|---|---|
| `config.rs` | Config loading: TOML file + env vars + CLI overrides, 4-layer priority |
| `migration.rs` | `ResolvedMigration`, `MigrationVersion`, filename parsing, file scanning |
| `checksum.rs` | CRC32 checksum (line-by-line, Flyway-compatible) |
| `placeholder.rs` | `${key}` placeholder replacement in SQL |
| `history.rs` | Schema history table CRUD (`waypoint_schema_history`) |
| `db.rs` | Connection with TLS (rustls), advisory locks, transaction execution |
| `hooks.rs` | SQL callback hooks (beforeMigrate, afterEachMigrate, etc.) |
| `error.rs` | `WaypointError` enum (35 variants) with `thiserror` |
| `directive.rs` | Parse `-- waypoint:*` directives (env, depends, require, ensure, safety-override) |
| `guard.rs` | Guard expression parser + evaluator (10 built-in assertion functions) |
| `reversal.rs` | Auto-reversal generation from schema diffs, storage/retrieval |
| `safety.rs` | Lock analysis, impact estimation, safety verdicts (Safe/Caution/Danger) |
| `advisor.rs` | Schema advisory rules (A001-A010), fix SQL generation |
| `sql_parser.rs` | Regex-based DDL extraction (`DdlOperation` enum), `split_statements()` |
| `schema.rs` | PostgreSQL introspection via `information_schema`/`pg_catalog`, diff, DDL generation |
| `dependency.rs` | Migration dependency graph, topological sort (Kahn's algorithm) |
| `preflight.rs` | Pre-migration health checks (recovery mode, replication lag, locks, etc.) |
| `multi.rs` | Multi-database orchestration with dependency ordering |

### Commands (waypoint-core/src/commands/)

18 command modules, one per subcommand: `migrate`, `info`, `validate`, `repair`, `baseline`, `clean`, `undo`, `lint`, `changelog`, `diff`, `drift`, `snapshot`, `explain`, `check_conflicts`, `preflight`, `safety`, `advisor`, `simulate`.

No-DB commands (pure file analysis): `lint`, `changelog`, `check_conflicts`.

### CLI (waypoint-cli/src/)

| File | Purpose |
|---|---|
| `main.rs` | clap CLI with `Cli` struct, `Commands` enum, subcommand routing, exit codes 0-15 |
| `output.rs` | Terminal formatters using `comfy-table` + `colored` for all commands |
| `self_update.rs` | GitHub releases API check, binary download/replace with backup+validation (feature-gated) |
| `build.rs` | Injects `GIT_HASH` and `BUILD_TIME` at compile time |

### Key patterns

- **Config resolution**: CLI > env vars > TOML > defaults (see `config.rs` `load()`)
- **Global CLI flags**: `--json`, `--dry-run`, `--quiet`, `--verbose`, `--environment`, `--skip-preflight`, `--database`, `--fail-fast`, `--force`, `--simulate`, `--no-color`, `--config/-c` are `global = true` in clap — work before or after subcommand
- **Self-update feature-gated**: `ureq`, `semver`, `flate2`, `tar` are behind `self-update` feature (default on). Build without: `cargo build --no-default-features`
- **Config macros**: `apply_option!` and `apply_option_some!` macros eliminate boilerplate in `config.rs`
- **print_report! macro**: CLI uses `print_report!` macro for uniform JSON/pretty-print output
- **Schema introspection**: Uses `tokio::try_join!()` to parallelize 9 independent queries; N+1 pattern eliminated with JOIN
- **Multi-database mode**: Auto-detected when `config.multi_database.is_some()`. Uses Kahn's algorithm for dependency ordering.
- **All reports are `Serialize`**: Every command returns a report struct that implements `serde::Serialize` for `--json` output
- **Migration file types**: `V{ver}__desc.sql` (versioned), `R__desc.sql` (repeatable), `U{ver}__desc.sql` (undo)
- **Directives**: `-- waypoint:env`, `-- waypoint:depends`, `-- waypoint:require`, `-- waypoint:ensure`, `-- waypoint:safety-override` parsed from SQL file headers by `directive.rs`
- **Guards**: `require` (preconditions) and `ensure` (postconditions) use a recursive descent parser in `guard.rs`; evaluated against live DB via information_schema/pg_catalog
- **Auto-reversals**: `reversal.rs` captures before/after schema snapshots, generates reverse DDL, stores in `reversal_sql` column; `undo.rs` falls back to stored reversals when no U file exists
- **Safety analysis**: `safety.rs` maps DDL → PostgreSQL lock levels, queries `pg_stat_user_tables` for row counts, produces Safe/Caution/Danger verdicts; `migrate.rs` gates DANGER migrations behind `--force`

## Config

Config resolution priority (highest wins):
1. CLI arguments
2. Environment variables (`WAYPOINT_DATABASE_URL`, `WAYPOINT_ENVIRONMENT`, etc.)
3. `waypoint.toml` (default path, override with `-c`)
4. Built-in defaults

Key TOML sections: `[database]`, `[migrations]`, `[lint]`, `[snapshots]`, `[preflight]`, `[hooks]`, `[placeholders]`, `[guards]`, `[reversals]`, `[safety]`, `[advisor]`, `[simulation]`, `[[databases]]` (multi-db array).

## Integration testing

Integration tests require a running PostgreSQL instance:
```bash
export TEST_DATABASE_URL="postgres://user:pass@localhost:5432/waypoint_test"
cargo test --test integration_test
```

Each test creates an isolated schema (`waypoint_test_{prefix}_{counter}`) and tears it down after.
