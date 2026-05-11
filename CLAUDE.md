# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                                              # Build both crates (default: postgres feature)
cargo build --features mysql                             # Build with MySQL backend
cargo test --lib                                         # Unit tests (postgres only, 261 tests)
cargo test --features mysql --lib                        # Unit tests with both backends (267 tests)
cargo test --features mysql --test mysql_integration_test  # MySQL integration tests (18 tests, needs container)
cargo test                                               # Integration tests need TEST_DATABASE_URL (PG)
cargo clippy --features mysql --all-targets -- -D warnings  # Lint (use --features mysql to cover both paths)
cargo fmt --check                                        # Format check
cargo run -- --help                                      # Show CLI help
cargo run -- lint                                        # Run lint command (no DB needed)
```

Single test: `cargo test --lib test_name`

## Backend selection (Cargo features)

`waypoint-core` and `waypoint-cli` are feature-gated for engine support:

- `postgres` (default) — pulls in `tokio-postgres` + `rustls`. Existing PG users get this with zero changes.
- `mysql` (opt-in) — pulls in `mysql_async` with `rustls-tls` + `chrono`. Enable with `--features mysql`.

Both features can be enabled together for mixed-engine multi-database orchestration. Engine is auto-detected from the connection URL scheme: `postgres://` / `postgresql://` → PostgreSQL, `mysql://` → MySQL.

For the workspace `cargo check`, build, and clippy, **prefer `--features mysql`** (which is additive on top of the default `postgres`) to cover both code paths in one pass.

## Architecture

Cargo workspace with two crates:

- **waypoint-core** (`waypoint-core/`) — Library crate. Public API is `Waypoint` struct in `lib.rs`. All migration logic, schema introspection, and command implementations live here.
- **waypoint-cli** (`waypoint-cli/`) — Binary crate (`waypoint`). clap-based CLI with 19 subcommands, colored table output, self-update.

### Core modules (waypoint-core/src/)

| Module | Purpose |
|---|---|
| `config.rs` | Config loading: TOML file + env vars + CLI overrides, 4-layer priority |
| `dialect/` | `DatabaseDialect` trait + `DialectKind` enum + Postgres/MySQL impls. Pure (no-DB) per-engine knobs: identifier quoting, history-table DDL, transactional-DDL capability (gates batch-transaction mode) |
| `migration.rs` | `ResolvedMigration`, `MigrationVersion`, filename parsing, file scanning |
| `checksum.rs` | CRC32 checksum (line-by-line, Flyway-compatible) |
| `placeholder.rs` | `${key}` placeholder replacement in SQL |
| `history.rs` | Schema history table CRUD. Has legacy PG-only fns and `_db`-suffixed dialect-aware variants |
| `db.rs` | `DbClient` enum wrapping `tokio_postgres::Client` or `mysql_async::Pool`. Dialect-aware methods: `acquire_lock`, `current_user`, `current_database`, `resolve_schema`, `execute_raw`, `execute_in_transaction`. Legacy PG-only `connect_*` / `acquire_advisory_lock` fns retained |
| `hooks.rs` | SQL callback hooks (beforeMigrate, afterEachMigrate, etc.) |
| `error.rs` | `WaypointError` enum (36 variants). `DatabaseError(tokio_postgres::Error)` is feature-gated; `MysqlError(mysql_async::Error)` added behind `mysql` feature |
| `directive.rs` | Parse `-- waypoint:*` directives (env, depends, require, ensure, safety-override) |
| `guard.rs` | Guard expression parser + evaluator (10 built-in assertion functions). PostgreSQL-only for now |
| `reversal.rs` | Auto-reversal generation from schema diffs, storage/retrieval. PostgreSQL-only for now |
| `safety.rs` | Lock analysis, impact estimation, safety verdicts (Safe/Caution/Danger). PostgreSQL-only for now |
| `advisor.rs` | Schema advisory rules (A001-A010), fix SQL generation. PostgreSQL-only for now |
| `sql_parser.rs` | Regex-based DDL extraction (`DdlOperation` enum), `split_statements()` |
| `schema.rs` | PostgreSQL introspection via `information_schema`/`pg_catalog`, diff, DDL generation. PostgreSQL-only for now |
| `dependency.rs` | Migration dependency graph, topological sort (Kahn's algorithm) |
| `preflight.rs` | Pre-migration health checks (recovery mode, replication lag, locks, etc.). PostgreSQL-only for now |
| `multi.rs` | Multi-database orchestration with dependency ordering |

### Commands (waypoint-core/src/commands/)

18 command modules, one per subcommand: `migrate`, `info`, `validate`, `repair`, `baseline`, `clean`, `undo`, `lint`, `changelog`, `diff`, `drift`, `snapshot`, `explain`, `check_conflicts`, `preflight`, `safety`, `advisor`, `simulate`.

No-DB commands (pure file analysis): `lint`, `changelog`, `check_conflicts` — already dialect-agnostic.

**MySQL support status** (commands working end-to-end against a real MySQL 8.0+ container):

| Command | Status | Notes |
|---|---|---|
| `migrate` | ✅ working | Hooks + validate-on-migrate + preflight + guards (require/ensure). Errors on `batch_transaction = true` (MySQL DDL auto-commits). |
| `info` | ✅ working | Dialect-aware via `execute_db` |
| `validate` | ✅ working | Checksum check; same Flyway-compat CRC32 |
| `repair` | ✅ working | Drops failed rows; updates checksums |
| `baseline` | ✅ working | Refuses if history table has entries |
| `clean` | ✅ working | Disables FOREIGN_KEY_CHECKS, drops views/tables/routines/events |
| `snapshot` | ✅ working | `SHOW CREATE TABLE` / `SHOW CREATE VIEW` based |
| `restore` | ✅ working | Wipes target DB, replays snapshot via MySQL-aware splitter |
| `undo` | ✅ working | Manual U-files only — auto-reversal generation still PG-specific |
| `preflight` | ✅ working | 6 MySQL checks: read-only, connections, processlist, replica lag, db size, metadata locks |
| `simulate` | ✅ working | Replicates tables + views into a temp DB via SHOW CREATE; view DB qualifiers rewritten |
| `safety` | ✅ working | Pessimistic worst-case ALGORITHM=COPY lock mapping; size from `information_schema.tables.table_rows` |
| `advise` | ✅ working | MySQL rule set M001-M005 (FK without index, no PK, non-utf8mb4, non-InnoDB, dup indexes) |
| `guards` (require / ensure) | ✅ working | 9 builtin functions ported to information_schema (`enum_exists` rejected — MySQL has no enum type) |
| `diff` | ✅ working | Structural diffs over information_schema introspection; generated DDL is best-effort PG syntax |
| `drift` | ✅ working | Throwaway database + USE-scoped migration replay; structural diff against live |
| `explain` | ✅ working | `EXPLAIN FORMAT=JSON`; access_type=ALL surfaced as a warning |
| `lint` / `changelog` / `check-conflicts` | ✅ working | No-DB; engine-agnostic |
| Multi-database orchestration | ✅ working | Mixed-engine configs (PG + MySQL in the same `[[databases]]` list) supported |
| Auto-reversal generation | ⚠️ PG only | Depends on PG-specific DDL generation; structural MySQL diff lands, DDL emission deferred |

### CLI (waypoint-cli/src/)

| File | Purpose |
|---|---|
| `main.rs` | clap CLI with `Cli` struct, `Commands` enum, subcommand routing, exit codes 0-15 |
| `output.rs` | Terminal formatters using `comfy-table` + `colored` for all commands |
| `self_update.rs` | GitHub releases API check, binary download/replace with backup+validation (feature-gated) |
| `build.rs` | Injects `GIT_HASH` and `BUILD_TIME` at compile time |

### Key patterns

- **Config resolution**: CLI > env vars > TOML > defaults (see `config.rs` `load()`)
- **Engine dispatch**: `Waypoint::new` auto-detects the engine from `config.connection_string()`'s URL scheme. Each public method on `Waypoint` either uses the dialect-aware `execute_db(&DbClient, ...)` path or routes via `client.dialect_kind()` to the right backend impl
- **Legacy + dialect-aware command pairs**: Most ported commands keep an `execute(&Client, ...)` PG-only entry alongside a new `execute_db(&DbClient, ...)` dialect-aware entry. The legacy path serves internal callers in `multi.rs`, `explain.rs`, and the PG-specific helpers in `migrate.rs`. Removing the legacy path is deferred until every command is dialect-aware
- **Global CLI flags**: `--json`, `--dry-run`, `--quiet`, `--verbose`, `--environment`, `--skip-preflight`, `--database`, `--fail-fast`, `--force`, `--simulate`, `--no-color`, `--config/-c` are `global = true` in clap — work before or after subcommand
- **Self-update feature-gated**: `ureq`, `semver`, `flate2`, `tar` are behind `self-update` feature (default on). Build without: `cargo build --no-default-features --features postgres`
- **Config macros**: `apply_option!` and `apply_option_some!` macros eliminate boilerplate in `config.rs`
- **print_report! macro**: CLI uses `print_report!` macro for uniform JSON/pretty-print output
- **Schema introspection**: PG uses `tokio::try_join!()` to parallelize 9 independent queries; N+1 pattern eliminated with JOIN. MySQL schema introspection is not yet implemented
- **Multi-database mode**: Auto-detected when `config.multi_database.is_some()`. Uses Kahn's algorithm for dependency ordering. Currently PG-only at the routing layer; mixed-engine support is Phase 4 work
- **All reports are `Serialize`**: Every command returns a report struct that implements `serde::Serialize` for `--json` output
- **Migration file types**: `V{ver}__desc.sql` (versioned), `R__desc.sql` (repeatable), `U{ver}__desc.sql` (undo)
- **Directives**: `-- waypoint:env`, `-- waypoint:depends`, `-- waypoint:require`, `-- waypoint:ensure`, `-- waypoint:safety-override` parsed from SQL file headers by `directive.rs`
- **Guards**: `require` (preconditions) and `ensure` (postconditions) use a recursive descent parser in `guard.rs`; evaluated against live DB via `information_schema`/`pg_catalog`. MySQL guards not yet wired
- **Auto-reversals**: `reversal.rs` captures before/after schema snapshots, generates reverse DDL, stores in `reversal_sql` column; `undo.rs` falls back to stored reversals when no U file exists. PG only
- **Safety analysis**: `safety.rs` maps DDL → PostgreSQL lock levels, queries `pg_stat_user_tables` for row counts, produces Safe/Caution/Danger verdicts; `migrate.rs` gates DANGER migrations behind `--force`. PG only
- **MySQL non-transactional DDL caveat**: Documented and respected, not emulated. `--transaction` batch mode is not supported on MySQL. `ensure` guards (when ported in Phase 3) become verify-after rather than rollback-if-false
- **MySQL schema fallback**: `DbClient::resolve_schema(configured)` returns `configured` on PG. On MySQL, when `configured == "public"` (the PG default) it falls back to `DATABASE()` so a PG-shaped config keeps working when pointed at MySQL

## Config

Config resolution priority (highest wins):
1. CLI arguments
2. Environment variables (`WAYPOINT_DATABASE_URL`, `WAYPOINT_ENVIRONMENT`, etc.)
3. `waypoint.toml` (default path, override with `-c`)
4. Built-in defaults

Key TOML sections: `[database]`, `[migrations]`, `[lint]`, `[snapshots]`, `[preflight]`, `[hooks]`, `[placeholders]`, `[guards]`, `[reversals]`, `[safety]`, `[advisor]`, `[simulation]`, `[[databases]]` (multi-db array).

## Integration testing

### PostgreSQL

```bash
export TEST_DATABASE_URL="postgres://user:pass@localhost:5432/waypoint_test"
cargo test --test integration_test
```

Each PG test creates an isolated schema (`waypoint_test_{prefix}_{counter}`) within the database and tears it down after.

### MySQL

```bash
export TEST_MYSQL_URL="mysql://root:mysql@127.0.0.1:13306/mysql"  # optional; this is the default
cargo test --features mysql --test mysql_integration_test
```

Each MySQL test creates and drops a uniquely-named database (`waypoint_test_{prefix}_{counter}`). The default URL targets the developer container `tbdevrig-mysql` (MySQL 8.4 on port 13306).
