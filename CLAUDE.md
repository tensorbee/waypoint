# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo build                                                          # Build both crates (default: postgres feature)
cargo build --features mysql                                         # Build with MySQL backend (postgres + mysql)
cargo build -p waypoint-core --no-default-features --features mysql  # MySQL-only (no PostgreSQL deps compiled in)
cargo test --lib                                                     # Unit tests (postgres only, 327 tests)
cargo test --features mysql --lib                                    # Unit tests with both backends (355 tests)
cargo test -p waypoint-core --no-default-features --features mysql --lib  # MySQL-only unit tests (276 tests)
cargo test --features mysql --test mysql_integration_test            # MySQL integration tests (20 tests, needs container)
cargo test                                               # Integration tests need TEST_DATABASE_URL (PG)
cargo clippy --features mysql --all-targets -- -D warnings  # Lint (use --features mysql to cover both paths)
cargo fmt --check                                        # Format check
cargo run -- --help                                      # Show CLI help
cargo run -- lint                                        # Run lint command (no DB needed)
```

Single test: `cargo test --lib test_name`

## Toolchain

Both crates are **edition 2024**, `rust-version = "1.88"` (edition 2024 needs
1.85; the let-chains used here need 1.88). `rust-toolchain.toml` pins the
compiler to 1.97.1 so local builds, CI, and release artifacts agree — bump it
deliberately, separately from `rust-version`.

Supply-chain gates (all must be clean):

```bash
cargo audit                          # no known advisories
cargo deny --all-features check      # advisories + licenses + bans + sources
cargo machete                        # no unused dependencies
```

### Release signature verification

`self-update` delegates Sigstore verification to the `cosign` binary rather
than doing it in-process. This is deliberate: keyless Fulcio certificates are
valid for only ~10 minutes, so proving one was valid *at signing time* requires
the Rekor transparency log. A partial in-process implementation would skip
exactly the check that makes keyless signing sound. The `sigstore-verify` crate
does it properly but pulls ~166 crates including a second HTTP stack and
`aws-lc-sys`, which `deny.toml` bans — so we shell out when cosign is present
and report honestly (`verification: "cosign-signature"` vs `"sha256-only"` in
`--json`) when it is not.

Verify a release by hand with:

```bash
cosign verify-blob --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity-regexp '^https://github\.com/tensorbee/waypoint/\.github/workflows/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
```

`deny.toml` bans `aws-lc-rs` / `aws-lc-sys` / `openssl-sys`. We are rustls +
**ring** only; rustls' *default* feature set selects the `aws_lc_rs` provider,
which drags in a cmake-built C/assembly crate, so our `rustls` dependency sets
`default-features = false`. If the ban ever fires, some dependency re-enabled
rustls' defaults — fix it there rather than relaxing the rule.

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
| `history.rs` | Schema history-table dispatcher + `AppliedMigration` shared type. PG/MySQL implementations live in `engines/{postgres,mysql}/history.rs` |
| `db.rs` | `DbClient` enum wrapping `tokio_postgres::Client` or `mysql_async::Pool`. Dialect-aware methods: `acquire_lock`, `current_user`, `current_database`, `resolve_schema`, `execute_raw`, `execute_in_transaction`. `connect_for_url()` is the single URL→`DbClient` entry (used by `lib.rs`, `multi.rs`, `commands/diff.rs`). `quote_ident` (PG) / `quote_ident_mysql` (backticks) for identifier quoting. Legacy PG-only `connect_*` / `acquire_advisory_lock` fns retained |
| `engines/` | Per-engine implementation modules. `engines/postgres/{history,migrate,advisor,safety}.rs` and `engines/mysql/{history,migrate,advisor,safety}.rs` hold the engine-specific bodies; the top-level modules expose the shared types and dialect-aware dispatchers and re-export the engine entry points for back-compat |
| `hooks.rs` | SQL callback hooks (beforeMigrate, afterEachMigrate, etc.) |
| `error.rs` | `WaypointError` enum (36 variants). `DatabaseError(tokio_postgres::Error)` is feature-gated; `MysqlError(mysql_async::Error)` added behind `mysql` feature |
| `directive.rs` | Parse `-- waypoint:*` directives (env, depends, require, ensure, safety-override) + inline `lint-ignore` / `lint-ignore-file` suppressions (`parse_lint_ignores`) |
| `guard.rs` | Guard expression parser + evaluator (10 built-in assertion functions). PG + MySQL builtin tables; engine paths still co-located. |
| `reversal.rs` | Auto-reversal generation from schema diffs, storage/retrieval. PG + MySQL paths still co-located. |
| `safety.rs` | Shared safety types (`LockLevel`, `SafetyVerdict`, `SafetyReport`, `SafetyConfig`) + `analyze_migration_db` dispatcher. Engine analysers live under `engines/{postgres,mysql}/safety.rs` |
| `advisor.rs` | Shared advisor types (`Advisory`, `AdvisorReport`, `AdvisorConfig`) + `analyze_db` dispatcher + `generate_fix_sql`. Engine rule sets live under `engines/{postgres,mysql}/advisor.rs` (A001-A010 / M001-M005) |
| `sql_parser.rs` | DDL extraction (`DdlOperation` enum), `split_statements()`, `strip_comments()` (offset-preserving comment blanking), `extract_ddl_operations_located()` (ops + source spans). Most forms are regex-matched; `ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS]` uses a hand-rolled tokenizer so `NOT NULL` / `DEFAULT` are read from the parsed column definition rather than substring-matched |
| `schema.rs` | Schema introspection, diff, and DDL generation. PG path uses `information_schema`/`pg_catalog` and emits PG-flavoured DDL; MySQL path uses `information_schema` + `SHOW CREATE` and emits MySQL-flavoured DDL via `generate_ddl_mysql` |
| `tls.rs` | TLS trust policy for both engines: `parse_url_sslmode` (lifts libpq `sslmode=`/`sslrootcert=` out of a connection string), `reconcile_ssl_mode` / `reconcile_root_cert` (precedence), `load_root_store` + `make_rustls_config` (PG), `make_mysql_ssl_opts` (MySQL) |
| `dependency.rs` | Migration dependency graph, topological sort (Kahn's algorithm). Live: `commands::migrate::select_pending` uses it when `migrations.dependency_ordering` is set |
| `preflight.rs` | Pre-migration health checks. PG checks (recovery mode, replication lag MB, locks, etc.) and MySQL checks (read-only, processlist, replica lag secs, etc.) co-located; dispatcher is `run_preflight_db` |
| `multi.rs` | Multi-database orchestration with dependency ordering |

### Commands (waypoint-core/src/commands/)

18 command modules, one per subcommand: `migrate`, `info`, `validate`, `repair`, `baseline`, `clean`, `undo`, `lint`, `changelog`, `diff`, `drift`, `snapshot`, `explain`, `check_conflicts`, `preflight`, `safety`, `advisor`, `simulate`.

No-DB commands (pure file analysis): `lint`, `changelog`, `check_conflicts` — already dialect-agnostic.

**MySQL support status** (commands working end-to-end against a real MySQL 8.0+ container):

| Command | Status | Notes |
|---|---|---|
| `migrate` | ✅ working | Hooks + validate-on-migrate + preflight + guards (require/ensure). Records `success = false` history rows on failure (DDL auto-commits, so a mid-file failure leaves a partially-migrated schema and the row is the only trace). Errors on `batch_transaction = true`. |
| `info` | ✅ working | Dialect-aware via `execute_db` |
| `validate` | ✅ working | Checksum check; same Flyway-compat CRC32 |
| `repair` | ✅ working | Drops failed rows; updates checksums |
| `baseline` | ✅ working | Refuses if history table has entries |
| `clean` | ✅ working | Disables FOREIGN_KEY_CHECKS, drops views/tables/routines/events |
| `snapshot` | ✅ working | `SHOW CREATE TABLE` / `SHOW CREATE VIEW` based |
| `restore` | ✅ working | Wipes target DB, replays snapshot via MySQL-aware splitter |
| `undo` | ✅ working | Manual U-files take precedence; falls back to auto-generated reversal via `generate_ddl_mysql` |
| `preflight` | ✅ working | 6 MySQL checks: read-only, connections, processlist, replica lag, db size, metadata locks |
| `simulate` | ✅ working | Replicates tables + views into a temp DB via SHOW CREATE; view DB qualifiers rewritten. The whole replay runs on the single `Conn` that issued `USE temp_db`, and `${waypoint:database}` resolves to the temp DB — the sandbox must never leak onto the live schema |
| `safety` | ✅ working | Pessimistic worst-case ALGORITHM=COPY lock mapping; size from `information_schema.tables.table_rows` |
| `advise` | ✅ working | MySQL rule set M001-M005 (FK without index, no PK, non-utf8mb4, non-InnoDB, dup indexes) |
| `guards` (require / ensure) | ✅ working | 9 builtin functions ported to information_schema (`enum_exists` rejected — MySQL has no enum type) |
| `diff` | ✅ working | Structural diffs over information_schema introspection; generated DDL is best-effort PG syntax |
| `drift` | ✅ working | Throwaway database + USE-scoped migration replay; structural diff against live |
| `explain` | ✅ working | `EXPLAIN FORMAT=JSON`; access_type=ALL surfaced as a warning |
| `lint` / `changelog` / `check-conflicts` | ✅ working | No-DB; engine-agnostic |
| Multi-database orchestration | ✅ working | Mixed-engine configs (PG + MySQL in the same `[[databases]]` list) supported |
| Auto-reversal generation | ✅ working | `schema::generate_ddl_mysql` emits MySQL-flavored reverse DDL; dependent constraint/index diffs filtered when their parent table is also being dropped (since MySQL has no CASCADE) |

### CLI (waypoint-cli/src/)

| File | Purpose |
|---|---|
| `main.rs` | clap CLI with `Cli` struct, `Commands` enum, subcommand routing, exit codes 0-15 |
| `output.rs` | Terminal formatters using `comfy-table` + `colored` for all commands |
| `self_update.rs` | GitHub releases API check, binary download/replace (feature-gated). Two gates: (1) `SHA256SUMS` is verified against its Sigstore signature by shelling out to `cosign` (bundle form, falling back to the detached `.sig`/`.pem` that releases ≤ v0.5.0 publish), pinned to this repo's workflow identity and GitHub's OIDC issuer; (2) the tarball's SHA-256 must match its `SHA256SUMS` entry. A *failed* signature always aborts. A *missing* `cosign` downgrades to checksum-only with a loud warning, or aborts under `--require-signature`. Never auto-runs `curl \| sh` |
| `build.rs` | Injects `GIT_HASH` and `BUILD_TIME` at compile time |

### Key patterns

- **Config resolution**: CLI > env vars > TOML > defaults (see `config.rs` `load()`)
- **Engine dispatch**: `Waypoint::new` auto-detects the engine from `config.connection_string()`'s URL scheme. Each public method on `Waypoint` either uses the dialect-aware `execute_db(&DbClient, ...)` path or routes via `client.dialect_kind()` to the right backend impl
- **Legacy + dialect-aware command pairs**: Every command has a dialect-aware `execute_db(&DbClient, ...)` entry, used by `lib.rs` and `multi.rs`. A handful of commands still keep a legacy `execute(&Client, ...)` PG-only entry — these are now only consumed by `engines/postgres/migrate.rs` (which calls `validate::execute`) and by `commands/explain.rs` (which calls `info::execute`). The unused `execute(&Client, ...)` entries (`advisor`, `baseline`, `clean`, `diff`, `repair`, `safety`, plus both `migrate::execute`) are marked `#[deprecated(since = "0.6.0")]` and scheduled for removal in 1.0 — removing a public item is a breaking change, so they are retired rather than deleted
- **Global CLI flags**: `--json`, `--dry-run`, `--quiet`, `--verbose`, `--environment`, `--skip-preflight`, `--database`, `--fail-fast`, `--force`, `--simulate`, `--no-color`, `--config/-c` are `global = true` in clap — work before or after subcommand
- **Self-update feature-gated**: `ureq`, `semver`, `flate2`, `tar`, `sha2` are behind `self-update` feature (default on). Build without: `cargo build --no-default-features --features postgres`
- **Config macros**: `apply_option!` and `apply_option_some!` macros eliminate boilerplate in `config.rs`
- **print_report! macro**: CLI uses `print_report!` macro for uniform JSON/pretty-print output
- **Schema introspection**: PG uses `tokio::try_join!()` to parallelize 9 independent queries; N+1 pattern eliminated with JOIN. MySQL path (`schema::introspect_mysql`) issues per-area `information_schema` queries and resolves view DB-qualifiers — sequences/functions/enums come back empty (no MySQL equivalents)
- **Multi-database mode**: Auto-detected when `config.multi_database.is_some()`. Uses Kahn's algorithm for dependency ordering; mixed-engine configs (PG + MySQL in the same `[[databases]]` list) are supported via `multi::run_migrate_for_db` which routes per-database based on `DialectKind`
- **All reports are `Serialize`**: Every command returns a report struct that implements `serde::Serialize` for `--json` output
- **Migration file types**: `V{ver}__desc.sql` (versioned), `R__desc.sql` (repeatable), `U{ver}__desc.sql` (undo)
- **Directives**: `-- waypoint:env`, `-- waypoint:depends`, `-- waypoint:require`, `-- waypoint:ensure`, `-- waypoint:safety-override` parsed from SQL file headers by `directive.rs`
- **Lint semantics run on comment-free SQL**: `lint.rs` calls `strip_comments()` (which blanks comments to spaces, preserving byte offsets and line breaks) and evaluates every rule against the individual statement span, not the whole file. Diagnostics resolve offsets back against the original SQL, so E001 points at the column-name token. When adding a rule, anchor it to `LocatedDdl.focus` and never substring-match the raw file text
- **Lint suppression**: `-- waypoint:lint-ignore <RULES> reason=<why>` (next statement) and `-- waypoint:lint-ignore-file <RULES> reason=<why>` (whole file). The reason is mandatory — a directive missing rules or a reason raises `E003` and is not applied; one that matches nothing raises `I002`. Applied suppressions are reported in `LintReport.suppressions`
- **Guards**: `require` (preconditions) and `ensure` (postconditions) use a recursive descent parser in `guard.rs`; the legacy `evaluate(&Client, ...)` path queries `information_schema`/`pg_catalog`, and the dialect-aware `evaluate_db(&DbClient, ...)` path dispatches between the PG and MySQL builtin tables (`enum_exists` rejected on MySQL — no enum type)
- **Auto-reversals**: `reversal.rs` captures before/after schema snapshots, generates reverse DDL, stores in `reversal_sql` column; `undo.rs` falls back to stored reversals when no U file exists. PG uses `schema::generate_ddl`; MySQL uses `schema::generate_ddl_mysql` (with dependent constraint/index diffs filtered when the parent table is being dropped — MySQL has no CASCADE)
- **Safety analysis**: shared types in `safety.rs` + dialect-aware `analyze_migration_db` dispatcher. PG analyser (`engines/postgres/safety.rs`) maps DDL → PG lock levels and queries `pg_stat_user_tables`; MySQL analyser (`engines/mysql/safety.rs`) uses worst-case ALGORITHM=COPY lock mapping and `information_schema.tables.table_rows`; `migrate.rs` gates DANGER migrations behind `--force` on PG (MySQL safety verdicts are advisory, not gating)
- **MySQL non-transactional DDL caveat**: Documented and respected, not emulated. `--transaction` batch mode is not supported on MySQL. `ensure` guards run verify-after on MySQL (DDL has auto-committed) rather than rollback-if-false
- **TLS trust policy is single-sourced**: `tls.rs` owns the mode→trust mapping for both engines. `SslMode` is libpq's ladder and carries libpq's meanings — `require` encrypts *without* authenticating, so only `verify-ca`/`verify-full` build a trust store. `ssl_root_cert` replaces the default roots rather than adding to them, and a bad CA path is an error on the verifying modes rather than a fallback. **Enforcement lives in `tokio_postgres::Config::ssl_mode`, not just in the connector choice** — setting only the connector (as before 0.7.0) leaves tokio-postgres at its own `Prefer` default, so a server refusing SSL silently downgrades. Do not add an outer plaintext retry for `prefer`: tokio-postgres does that in band, and a second connect doubles authentication attempts and misreports non-TLS failures as downgrades
- **Advisory locks are scoped by schema on both engines**: `db::advisory_lock_id(schema, table)` hashes `schema\0table` (NUL separator so `("a","b_c")` and `("a_b","c")` cannot collide); `mysql_lock_key(schema, table)` already did the equivalent. PostgreSQL used to hash the table name alone, which made schema-per-tenant layouts serialise every migration in the database. **Changing this key means two waypoint versions do not exclude each other** — that is why 0.8.0 carries a rollout note. Callers must resolve the schema *before* acquiring, so the lock key matches the schema the command then operates on
- **Anything an operator can observe must be deterministic**: both topological sorts (`dependency::topological_sort`, `multi::execution_order`) fed their ready-set from a `HashSet`/`HashMap`, whose iteration order is randomly seeded per process — so apply order varied run to run, `explain` previewed an order `migrate` did not use, and environments disagreed on `installed_rank`. Ties break by version and by `[[databases]]` declaration order respectively. When iterating a hash container, ask whether the order reaches the operator; if it does, sort it
- **Generated DDL must be in dependency order and self-contained**: `schema::generate_ddl` sorts by `ddl_rank` (drops inwards, creates outwards) and `order_diffs_for_ddl` drops indexes that a constraint in the same set recreates — PostgreSQL builds a backing index for every PK/UNIQUE, and introspection sees both. `reversal::scope_reversal_to_schema` prepends `SET LOCAL search_path`, because neither `migrate` nor `undo` sets it and the generator emits unqualified names. All three were needed before `undo` of a dropped table worked at all
- **The four apply paths must agree**: `migrate` has four — PG non-batch versioned, PG non-batch repeatable, PG batch, and MySQL — and every guard/hook defect found so far was one of them disagreeing with the other three. The order is **safety → require → beforeEachMigrate → apply → ensure → afterEachMigrate** in all four. Guards go *before* the before-hook so a skipped migration does not fire an unpaired hook. When adding a step, add it to all four or explain in a comment why not
- **Analysis runs on comment- and literal-blanked SQL**: `extract_ddl_operations_located` matches against `blank_string_literals(strip_comments(sql))`, because the patterns are unanchored and would otherwise read `INSERT … VALUES ('DROP TABLE users')` as a real drop — which made `safety` block a valid migration. Both helpers preserve byte offsets, so diagnostics still resolve. `parse_add_columns` deliberately gets the *un*-blanked copy: it is already literal-aware and it captures `default_expr`, which may legitimately be a literal
- **Values in generated SQL go through `db::quote_literal`**, names through `quote_ident` / `quote_ident_mysql`. Enum labels were interpolated raw, so an apostrophe produced a snapshot that would not restore — and `restore` only warns on a failed statement, so the loss was near-silent
- **Throwaway sandbox names come from `db::sandbox_name`**: `simulate` and `drift` drop their sandbox unconditionally, including when their own `CREATE` failed, so a name collision means one run destroys another's. A clock reading alone is not enough — the helper adds the process id and randomness
- **A setting that cannot be honoured must be refused or documented, never ignored**: MySQL silently dropped `statement_timeout` and `keepalive`; five numeric env vars silently dropped unparseable values; `[[databases]]` entries silently ignored the top-level `[migrations]` block. Per-engine gaps that are genuinely unimplementable (`connect_timeout`/`connect_retries` on MySQL's lazy pool) belong in `docs/ENGINES.md`'s difference table
- **`--dry-run` is global but not universally implementable**: clap advertises it on every subcommand, so a command that cannot preview must *refuse* it, never ignore it. `reject_unpreviewable_dry_run` in `waypoint-cli/src/main.rs` returns a `ConfigError` for `baseline`/`undo`/`clean`/`restore <id>`/`self-update`; `migrate` routes to `explain`, and `repair` has a real preview. Silently ignoring the flag on a mutating command is how issue #2 destroyed history rows — if you add a mutating subcommand, add it to the guard or give it a preview
- **`repair --dry-run` must issue no writes at all**: `commands::repair::execute_db_with(client, config, dry_run)` computes the whole plan from a `SELECT` and only then applies it, so a dry run skips the failed-row `DELETE`, the checksum `UPDATE`s, *and* the `CREATE TABLE IF NOT EXISTS` bootstrap (it calls `history_table_exists_db` instead). The invariant is that dry-run-then-real must report identical counts — asserted in `test_repair_dry_run_makes_no_changes_and_reports_same_work_as_real_repair`
- **`repair` refuses to run against missing migration locations**: `scan_migrations` only warns and skips a directory that does not exist, which is right for read-only commands but not for `repair` — with no files on disk every checksum comparison is vacuous, so it would report "nothing to update" for the wrong reason while still deleting rows. `ensure_migration_locations` raises a `ConfigError` before the ledger is touched
- **Pending-migration selection is single-sourced**: `commands::migrate::select_pending(resolved, &PendingCriteria)` decides baseline/target/out-of-order/environment/repeatable-checksum for *both* engines, so PG and MySQL cannot drift. An out-of-order version with `out_of_order = false` is an `OutOfOrder` **error** on both engines — never a silent skip
- **Guard policy is single-sourced**: `classify_require` / `classify_ensure` / `guard_parse_error` in `commands/migrate.rs` own the on-fail policy, logging, and `GuardFailed` shape; the engine modules only supply the evaluation call (`guard::evaluate` vs `guard::evaluate_db`). `guards.enabled = false` short-circuits both
- **MySQL named locks need a pinned connection**: `GET_LOCK` is session-scoped and `mysql_async`'s pool resets connections on return (`COM_RESET_CONNECTION` releases `GET_LOCK` locks). `db.rs` parks the acquiring `Conn` in a process-global registry for the lock's lifetime and releases on that same session. Never take a MySQL lock on a borrowed-then-dropped connection
- **MySQL session state must stay on one connection**: `DbClient::execute_raw` checks out a *fresh* pooled connection each call, so anything relying on `USE`/`SET` (simulate, drift, clean, restore) must hold one `Conn` and issue every statement on it. `simulate` previously replayed migrations through `execute_raw` after `USE temp_db` and hit the live database
- **`MigrationVersion` identity is the normalized segment list**: trailing zeros are dropped, so `1`, `1.0` and `1.0.0` are one version for `Ord`, `Eq` *and* `Hash`. `scan_migrations` rejects `V1__a.sql` alongside `V1.0__b.sql` as duplicates
- **Multi-database inherits global policy**: `NamedDatabaseConfig::to_waypoint_config_inheriting(parent)` carries `[safety]`, `[preflight]`, `[guards]`, `[reversals]`, `[advisor]`, `[snapshots]`, `[lint]`, `[simulation]` from the top level; `[[databases]]` entries only override connection/migration/hook/placeholder settings. The `*_inheriting` variants on `MultiWaypoint` are what the CLI calls
- **MySQL schema fallback**: `DbClient::resolve_schema(configured)` returns `configured` on PG. On MySQL, when `configured == "public"` (the PG default) it falls back to `DATABASE()` so a PG-shaped config keeps working when pointed at MySQL

## Config

Config resolution priority (highest wins):
1. CLI arguments
2. Environment variables (`WAYPOINT_DATABASE_URL`, `WAYPOINT_ENVIRONMENT`, etc.)
3. `waypoint.toml` (default path, override with `-c`)
4. Built-in defaults

Key TOML sections: `[database]`, `[migrations]`, `[lint]`, `[snapshots]`, `[preflight]`, `[hooks]`, `[placeholders]`, `[guards]`, `[reversals]`, `[safety]`, `[advisor]`, `[simulation]`, `[[databases]]` (multi-db array).

Notable keys:

- `[database] engine = "postgres" | "mysql"` (env: `WAYPOINT_DATABASE_ENGINE`) — only consulted when `url` is unset. With a `url` the engine comes from the scheme. Without it, this decides whether `connection_string()` emits a libpq `key=value` string or a `mysql://` URL
- `[database] ssl_mode` (env: `WAYPOINT_SSL_MODE`, flag: `--ssl-mode`) — the libpq ladder: `disable | prefer | require | verify-ca | verify-full`. `require` encrypts *without* verifying, as libpq defines it; `allow` is rejected rather than aliased to `prefer`
- `[database] ssl_root_cert` (env: `WAYPOINT_SSL_ROOT_CERT`, flag: `--ssl-root-cert`) — CA PEM that **replaces** the built-in trust store (libpq `sslrootcert`). Only read by the verifying modes; a missing or empty file is an error, never a fallback to the default roots
- `[guards] enabled = true|false` — turns `require`/`ensure` evaluation off wholesale
- `[migrations] dependency_ordering` — apply order follows `-- waypoint:depends` topologically instead of version order (degrades to version order when nothing declares a dependency)
- `[migrations] show_progress` — suppresses the migrate summary like `--quiet`, without affecting `--json` or errors
- `[snapshots] auto_snapshot_on_migrate` and `[advisor] run_after_migrate` — post-migrate follow-ups; neither can turn a successful migration into a failed command

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
