# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-07-30

### Breaking

- **`ssl_mode = "require"` no longer verifies the server certificate.**
  `SslMode` now implements libpq's full ladder — `disable`, `prefer`,
  `require`, `verify-ca`, `verify-full` — and each rung carries libpq's
  meaning. Previously `require` verified against the Mozilla CA bundle, which
  matched neither libpq's `require` (encrypt, do not authenticate) nor its
  `verify-full`. **If you set `require` and want the certificate checked,
  change it to `verify-full`.** In practice most `require` users were not
  getting verification anyway: a verification failure fell through to the
  plaintext path described below.
- **`DatabaseConfig` gained an `ssl_root_cert` field** and `SslMode` gained two
  variants. Both are additive and `Default` is unchanged, but an exhaustive
  struct literal or an exhaustive `match` on `SslMode` will need updating.
  `CliOverrides` gained the matching field.
- `ssl_mode = "allow"` is now rejected with an error naming `prefer`. libpq's
  `allow` prefers *plaintext* and only upgrades if the server refuses, so
  silently treating it as `prefer` would have inverted the preference order.
- `db::connect`, `db::connect_with_config` and `db::connect_with_full_config`
  are deprecated in favour of `db::connect_with_transport`, which takes a
  `TransportConfig`. The old signatures cannot express `ssl_root_cert`. They
  still work and are scheduled for removal in 1.0.

### Fixed

- **`ssl_mode = "require"` did not require TLS.** Waypoint chose the TLS
  *connector* but never set `tokio_postgres::Config::ssl_mode`, and
  `connection_string()` never emitted `sslmode=`. tokio-postgres therefore
  stayed at its own default of `Prefer`, which accepts a server that answers
  the SSLRequest with "no" and continues in cleartext. A PostgreSQL server with
  TLS switched off silently produced an unencrypted connection under
  `require`. The mode is now pushed into the driver config, so `require` and
  above genuinely refuse to proceed unencrypted.
- **`prefer` silently downgraded to plaintext on any TLS error, including
  certificate failures.** The fallback caught every error and retried without
  TLS, logging only at `debug`. Against a server using a private CA — RDS with
  a private CA, or any internal PKI — the default configuration therefore ran
  unencrypted with no indication. `prefer` now relies on tokio-postgres's
  in-band downgrade, which only triggers when the server actually declines
  TLS; a handshake failure is reported instead of being papered over. This also
  removes a second connection attempt that doubled authentication attempts
  against the server on every plaintext connect.
- **MySQL ignored `ssl_mode` entirely.** `connect_for_url` passed the setting
  only down the PostgreSQL arm and built the MySQL pool with a bare
  `Pool::from_url`, so MySQL connections were plaintext unless the URL itself
  carried `require_ssl=true`. The full ladder now applies to both engines.
- A `sslmode=verify-ca` or `sslmode=verify-full` embedded in a connection URL
  was a hard parse error, as was `sslrootcert=` — tokio-postgres rejects the
  first as an invalid value and the second as an unknown option. Both are now
  lifted out of the connection string and honoured, so an ordinary libpq or
  JDBC-shaped URL works.
- An invalid `WAYPOINT_SSL_MODE` or `--ssl-mode` was discarded with no message
  at all, leaving the connection on `prefer`. A typo in either now warns.

### Added

- **`ssl_root_cert`** — a PEM file of CA certificates to verify the server
  against, settable as `[database] ssl_root_cert`, `WAYPOINT_SSL_ROOT_CERT`,
  `--ssl-root-cert`, or `sslrootcert=` in a connection URL. Matching libpq's
  `sslrootcert`, it **replaces** the built-in Mozilla trust store rather than
  adding to it — the point of pinning a private CA is that the public ones no
  longer apply. Only the verifying modes read it, and a missing, unreadable or
  certificate-free file is an error rather than a quiet fallback to the default
  roots. `[[databases]]` entries inherit it along with the rest of `[database]`.
- `SSL_ROOT_CERT` is passed through by the Docker entrypoint, so a CA can be
  mounted into the container.

### Changed

- TLS trust policy for both engines now lives in one new module,
  `waypoint-core/src/tls.rs`, so the PostgreSQL and MySQL paths cannot drift.
- MySQL refuses `require` and above over a Unix socket. `mysql_async` sends the
  SSLRequest and then skips the upgrade for socket connections, which would
  otherwise hand back a plaintext session reporting success.
- `verify-ca` on MySQL warns that it behaves as `verify-full`. `mysql_async`
  0.37 detects a hostname mismatch by looking for `NotValidForName` in the
  rustls error text, and rustls 0.23 renders that error as `certificate not
  valid for name …`, so the relaxation never takes effect. Waypoint still
  requests it — it starts working as soon as the driver matches on the error
  enum — and says so, since the failure is stricter than requested rather than
  weaker. See `docs/ENGINES.md`.
- No new dependencies. The CA PEM is parsed through `rustls::pki_types::pem`,
  which is already in the graph, rather than adding `rustls-pemfile`.

## [0.6.1] - 2026-07-29

### Fixed

- **MySQL advisory locks were scoped to the whole server, not the database.**
  `GET_LOCK` names live in a server-global namespace, unlike PostgreSQL
  advisory locks which are per-database. The key was derived from the history
  table name alone, so every database on a shared MySQL server contended for
  one lock — migrating `app_staging` blocked a concurrent migration of
  `app_prod`. The key now includes the database name, and over-long keys fall
  back to a CRC32 rather than being truncated (truncation could fold two
  distinct `db.table` pairs onto one lock).

### Changed

- The release workflow installs cross-compilation targets on the pinned
  toolchain. `rust-toolchain.toml`, added in 0.6.0, takes precedence over the
  toolchain the CI action installs, so the macOS and Linux cross-targets were
  built with a toolchain that lacked them. This is why v0.6.0 has no published
  binaries or signature; v0.6.1 restores them.

## [0.6.0] - 2026-07-29

### Breaking

- **MySQL: `beforeMigrate` / `afterMigrate` now fire on every `migrate`
  invocation**, including runs that apply nothing. Previously they only ran
  when there was pending work. This matches the PostgreSQL path and Flyway —
  they are run-lifecycle hooks, not work-lifecycle hooks. **A non-idempotent
  `beforeMigrate` hook will now fail on a no-op migrate**; make such hooks
  idempotent (`CREATE TABLE IF NOT EXISTS`, etc.), as the PostgreSQL side has
  always required.
- **`DatabaseConfig` gained an `engine` field.** Additive, and `Default` is
  `Postgres`, so `..Default::default()` construction and all existing
  behaviour are unchanged — but an exhaustive struct literal will need
  updating.
- **Minimum supported Rust version is now 1.88** and both crates moved to
  edition 2024.
- `V1__a.sql` alongside `V1.0__b.sql` is now rejected as a duplicate version.
  The two always compared equal for ordering; allowing both applied two
  migrations that every comparison treated as one.
- An out-of-order migration on MySQL now raises `OutOfOrder` instead of being
  silently skipped. Set `out_of_order = true` if the old behaviour was load-
  bearing.

### Fixed

- **`simulate` on MySQL applied migrations to the live database.** It set
  `USE <temp_db>` on one pooled connection and then replayed migrations
  through a *different* pooled connection whose default database was still the
  source, so a command documented as a dry run mutated the real schema.
- **The MySQL migration lock provided no mutual exclusion.** `GET_LOCK` is
  session-scoped and the connection pool resets connections on return, which
  releases such locks — so the lock was dropped the moment it was taken.
  Concurrent `waypoint migrate` runs against MySQL could interleave.
- **A failed MySQL migration left no record in the schema history table.**
  MySQL DDL auto-commits per statement, so a mid-file failure leaves a
  partially-migrated schema; the `success = false` row is the only trace of
  it, and is what `info` shows and `repair` clears.
- **Batch mode stored the whole-batch reversal against every version**, so
  undoing any single migration reverted the entire batch.
- **`self-update` verified nothing** beyond an HTTPS fetch and a `--version`
  smoke test, and fell back to `curl … | sh` automatically on any failure.
- MySQL migration files ending in a trailing comment (`…; -- done`) failed
  with `ER_EMPTY_QUERY`.
- Multi-database runs silently discarded the top-level `[safety]`,
  `[preflight]`, `[guards]`, `[reversals]`, `[advisor]`, `[snapshots]`,
  `[lint]` and `[simulation]` sections, and per-database entries ignored the
  `[database]` transport settings.
- `--database <name>` broke `migrate` and `info` in multi-database mode: every
  other database was reported as "not connected" and the command failed.
- `MigrationVersion` violated the `Ord`/`Eq` contract (`1` and `1.0` compared
  `Equal` but not `==`).
- `repair` rewrote `UNDO_SQL` rows' checksums to the forward migration's.
- Batch-transaction compatibility checks matched keywords inside comments, so
  a `-- VACUUM later` note could reject a valid migration.
- JDBC-style URLs did not percent-encode credentials lifted from the query
  string, producing a malformed URL for passwords containing `@` or `/`.
- A `waypoint-cli` build with only the `mysql` feature did not compile.
- MySQL TLS could never have worked in a `mysql`-only build: rustls was linked
  with no crypto provider.
- The crate's own Quick Start doctest had stopped compiling.
- `restore` no longer accepts a snapshot id that escapes the snapshot
  directory.

### Added

- **Release signature verification on `self-update`.** `SHA256SUMS` is
  verified against its Sigstore signature with `cosign` (bundle form, falling
  back to the detached `.sig`/`.pem` that releases up to v0.5.0 publish),
  pinned to this repository's workflow identity and GitHub's OIDC issuer; the
  tarball is then checked against that manifest. A failed signature always
  aborts. A missing `cosign` downgrades to SHA-256-only with a warning, or
  aborts under the new `--require-signature`. `--json` reports which level
  applied.
- `[database] engine = "postgres" | "mysql"` (env `WAYPOINT_DATABASE_ENGINE`),
  consulted when no `url` is set, so a host/port/user/database config can
  reach MySQL.
- `[guards] enabled` now works; it was previously read from nothing.
- `migrations.dependency_ordering` now works: apply order follows
  `-- waypoint:depends` topologically. This brought the dependency graph into
  use for the first time and fixed a latent false-cycle bug in it.
- `migrations.show_progress`, `snapshots.auto_snapshot_on_migrate` and
  `advisor.run_after_migrate` are now honoured; all three were previously
  parsed and ignored.

### Changed

- Dependencies updated to current majors (`mysql_async` 0.37, `webpki-roots`
  1.0, `colored` 3, `tokio-postgres-rustls` 0.14, `toml` 1.1).
- `aws-lc-sys` — a cmake-built C and assembly crate — is no longer in the
  dependency graph, and `deny.toml` now bans it along with `openssl-sys`.
  Four unused dependencies were removed.
- The unused PostgreSQL-only `execute(&Client, …)` entry points on `advisor`,
  `baseline`, `clean`, `diff`, `repair`, `safety` and `migrate` are
  deprecated; use the `execute_db(&DbClient, …)` equivalents. They will be
  removed in 1.0.

## [0.5.0] - 2026-07-29

### Fixed

- **`lint` E001 false positive on `ADD COLUMN IF NOT EXISTS`.** The
  `ALTER TABLE ... ADD COLUMN` parser treated the token after `ADD COLUMN` as
  the column name, so `IF NOT EXISTS` was reported as a column called `IF`.
  It now parses `ALTER TABLE <table> ADD [COLUMN] [IF NOT EXISTS] <column>
  <type> [constraints...]` with a real tokenizer that understands quoted and
  schema-qualified identifiers, multi-word and parenthesised types, and
  comma-separated `ADD COLUMN` clauses (each of which now yields its own
  operation).
- **`NOT NULL` / `DEFAULT` are read from the parsed column definition.**
  Previously E001 substring-matched the whole statement, so a `NOT NULL` in a
  comment, a string literal, or a `CHECK (...)` expression triggered the rule.
- **Comments are ignored during semantic analysis.** Lint, safety analysis,
  and changelog extraction now run against a comment-blanked copy of each
  file. Blanking preserves byte offsets, so diagnostics still point at the
  real statement rather than a preceding comment.
- **Accurate diagnostic line numbers.** Rules previously reported the first
  occurrence of a keyword anywhere in the file and counted lines off-by-one.
  Each finding now anchors to its own statement — E001 points at the column
  name — and line numbers are 1-based and exact.
- `W003` (`ALTER COLUMN TYPE`) evaluated its keyword guard against the whole
  file, so one matching statement could tag unrelated ones. It is now
  evaluated per statement.
- `W006` (volatile `DEFAULT`) matched `now()` / `random()` /
  `gen_random_uuid()` anywhere in the file. It now reads the column's own
  parsed `DEFAULT` expression, so a sibling clause in the same `ALTER TABLE` —
  or an unrelated `UPDATE ... now()` — no longer flags it.
- `E002` counted DDL *operations* rather than statements; a single
  `ALTER TABLE` with two `ADD COLUMN` clauses no longer trips it.
- `split_statements` now respects double-quoted and backtick-quoted
  identifiers, so a `;` inside a quoted identifier no longer splits a
  statement.

### Added

- **Scoped lint suppression with a mandatory reason.** `--disable E001`
  silences a rule for the whole run; these directives silence one statement
  or one file:

  ```sql
  -- waypoint:lint-ignore E001 reason="table is empty until the backfill runs"
  ALTER TABLE reid_shares ADD COLUMN threshold smallint NOT NULL;
  ```

  `-- waypoint:lint-ignore-file <RULES> reason=<why>` covers the whole file.
  A directive without a rule list or without a reason is rejected with the new
  `E003` and takes no effect. Applied suppressions and their justifications
  are reported in both pretty and `--json` output; a directive that matches
  nothing is flagged with the new `I002`.
- **Signed release checksums.** Every release now publishes `SHA256SUMS` over
  all platform archives, plus a keyless cosign signature (`SHA256SUMS.sig`)
  and certificate (`SHA256SUMS.pem`). `install.sh` verifies the downloaded
  archive automatically (`WAYPOINT_SKIP_CHECKSUM=1` to bypass).
- `sql_parser::extract_ddl_operations_located`, `strip_comments`, and
  `LocatedDdl` for callers that need source positions.

### Changed

- `DdlOperation::AlterTableAddColumn` gained `if_not_exists` and
  `default_expr` fields, and `LintReport` gained `suppressions` /
  `suppressed_count`. These are additive but break exhaustive struct-literal
  construction and exhaustive patterns in downstream code.

## [0.4.0] - 2026-05-11

### Added — MySQL 8.0+ support (opt-in via `mysql` Cargo feature)

Engine auto-detected from the connection URL scheme (`mysql://` → MySQL,
`postgres://` / `postgresql://` → PostgreSQL). Existing PostgreSQL users see
zero changes — the `postgres` feature is on by default.

**Commands working end-to-end on MySQL 8.0+:**

- `migrate` — with hooks (beforeMigrate, beforeEachMigrate, afterEachMigrate, afterMigrate), validate-on-migrate, preflight gating, and environment scoping. Refuses `batch_transaction = true` with a clear error since MySQL DDL is non-transactional.
- `info`, `validate`, `repair`, `baseline` — full parity with PostgreSQL using the dialect-aware `_db` entry points.
- `clean` — drops views, base tables, routines, and events; uses `FOREIGN_KEY_CHECKS = 0` so drop order doesn't matter.
- `snapshot` / `restore` — backed by `SHOW CREATE TABLE` / `SHOW CREATE VIEW` rather than full schema introspection.
- `undo` — supports manual `U{version}__*.sql` files plus auto-generated reversals on both engines.
- `preflight` — 6 MySQL-specific checks: `@@read_only` / `@@super_read_only`, threads-connected vs `@@max_connections`, long-running queries from `information_schema.PROCESSLIST`, `Seconds_Behind_Source` from `SHOW REPLICA STATUS`, database size from `information_schema.TABLES`, pending metadata locks from `performance_schema.metadata_locks`.
- `simulate` — replicates source structure into a throwaway database via `SHOW CREATE TABLE` and `SHOW CREATE VIEW` (with `\`source_db\`.` qualifier stripped so views bind to the temp database).
- `lint`, `changelog`, `check-conflicts` — already engine-agnostic (no DB required).

**Architecture:**

- `dialect/` module with `DatabaseDialect` trait + `DialectKind` enum + `PostgresDialect` / `MysqlDialect` impls. Pure (no-DB) per-engine knobs: identifier quoting, history-table DDL, transactional-DDL capability.
- `DbClient` enum wraps `tokio_postgres::Client` or `mysql_async::Pool`. Dialect-aware methods on `DbClient`: `acquire_lock`, `release_lock`, `current_user`, `current_database`, `resolve_schema`, `execute_raw`, `execute_in_transaction`.
- "Schema" fallback: when the configured schema is the PG default `"public"`, MySQL paths fall back to `DATABASE()` so a PG-shaped config keeps working when pointed at MySQL.
- Most ported commands keep a paired `execute(&Client, ...)` (PG legacy) + `execute_db(&DbClient, ...)` (dialect-aware) entry. Legacy entries serve internal callers in `multi.rs`, `explain.rs`, and the PG-specific helpers in `migrate.rs`.

**Configuration:**

- New `[preflight] max_replication_lag_secs` (default 30) — MySQL replica-lag threshold. Existing `max_replication_lag_mb` (default 100) remains PostgreSQL-only.
- New `mysql` Cargo feature on both `waypoint-core` and `waypoint-cli`. Build with `--features mysql` to opt in.

**Additional commands now working on MySQL:**

- `guards` (`require` / `ensure`) — 9 of 10 builtin assertion functions ported to `information_schema` (`table_exists`, `column_exists`, `column_type`, `column_nullable`, `index_exists`, `constraint_exists`, `function_exists`, `row_count`, `sql`). `enum_exists` is explicitly unsupported on MySQL since ENUM is a column-type modifier, not a schema object.
- `safety` — version-aware lock-level mapping for MySQL. On 8.0.29+ we detect `ALGORITHM=INSTANT`-eligible operations (`ADD COLUMN` nullable / NOT NULL with DEFAULT, `DROP COLUMN`) and downgrade them from worst-case `AccessExclusiveLock` to `LockLevel::None`. On older MySQL or when version detection fails, falls back to the conservative pessimistic mapping. Size classification from `information_schema.tables.table_rows`, optionally refreshed via `ANALYZE TABLE` first (`[safety] refresh_stats_mysql = true`). Suggestions tailored to MySQL (gh-ost for large index creation, upgrade-or-pt-osc for `DROP COLUMN` on < 8.0.29).
- `advise` — new MySQL rule set `M001`-`M005`: FK column without index, table without primary key, non-utf8mb4 charset, non-InnoDB storage engine, duplicate indexes.
- `diff` — `introspect_mysql` produces the same `SchemaSnapshot` shape as PG; structural diffs work across engines. Generated DDL is still PG-flavored; consume the structured `diffs[]` for MySQL.
- `drift` — throwaway database + held connection with `USE temp_db`, replays applied migrations, diffs against the live database.
- `explain` — `EXPLAIN FORMAT=JSON` with `access_type=ALL` (full table scan) surfaced as a warning.
- Multi-database orchestration — `MultiWaypoint::connect` auto-detects per-database engine from URL scheme; one config can mix `postgres://` and `mysql://` entries.

**Final pieces — full MySQL parity:**

- `schema::generate_ddl_mysql` emits MySQL-flavored reverse DDL (backticks, `ENGINE=InnoDB`, no `CASCADE`). Dependent constraint/index/trigger diffs are filtered when their parent table is also being dropped in the same batch, since MySQL has no `CASCADE` on `DROP TABLE`.
- `reversal::generate_reversal_db`, `store_reversal_db`, `get_reversal_db` are the new dialect-aware entries. PG legacy fns retained for back-compat. MySQL migrate now captures before-snapshots and stores reverse DDL automatically; MySQL undo falls back to it when no `U{ver}__*.sql` is present.
- The `--no-default-features --features mysql` build (mysql-only, no PostgreSQL deps compiled in) is now green: clippy `-D warnings` clean, 155 unit tests pass.

See `CLAUDE.md` for the full per-command status table — every command now works on both engines.

**MySQL caution mitigations:**

Closed the four production cautions previously documented in `docs/ENGINES.md`:

- **Smarter safety verdicts.** `safety` now detects `@@version` and applies INSTANT-eligibility rules per operation: 8.0.29+ ADD COLUMN (nullable, or NOT NULL with DEFAULT) and DROP COLUMN downgrade to Safe; ALTER COLUMN TYPE and ADD COLUMN NOT NULL (no default) stay conservative. When version detection fails the fallback is conservative and logged as `log::warn!`.
- **Refresh-on-demand row stats.** `[safety] refresh_stats_mysql = true` runs `ANALYZE TABLE` before reading `table_rows`, for accurate size classification when callers care more about correctness than the brief metadata lock.
- **DEFINER stripping on snapshot.** `[snapshots] strip_definer_mysql = true` (default `true`) scrubs `DEFINER=...` and the redundant `SQL SECURITY DEFINER` from view DDL when capturing snapshots. View restore now works across accounts without needing `SUPER`/`SET_USER_ID`. Regex handles backtick-quoted, single-quoted, and `CURRENT_USER` / `CURRENT_USER()` forms.
- **Cross-database view warnings in `simulate`.** `SimulationReport` now exposes a `warnings: Vec<String>` field. Replication failures during simulation (commonly: a view referencing another database not replicated into the temp DB) are surfaced as user-visible warnings — yellow `!` lines in the CLI, `warnings[]` in JSON. Cross-database refs are detected via a manual identifier walker and named explicitly in the warning.

**Internal reorganization (no behaviour change):**

- Engine-specific implementations moved under `engines/postgres/` and `engines/mysql/` for `history`, `migrate`, `advisor`, and `safety`. Top-level modules retain shared types and dialect-aware dispatchers; back-compat re-exports keep all public paths working.
- `commands/migrate.rs` shrunk from ~1980 LOC to ~130 LOC (entry points and shared types only).

## [0.3.0] - 2026-02-20

### Added

**New commands (12):**
- `undo` — Undo applied migrations using manual U files or auto-generated reversals
- `lint` — Static analysis of migration SQL (no DB required), 8 rules (E001-E002, W001-W007, I001)
- `changelog` — Auto-generate changelog from migration DDL (no DB required)
- `diff` — Compare schemas between databases, generate migration SQL
- `drift` — Detect manual schema changes that bypassed migrations
- `snapshot` / `restore` — Save and restore schema snapshots as DDL
- `preflight` — Pre-migration health checks (recovery mode, replication lag, locks, connections)
- `check-conflicts` — Detect migration version conflicts between git branches (no DB required)
- `safety` — Analyze migrations for PostgreSQL lock levels, row-count impact, and safety verdicts (Safe/Caution/Danger)
- `advise` — Schema advisory rules (A001-A010) with auto-generated fix SQL
- `simulate` — Run pending migrations in a throwaway schema to verify correctness
- `self-update` — Update waypoint binary from GitHub Releases

**New core modules:**
- `directive.rs` — Parse `-- waypoint:env`, `-- waypoint:depends`, `-- waypoint:require`, `-- waypoint:ensure`, `-- waypoint:safety-override` directives from SQL file headers
- `guard.rs` — Recursive descent expression parser and evaluator with 10 built-in assertion functions (`table_exists`, `column_exists`, `row_count`, `sql`, etc.)
- `reversal.rs` — Auto-generate reverse DDL from before/after schema snapshots, store in history table
- `safety.rs` — Map DDL operations to PostgreSQL lock levels, estimate impact from `pg_stat_user_tables`
- `advisor.rs` — 10 schema advisory rules with severity levels and fix SQL generation
- `sql_parser.rs` — Regex-based DDL extraction (`DdlOperation` enum) and `split_statements()` with dollar-quote, string, and comment awareness
- `schema.rs` — PostgreSQL introspection via `information_schema`/`pg_catalog`, schema diff, DDL generation
- `dependency.rs` — Migration dependency graph with topological sort (Kahn's algorithm)
- `preflight.rs` — Pre-migration health checks against PostgreSQL system catalogs
- `multi.rs` — Multi-database orchestration with dependency ordering

**New features:**
- Undo migrations (`U{version}__desc.sql`) with automatic fallback to auto-generated reversals
- Environment-scoped migrations (`-- waypoint:env dev,staging`)
- Migration dependency ordering (`-- waypoint:depends V1,V3`) with cycle detection
- Guard expressions — preconditions (`-- waypoint:require`) and postconditions (`-- waypoint:ensure`) evaluated against live DB
- Safety analysis with DANGER migration blocking (`--force` to override)
- Batch transaction mode (`--transaction`) — wrap all pending migrations in a single atomic transaction
- Multi-database configuration (`[[databases]]` TOML array) with dependency ordering
- Enhanced dry-run with EXPLAIN output
- TCP keepalive support (`--keepalive`, `keepalive_secs` config)
- Connection retry with exponential backoff and jitter
- Transient error detection and automatic reconnection (max 3 retries)

**New CLI flags:**
- `--environment` — Filter migrations by environment
- `--dependency-ordering` — Enable topological sort for migration ordering
- `--skip-preflight` — Skip pre-flight health checks
- `--database` — Target a specific database in multi-db mode
- `--fail-fast` — Stop on first failure in multi-db mode
- `--force` — Override DANGER safety blocks
- `--simulate` — Run simulation before applying migrations
- `--transaction` — Batch transaction mode
- `--keepalive` — TCP keepalive interval

**New library API methods on `Waypoint`:**
- `undo()`, `lint()`, `changelog()`, `diff()`, `drift()`, `snapshot()`, `restore()`, `explain()`, `preflight()`, `check_conflicts()`, `client()`

**New public re-exports:**
- `ChangelogReport`, `ConflictReport`, `DiffReport`, `DriftReport`, `ExplainReport`, `LintReport`, `SnapshotReport`, `RestoreReport`, `UndoReport`, `UndoTarget`, `MultiWaypoint`, `PreflightReport`

**Infrastructure:**
- `install.sh` shell installer for Linux/macOS
- GitHub Actions release workflow for cross-platform binaries
- `self-update` command with GitHub Releases API
- docs.rs metadata and module-level documentation
- Test fixtures for all command types (`docs/fixtures/`)

### Changed

- Replaced `regex` crate with `regex-lite` (smaller binary, no Unicode tables needed for SQL patterns)
- Replaced `tracing`/`tracing-subscriber` with `log`/`env_logger` (simpler, fewer dependencies)
- Replaced `rand` with `fastrand` (smaller, no crypto overhead for jitter)
- `connect_with_config()` now injects TCP keepalive parameters
- `ResolvedMigration` now includes a `directives` field for parsed `-- waypoint:*` comments
- `MigrationKind` enum now includes `Undo(MigrationVersion)` variant
- `MigrationType` enum now includes `Undo` variant
- `WaypointConfig` now includes `lint`, `snapshots`, `preflight`, `multi_database` fields
- `MigrationSettings` now includes `environment`, `dependency_ordering`, `show_progress` fields
- `CliOverrides` now includes `environment`, `dependency_ordering` fields
- `WaypointError` enum expanded from 12 to 28 variants
- History table now tracks `reversal_sql` column for auto-generated undo SQL
- `migrate` command now runs safety analysis, preflight checks, guard evaluation, and auto-reversal generation
- `rustls` configuration now explicitly selects the `ring` crypto provider

### Performance

- Static `LazyLock` regex compilation for placeholder, batch validation, and migration filename patterns
- Pre-computed uppercase SQL in lint (avoids redundant `to_uppercase()` per rule)
- Zero-allocation case-insensitive comparison in guard tokenizer (`eq_ignore_ascii_case`)
- Borrowed `&str` references in dependency graph and multi-db topological sort (avoids intermediate `String` cloning)
- Parallel schema introspection queries in `schema.rs`

### Fixed

- E-string support in SQL statement splitter (`E'...\'..'`)
- Nested block comment support (`/* outer /* inner */ outer */`)
- Dollar-quote-aware placeholder replacement (skips `${key}` inside `$$...$$`)
- Duplicate migration version detection across files
- Graceful handling of malformed migration filenames (warns and skips instead of aborting)

## [0.2.0] - 2026-02-20

### Added

- README.md with full documentation
- MIT LICENSE
- crates.io metadata for `waypoint-core` and `waypoint-cli`
- Library usage documentation and examples
- GitHub Actions workflow for publishing to crates.io

### Changed

- Version bump from 0.1.0 to 0.2.0

## [0.1.1] - 2026-02-20

### Fixed

- Docker build: touch sources to invalidate cargo cache after dummy build
- Use latest stable Rust image for Docker builds
- Bump Rust Docker image to 1.87 for let-chains support
- Fix TIMESTAMPTZ type mismatch in history table reads
- Fix all clippy warnings and formatting issues

## [0.1.0] - 2026-02-20

### Added

- Initial release
- Core migration engine: versioned (`V`) and repeatable (`R`) migrations
- Flyway-compatible CRC32 checksums and migration naming
- Commands: `migrate`, `info`, `validate`, `repair`, `baseline`, `clean`
- TOML configuration with environment variable overrides
- TLS support via rustls with Mozilla CA bundle
- PostgreSQL advisory locking for concurrent safety
- `${key}` placeholder replacement in SQL
- SQL callback hooks (beforeMigrate, afterMigrate, beforeEachMigrate, afterEachMigrate)
- Docker image with Flyway-compatible environment variables
- CI/CD with GitHub Actions
- Colored table output with `comfy-table`

[0.7.0]: https://github.com/tensorbee/waypoint/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/tensorbee/waypoint/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/tensorbee/waypoint/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/tensorbee/waypoint/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/tensorbee/waypoint/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/tensorbee/waypoint/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/tensorbee/waypoint/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/tensorbee/waypoint/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tensorbee/waypoint/releases/tag/v0.1.0
