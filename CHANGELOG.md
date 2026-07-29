# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.5.0]: https://github.com/tensorbee/waypoint/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/tensorbee/waypoint/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/tensorbee/waypoint/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/tensorbee/waypoint/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/tensorbee/waypoint/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tensorbee/waypoint/releases/tag/v0.1.0
