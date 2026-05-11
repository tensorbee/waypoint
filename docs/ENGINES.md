# PostgreSQL vs MySQL: feature parity and cautions

Waypoint supports two database engines. This document is the canonical
reference for what behaves identically across both, where they diverge, and
the production cautions that come with each divergence.

For a one-line "does X work on Y" lookup, see the per-command status table in
[`CLAUDE.md`](../CLAUDE.md). This document goes deeper — when the cells say
"Yes/Yes", what subtle behavior differences should you still know about?

## Quick reference

| Area | PostgreSQL 12+ | MySQL 8.0+ |
|---|---|---|
| Cargo feature | `postgres` (default) | `mysql` (opt-in) |
| URL scheme | `postgres://` / `postgresql://` | `mysql://` |
| Connection lib | `tokio-postgres` + `rustls` | `mysql_async` + `rustls-tls` |
| Advisory lock primitive | `pg_advisory_lock(i64)` | `GET_LOCK('<name>', timeout)` |
| Identifier quoting | `"name"` (double quotes) | `` `name` `` (backticks) |
| Schema vs database | namespace within a DB | "schema" === database |
| Default `schema` config | `"public"` | auto-falls back to `DATABASE()` |
| History table TZ column | `TIMESTAMPTZ` | `TIMESTAMP` (UTC by convention) |
| Statement timeout | `SET statement_timeout = '<n>s'` | `SET SESSION MAX_EXECUTION_TIME = <n_ms>` (SELECT only) |
| Replication lag unit | bytes (WAL) — `max_replication_lag_mb` | seconds — `max_replication_lag_secs` |

## Commands

Every Waypoint command is supported on both engines. The differences below
are behavioral, not "does this command exist".

| Command | Postgres | MySQL | Caveats |
|---|---|---|---|
| `migrate` | ✅ | ✅ | `batch_transaction = true` errors on MySQL (no transactional DDL) |
| `info` | ✅ | ✅ | Same output shape both engines |
| `validate` | ✅ | ✅ | CRC32 checksums byte-identical across engines (Flyway-compat) |
| `repair` | ✅ | ✅ | Drops failed rows + updates checksums on both |
| `baseline` | ✅ | ✅ | Refuses if history table has any entries |
| `clean` | ✅ | ✅ | PG: `DROP SCHEMA CASCADE`. MySQL: drops views→tables→routines→events with FK checks off |
| `undo` | ✅ | ✅ | Both: manual U-file > auto-reversal fallback (if `[reversals] enabled`) |
| `snapshot` | ✅ | ✅ | PG: full introspection. MySQL: `SHOW CREATE TABLE` / `SHOW CREATE VIEW` |
| `restore` | ✅ | ✅ | MySQL: keeps `FOREIGN_KEY_CHECKS = 0` across the entire apply so forward refs resolve |
| `simulate` | ✅ | ✅ | PG: temp schema. MySQL: temp database. Views are replayed with `\`source_db\`.` qualifiers stripped |
| `preflight` | ✅ | ✅ | Engine-specific checks — see [Preflight checks](#preflight-checks) |
| `safety` | ✅ | ✅ | Verdict semantics differ — see [Safety analysis](#safety-analysis) |
| `advise` | ✅ | ✅ | Different rule sets — `A001–A010` on PG, `M001–M005` on MySQL |
| `guards` | ✅ | ✅ | `enum_exists()` unsupported on MySQL (no enum *type* concept) |
| `diff` | ✅ | ✅ | Structured `diffs[]` is identical-shape; `generated_sql` flavored per engine |
| `drift` | ✅ | ✅ | PG: temp schema replay. MySQL: temp database replay with `USE` |
| `explain` | ✅ | ✅ | PG: `EXPLAIN (FORMAT TEXT)`. MySQL: `EXPLAIN FORMAT=JSON` |
| `lint` / `changelog` / `check-conflicts` | ✅ | ✅ | No-DB; identical behavior |
| Multi-database orchestration | ✅ | ✅ | Mixed-engine configs supported (one config with both `postgres://` and `mysql://`) |

## Hard differences (production cautions)

### 1. MySQL DDL is not transactional

**The single most important caveat.** On PG, every migration runs in a
`BEGIN`/`COMMIT` block — a failure anywhere mid-migration rolls back the
whole thing, and the history table row is rolled back along with it. On
MySQL, DDL statements like `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
*auto-commit* and cannot be rolled back.

**Consequences:**
- `batch_transaction = true` returns a hard error on MySQL — there is no
  multi-migration atomic apply. Use multiple smaller migrations instead.
- A migration that contains both DDL and DML can leave you partially
  applied. If statement 3 of 5 fails, statements 1–2 are committed and
  statements 4–5 didn't run.
- `ensure` guards (`-- waypoint:ensure …`) execute *after* a successful
  apply on MySQL. They can fail and surface as `WaypointError::GuardFailed`,
  but the migration has already auto-committed. Treat MySQL `ensure` as
  *verify-after* rather than *rollback-if-false*. If you need atomicity,
  keep the postcondition simple and inline it as a SQL guard inside the
  migration itself.

### 2. MySQL has no `CASCADE` on `DROP TABLE`

PG's `DROP TABLE … CASCADE` automatically drops dependent indexes,
constraints, and triggers. MySQL has no `CASCADE` — dependent objects must
be removed explicitly or with the table itself.

**Waypoint's handling:** the MySQL `generate_ddl_mysql` (used by
auto-reversal) pre-collects the set of tables being dropped in a batch and
filters dependent diffs (`ColumnDropped`, `ConstraintDropped`, `IndexDropped`,
`TriggerDropped`, `ColumnAltered`, plus the corresponding `*Added` for the
dropped table) so dependent ALTERs aren't emitted after the parent table is
gone. If you write SQL manually that drops a table and a dependent constraint
in the same migration, order matters — drop the constraint first or use
`DROP TABLE` alone.

### 3. MySQL `enum_exists()` guard is rejected

MySQL has no enum *type* — `ENUM` is a column-type modifier. The
`enum_exists("status_type")` builtin returns a `ConfigError` on MySQL with a
clear message redirecting to `column_type("table", "col", "enum")`. If your
migrations target both engines, gate enum-related guards behind
`-- waypoint:env` or write them in terms of `column_type`.

### 4. MySQL safety verdicts are intentionally pessimistic

MySQL 8.0 supports `ALGORITHM=INSTANT`, `INPLACE`, and `COPY` for most
ALTER TABLE operations — but the engine chooses at execution time which to
use, and the same DDL can fall back from INSTANT to INPLACE to COPY
depending on table contents and history. Waypoint can't predict this
statically, so the safety analyzer maps **ALTER TABLE → worst-case
`AccessExclusiveLock`**. Verdicts on MySQL are conservative: an ADD COLUMN
that would actually use INSTANT may still show CAUTION/DANGER.

**Practical:** treat MySQL safety as a "review this if it touches a large
table" prompt rather than a precise lock-prediction. For production-scale
DDL, prefer `gh-ost` / `pt-online-schema-change` regardless of what Waypoint
says.

### 5. `current_user` format differs

PG: `postgres`, `app_user`, etc. — just the user name.
MySQL: `root@localhost`, `app_user@%` — `<user>@<host>`.

Waypoint stores whichever the engine returns in
`waypoint_schema_history.installed_by`. Tooling that parses this column
should handle both formats. Override with `[migrations] installed_by = "ci"`
in `waypoint.toml` for a consistent identifier across environments.

### 6. Approximate row counts on MySQL

PG's `pg_stat_user_tables.n_live_tup` is a maintained statistic, updated by
`ANALYZE` and autovacuum. MySQL's `information_schema.tables.table_rows` is
an *engine-reported estimate* — for InnoDB it can be off by an order of
magnitude on a busy table, and is sometimes `NULL` for empty tables.

Waypoint uses these counts for safety size classification (`Small`,
`Medium`, `Large`, `Huge`). A migration classified `Caution` on MySQL today
might be `Danger` after the next stats refresh. For migrations where size
classification matters, run `ANALYZE TABLE <name>` first.

### 7. Replication lag is configured differently

| Engine | Config knob | Unit | Source |
|---|---|---|---|
| PG | `[preflight] max_replication_lag_mb` | megabytes (WAL bytes) | `pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)` |
| MySQL | `[preflight] max_replication_lag_secs` | seconds | `SHOW REPLICA STATUS` → `Seconds_Behind_Source` |

The two knobs measure different things, so the same migration might pass
preflight on PG (small WAL backlog) and fail on MySQL (replica seconds
behind because it's hand-applying a long-running statement). Both fields
have defaults (100 MB / 30 secs); both can be set in the same
`waypoint.toml` and the relevant one is used per engine.

### 8. Snapshot/restore views — `DEFINER` clauses can fail

`SHOW CREATE VIEW` on MySQL emits the view with its original `DEFINER` user
(e.g. `DEFINER='root'@'localhost'`). If the user who runs `restore` doesn't
have the privilege to create views as that definer, the restore of that
view fails (recorded as a warning, not fatal — the rest of the snapshot
still applies).

**Mitigation:** run snapshots and restores as a user with `SUPER`/
`SET_USER_ID` privileges, or post-process the snapshot SQL to strip the
DEFINER clause if cross-account restores are part of your workflow.

### 9. Cross-database FK references in views

The MySQL `simulate` command replicates source tables and views into a
throwaway database. View DDL contains qualified column refs like
`SELECT \`source_db\`.\`t\`.\`c\` FROM \`source_db\`.\`t\``. Waypoint
rewrites occurrences of `` `source_db`. `` to empty so refs resolve against
the temp database (via `USE temp_db`).

**Limitation:** views that legitimately reference *other* databases
(`\`shared_db\`.`) can't be replicated into the temp DB — the original DB
isn't recreated there. Those views' simulation steps fail with a debug-log
warning; if your migrations reference such views via `SELECT`, simulate
won't catch errors against them.

## Preflight checks

The check names and numbers match across engines, but the underlying signal
differs.

| Check | PG source | MySQL source |
|---|---|---|
| Read-only / recovery mode | `pg_is_in_recovery()` | `@@read_only`, `@@super_read_only` |
| Active connections | `pg_stat_activity` vs `pg_settings.max_connections` | `Threads_connected` (perf_schema) vs `@@max_connections` |
| Long-running queries | `pg_stat_activity` where `now() - query_start > threshold` | `information_schema.PROCESSLIST` where `COMMAND <> 'Sleep' AND TIME > threshold` |
| Replication lag | `pg_wal_lsn_diff` bytes | `Seconds_Behind_Source` from `SHOW REPLICA STATUS` |
| Database size | `pg_database_size(current_database())` | `SUM(data_length + index_length)` from `information_schema.TABLES` |
| Lock contention | `pg_locks` where `NOT granted` | `performance_schema.metadata_locks` where `LOCK_STATUS = 'PENDING'` |

## Safety analysis

Both engines produce the same `SafetyReport` shape (`Safe` / `Caution` /
`Danger` verdicts, suggestions, row count estimates). The mapping from DDL
to lock level differs.

### Lock-level mapping

| Operation | PG lock | MySQL approximate | Notes |
|---|---|---|---|
| `CREATE TABLE` | None (new object) | None | Identical |
| `DROP TABLE` | AccessExclusiveLock | AccessExclusiveLock | Identical (effectively blocking on both) |
| `ALTER TABLE ADD COLUMN` | AccessExclusiveLock | AccessExclusiveLock (worst case) | MySQL may upgrade to INSTANT (free) or INPLACE (brief MDL); worst-case is COPY (full rewrite) |
| `ALTER TABLE DROP COLUMN` | AccessExclusiveLock | AccessExclusiveLock (worst case) | Same caveat |
| `ALTER TABLE ALTER COLUMN TYPE` | AccessExclusiveLock | AccessExclusiveLock | Both engines effectively do a full table rewrite |
| `CREATE INDEX (default)` | ShareLock | ShareLock | PG: blocks writes. MySQL InnoDB: INPLACE, reads OK, brief metadata lock |
| `CREATE INDEX CONCURRENTLY` | ShareUpdateExclusiveLock | — | No MySQL equivalent — closest is `gh-ost` or `pt-osc` |
| `DROP INDEX` | AccessExclusiveLock | ShareLock | MySQL: INPLACE by default |
| `TRUNCATE TABLE` | AccessExclusiveLock | AccessExclusiveLock | Both: drops the file/segment on InnoDB; irreversible |

### Advisor rules

Different rule numbering reflects different applicability.

| PG rule | What it checks | MySQL equivalent |
|---|---|---|
| A001 | FK column without index | M001 (same idea, via `STATISTICS`) |
| A002 | Unused indexes (`pg_stat_user_indexes`) | — (MySQL has no equivalent maintained counter) |
| A003 | `TIMESTAMP` without timezone | — (MySQL `TIMESTAMP` is UTC by default, no `WITH/WITHOUT TIME ZONE` choice) |
| A004 | Table without primary key | M002 (same idea) |
| A005 | Nullable column with no NULLs | — |
| A006 | `VARCHAR` without length limit | — (MySQL requires a length) |
| A007 | Duplicate indexes | M005 (same idea, via `STATISTICS` grouped by column sequence) |
| A008 | Seq scan on large table | — (MySQL has different stats surface) |
| A009 | Enum with > 20 values | — (MySQL ENUM is a column-type modifier, surfaces differently) |
| A010 | Orphaned sequences | — (MySQL has no sequences) |
| — | | M003: non-utf8mb4 charset |
| — | | M004: non-InnoDB storage engine |

## Building for one engine vs both

```toml
# Default (PostgreSQL only)
[dependencies]
waypoint-core = "0.3"

# MySQL only
[dependencies]
waypoint-core = { version = "0.3", default-features = false, features = ["mysql"] }

# Both engines (mixed multi-database configs)
[dependencies]
waypoint-core = { version = "0.3", features = ["mysql"] }
```

CLI install:
```bash
# PostgreSQL only (default)
cargo install waypoint-cli

# MySQL only
cargo install waypoint-cli --no-default-features --features mysql

# Both
cargo install waypoint-cli --features mysql
```

The crate uses Cargo features (not a workspace split), so all three
configurations compile and test green; only the engine code you opt into is
linked. PG-only installs do not pull in `mysql_async` and vice-versa.

## When to choose which

| Situation | Recommendation |
|---|---|
| New project, no engine constraint | **PostgreSQL.** Waypoint's safety/advisor/diff/drift on PG are more precise (real lock-level mapping, real row counts, transactional DDL means migrations are atomic). |
| Existing MySQL deployment | **MySQL.** Every command works; just understand the cautions above (non-transactional DDL, pessimistic safety verdicts, approximate row counts). |
| Multi-tenant infra with both engines | **Both** via the `mysql` feature on top of `postgres`. Use a single `waypoint.toml` with mixed `[[databases]]` entries; multi-db orchestration handles the dispatch. |
| Tight runtime/binary-size budget | **One engine only** via `--no-default-features --features <engine>`. Each engine drops ~5–8 MB from the binary. |
