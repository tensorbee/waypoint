# Waypoint

[![CI](https://github.com/mantissaman/waypoint/actions/workflows/ci.yml/badge.svg)](https://github.com/mantissaman/waypoint/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/waypoint-core.svg)](https://crates.io/crates/waypoint-core)
[![docs.rs](https://docs.rs/waypoint-core/badge.svg)](https://docs.rs/waypoint-core)
[![Downloads](https://img.shields.io/crates/d/waypoint-core.svg)](https://crates.io/crates/waypoint-core)
[![Docker Hub](https://img.shields.io/docker/v/mantissaman/waypoint?label=docker&sort=semver)](https://hub.docker.com/r/mantissaman/waypoint)
[![Docker Pulls](https://img.shields.io/docker/pulls/mantissaman/waypoint)](https://hub.docker.com/r/mantissaman/waypoint)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Lightweight, Flyway-compatible PostgreSQL migration tool built in Rust.

- **Fast** — single static binary, ~30MB Docker image
- **Flyway-compatible** — same migration naming, CRC32 checksums, JDBC URL support
- **Production-ready** — TLS via rustls, advisory locking, structured logging, retry with backoff
- **Drop-in Docker replacement** — same env vars as Flyway containers

## Install

### Quick install (Linux / macOS)

```bash
curl -sSf https://raw.githubusercontent.com/mantissaman/waypoint/main/install.sh | sh
```

Pin a specific version:

```bash
curl -sSf https://raw.githubusercontent.com/mantissaman/waypoint/main/install.sh | WAYPOINT_VERSION=v0.3.0 sh
```

### From crates.io

```bash
cargo install waypoint-cli
```

### From source

```bash
cargo install --path waypoint-cli
```

### Library

```toml
[dependencies]
waypoint-core = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

### Docker

```bash
docker pull mantissaman/waypoint:latest
```

## Quick Start

```bash
# Apply migrations
waypoint --url "postgres://user:pass@localhost:5432/mydb" \
  --locations db/migrations \
  migrate

# Show migration status
waypoint --url "postgres://user:pass@localhost:5432/mydb" \
  --locations db/migrations \
  info

# Validate applied migrations
waypoint --url "postgres://user:pass@localhost:5432/mydb" \
  --locations db/migrations \
  validate
```

## Migration Files

Place SQL files in your migrations directory:

```
db/migrations/
  V1__Create_users.sql
  V1.1__Add_email_column.sql
  V2__Create_orders.sql
  R__Create_user_view.sql
```

- **Versioned** — `V{version}__{description}.sql` — applied once, in order
- **Repeatable** — `R__{description}.sql` — re-applied when checksum changes

## Commands

| Command | Description |
|---|---|
| `migrate` | Apply pending migrations |
| `info` | Show migration status |
| `validate` | Verify applied migrations match local files |
| `repair` | Remove failed entries, update checksums |
| `baseline` | Mark an existing database at a version |
| `clean` | Drop all objects in managed schemas (requires `--allow-clean`) |

## Configuration

Config is resolved in priority order (highest wins):

1. CLI arguments
2. Environment variables (`WAYPOINT_DATABASE_URL`, etc.)
3. `waypoint.toml` (override path with `-c`)
4. Built-in defaults

### waypoint.toml

```toml
[database]
url = "postgres://user:pass@localhost:5432/mydb"
connect_retries = 5
ssl_mode = "prefer"      # disable | prefer | require
connect_timeout = 30     # seconds
statement_timeout = 0    # seconds, 0 = no limit

[migrations]
locations = ["db/migrations"]
schema = "public"
table = "waypoint_schema_history"
out_of_order = false
validate_on_migrate = true
baseline_version = "1"

[placeholders]
env = "production"
app_name = "myapp"
```

### Environment Variables

| Variable | Description |
|---|---|
| `WAYPOINT_DATABASE_URL` | Database connection URL |
| `WAYPOINT_SSL_MODE` | TLS mode: `disable`, `prefer`, `require` |
| `WAYPOINT_CONNECT_TIMEOUT` | Connection timeout in seconds |
| `WAYPOINT_STATEMENT_TIMEOUT` | Statement timeout in seconds |
| `WAYPOINT_MIGRATIONS_LOCATIONS` | Comma-separated migration paths |
| `WAYPOINT_MIGRATIONS_SCHEMA` | Target schema |
| `WAYPOINT_MIGRATIONS_TABLE` | History table name |
| `WAYPOINT_PLACEHOLDER_{KEY}` | Set placeholder value |

### CLI Flags

```
waypoint [OPTIONS] <COMMAND>

Options:
  -c, --config <PATH>            Config file path
      --url <URL>                Database URL
      --schema <SCHEMA>          Target schema
      --table <TABLE>            History table name
      --locations <PATHS>        Migration locations (comma-separated)
      --connect-retries <N>      Connection retry attempts
      --ssl-mode <MODE>          TLS mode: disable, prefer, require
      --connect-timeout <SECS>   Connection timeout (default: 30)
      --statement-timeout <SECS> Statement timeout (default: 0)
      --out-of-order             Allow out-of-order migrations
      --json                     Output as JSON
      --dry-run                  Preview without applying changes
  -q, --quiet                    Suppress non-essential output
  -v, --verbose                  Enable debug output
```

## Docker

Drop-in replacement for Flyway containers. Same environment variables work:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=host.docker.internal \
  -e DB_NAME=mydb \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=secret \
  mantissaman/waypoint
```

### Docker Compose

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: myapp
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d myapp"]
      interval: 5s
      timeout: 5s
      retries: 5

  migrate:
    image: mantissaman/waypoint:latest
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./db/migrations:/waypoint/sql
    environment:
      DB_HOST: db
      DB_NAME: myapp
      DB_USERNAME: app
      DB_PASSWORD: secret
```

### Migrating from Flyway

```dockerfile
# Before
FROM flyway/flyway
COPY migrations /flyway/sql

# After
FROM mantissaman/waypoint
COPY migrations /waypoint/sql
```

See [DOCKER.md](DOCKER.md) for full Docker documentation.

## Placeholders

Use `${key}` syntax in SQL files:

```sql
CREATE TABLE ${schema}.users (
    id SERIAL PRIMARY KEY,
    env VARCHAR(20) DEFAULT '${env}'
);
```

Set values via config, env vars (`WAYPOINT_PLACEHOLDER_ENV=production`), or CLI.

## Hooks

SQL callback hooks run before/after migrations (Flyway-compatible):

```
db/migrations/
  beforeMigrate.sql
  afterMigrate.sql
  beforeEachMigrate.sql
  afterEachMigrate__Refresh_views.sql
  V1__Create_users.sql
```

Or configure in `waypoint.toml`:

```toml
[hooks]
before_migrate = ["hooks/before.sql"]
after_migrate = ["hooks/after.sql"]
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Validation failed |
| 4 | Database error |
| 5 | Migration or hook failed |
| 6 | Lock error |
| 7 | Clean disabled |

## Using as a Library

Add `waypoint-core` to embed migrations in your Rust application:

```rust
use waypoint_core::config::WaypointConfig;
use waypoint_core::Waypoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config from waypoint.toml + env vars
    let config = WaypointConfig::load(None, None)?;
    let wp = Waypoint::new(config).await?;

    // Apply pending migrations
    let report = wp.migrate(None).await?;
    println!("Applied {} migrations", report.migrations_applied);

    Ok(())
}
```

### Build config programmatically

```rust
use std::path::PathBuf;
use waypoint_core::config::{DatabaseConfig, MigrationSettings, WaypointConfig};
use waypoint_core::Waypoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WaypointConfig {
        database: DatabaseConfig {
            url: Some("postgres://user:pass@localhost:5432/mydb".to_string()),
            ..Default::default()
        },
        migrations: MigrationSettings {
            locations: vec![PathBuf::from("db/migrations")],
            ..Default::default()
        },
        ..Default::default()
    };

    let wp = Waypoint::new(config).await?;

    // Check migration status
    let infos = wp.info().await?;
    for info in &infos {
        println!("{:?} - {} - {}", info.state, info.version.as_deref().unwrap_or("R"), info.description);
    }

    // Apply migrations
    let report = wp.migrate(None).await?;
    println!("Applied {} migrations in {}ms", report.migrations_applied, report.total_time_ms);

    // Validate
    let validation = wp.validate().await?;
    println!("Valid: {}", validation.valid);

    Ok(())
}
```

### Use with an existing connection

```rust
use waypoint_core::config::WaypointConfig;
use waypoint_core::db;
use waypoint_core::Waypoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WaypointConfig::load(None, None)?;
    let client = db::connect("postgres://user:pass@localhost:5432/mydb").await?;

    let wp = Waypoint::with_client(config, client);
    wp.migrate(None).await?;

    Ok(())
}
```

### Available methods

| Method | Returns | Description |
|---|---|---|
| `Waypoint::new(config)` | `Waypoint` | Connect and create instance |
| `Waypoint::with_client(config, client)` | `Waypoint` | Use existing connection |
| `wp.migrate(target)` | `MigrateReport` | Apply pending migrations |
| `wp.info()` | `Vec<MigrationInfo>` | Get migration status |
| `wp.validate()` | `ValidateReport` | Validate applied migrations |
| `wp.repair()` | `RepairReport` | Fix history table |
| `wp.baseline(version, desc)` | `()` | Baseline existing database |
| `wp.clean(allow)` | `Vec<String>` | Drop all managed objects |

## Development

### Prerequisites

- Rust (latest stable)
- PostgreSQL (for integration tests)

### Build & Test

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo test --lib               # Unit tests (no DB required)
cargo clippy -- -D warnings    # Lint
cargo fmt --check              # Format check
```

### Integration Tests

```bash
# Start PostgreSQL, then:
export TEST_DATABASE_URL="postgres://user:pass@localhost:5432/waypoint_test"
cargo test --test integration_test
```

### Project Structure

```
waypoint/
  waypoint-core/     # Library crate — migration logic
    src/
      commands/      # migrate, info, validate, repair, baseline, clean
      config.rs      # Config loading (TOML + env + CLI)
      db.rs          # Connection, TLS, advisory locks
      history.rs     # Schema history table
      migration.rs   # File parsing, scanning
      checksum.rs    # CRC32 checksums (Flyway-compatible)
      placeholder.rs # ${key} replacement
      hooks.rs       # SQL callback hooks
      error.rs       # Error types
      lib.rs         # Public API (Waypoint struct)
    tests/
      integration_test.rs
  waypoint-cli/      # Binary crate — CLI
    src/
      main.rs        # clap CLI, exit codes, JSON output
      output.rs      # Table formatting
    build.rs         # Git hash + build timestamp
```

## License

MIT License

Copyright (c) 2025 mantissaman

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
