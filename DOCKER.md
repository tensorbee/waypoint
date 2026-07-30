# Waypoint Docker Image

Lightweight SQL migration tool, distributed as a minimal Docker image (~30MB).
Supports PostgreSQL 12+ and MySQL 8.0+ (engine auto-detected from the
connection URL scheme). Drop-in replacement for Flyway containers.

## Quick Start

PostgreSQL:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=mydb \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=secret \
  tensorbeeio/waypoint
```

MySQL 8.0+ (pass the full URL so the `mysql://` scheme triggers the MySQL backend):

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e WAYPOINT_DATABASE_URL="mysql://user:pass@host.docker.internal:3306/mydb" \
  tensorbeeio/waypoint
```

## Pull from Docker Hub

```bash
docker pull tensorbeeio/waypoint:latest
docker pull tensorbeeio/waypoint:0.1.0    # pinned version
```

## Migrating from Flyway

Replace your Flyway setup:

```dockerfile
# Before
FROM flyway/flyway
COPY migrations /flyway/sql

# After
FROM tensorbeeio/waypoint
COPY migrations /waypoint/sql
```

The same environment variables work:

| Env Var | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | Database host |
| `DB_PORT` | `5432` | Database port |
| `DB_NAME` | `postgres` | Database name |
| `DB_USERNAME` | `postgres` | Database user |
| `DB_PASSWORD` | (empty) | Database password |
| `CONNECT_RETRIES` | `50` | Connection retry attempts |
| `SSL_MODE` | `prefer` | TLS mode: `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `SSL_ROOT_CERT` | (empty) | Path to a CA PEM file inside the container; replaces the built-in trust store |
| `LOCATIONS` | `/waypoint/sql` | Migration file directory |

## Entrypoint Behavior

The `docker-entrypoint.sh` script:

1. Builds a JDBC-style connection URL from environment variables
2. Runs `waypoint migrate` with `--out-of-order` enabled
3. Retries connection up to 50 times (configurable)
4. Prints elapsed time on completion

## Docker Compose

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
    image: tensorbeeio/waypoint:latest
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

## Advanced Usage

Override the entrypoint to use the CLI directly:

```bash
# Show help
docker run --rm --entrypoint waypoint tensorbeeio/waypoint --help

# Migration status
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  info

# Dry-run
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  --dry-run migrate

# JSON output
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  --json info

# Validate / Repair
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  tensorbeeio/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  validate
```

## TLS Connections

The image includes the Mozilla CA bundle. `SSL_MODE` takes libpq's values and
libpq's meanings:

| Mode | TLS | Chain verified | Hostname verified |
|---|---|---|---|
| `disable` | no | — | — |
| `prefer` (default) | opportunistic, may end up plaintext | no | no |
| `require` | mandatory | no | no |
| `verify-ca` | mandatory | yes | no |
| `verify-full` | mandatory | yes | yes |

Note that `require` encrypts but does **not** authenticate the server. Use
`verify-full` when you need the server's identity checked.

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=my-rds-instance.amazonaws.com \
  -e DB_NAME=mydb \
  -e DB_USERNAME=admin \
  -e DB_PASSWORD=secret \
  -e SSL_MODE=verify-full \
  tensorbeeio/waypoint
```

### Private certificate authority

For a server whose certificate is issued by an internal CA, mount the CA file
and point `SSL_ROOT_CERT` at it. As with libpq's `sslrootcert`, it **replaces**
the built-in trust store rather than adding to it:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -v /etc/ssl/certs/internal-ca.pem:/ca.pem:ro \
  -e DB_HOST=db.internal \
  -e DB_NAME=mydb \
  -e DB_USERNAME=admin \
  -e DB_PASSWORD=secret \
  -e SSL_MODE=verify-full \
  -e SSL_ROOT_CERT=/ca.pem \
  tensorbeeio/waypoint
```

If the file is missing or contains no certificates the run fails rather than
silently falling back to the public CA bundle.

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
