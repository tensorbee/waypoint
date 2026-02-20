# Waypoint Docker Image

Lightweight PostgreSQL migration tool, distributed as a minimal Docker image (~30MB).

## Quick Start

The default entrypoint uses environment variables — a drop-in replacement for Flyway containers:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=mydb \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=secret \
  mantissaman/waypoint
```

## Pull from Docker Hub

```bash
docker pull mantissaman/waypoint:latest
docker pull mantissaman/waypoint:0.1.0    # pinned version
```

## Migrating from Flyway

If you're currently using this Flyway pattern:

```dockerfile
# Old Flyway setup
FROM flyway/flyway
COPY migrations /flyway/sql
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
```

Replace it with:

```dockerfile
# New Waypoint setup
FROM mantissaman/waypoint
COPY migrations /waypoint/sql
```

That's it. The same environment variables work:

| Flyway Env Var | Waypoint Env Var | Default |
|---|---|---|
| `DB_HOST` | `DB_HOST` | `localhost` |
| `DB_PORT` | `DB_PORT` | `5432` |
| `DB_NAME` | `DB_NAME` | `postgres` |
| `DB_USERNAME` / `DB_USER` | `DB_USERNAME` | `postgres` |
| `DB_PASSWORD` / `DB_PWD` | `DB_PASSWORD` | (empty) |
| — | `CONNECT_RETRIES` | `50` |
| — | `SSL_MODE` | `prefer` |
| — | `LOCATIONS` | `/waypoint/sql` |

## Entrypoint Behavior

The `docker-entrypoint.sh` script:

1. Builds a JDBC-style connection URL from environment variables
2. Runs `waypoint migrate` with `--out-of-order` enabled and 50 connect retries
3. Prints elapsed time on completion

This matches the typical Flyway entrypoint behavior. Waypoint natively handles JDBC URLs (`jdbc:postgresql://...`) so the URL format is compatible.

## Docker Compose

### Drop-in Flyway Replacement

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
      DB_PORT: 5432
      DB_NAME: myapp
      DB_USERNAME: app
      DB_PASSWORD: secret
```

### Using Waypoint-Native Config

```yaml
  migrate:
    image: mantissaman/waypoint:latest
    depends_on:
      db:
        condition: service_healthy
    volumes:
      - ./db/migrations:/waypoint/sql
    environment:
      WAYPOINT_DATABASE_URL: "postgres://app:secret@db:5432/myapp"
    # Override entrypoint to use waypoint CLI directly
    entrypoint: ["waypoint"]
    command: ["--locations", "/waypoint/sql", "migrate"]
```

## Advanced Usage

Override the entrypoint to use the waypoint CLI directly:

```bash
# Show help
docker run --rm --entrypoint waypoint mantissaman/waypoint --help

# Show migration status
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  mantissaman/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  info

# Dry-run
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  mantissaman/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  --dry-run migrate

# JSON output
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  mantissaman/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  --json info

# Validate
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  mantissaman/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  validate

# Repair
docker run --rm --entrypoint waypoint \
  -v ./db/migrations:/waypoint/sql \
  mantissaman/waypoint \
  --url "postgres://user:pass@host:5432/mydb" \
  --locations /waypoint/sql \
  repair
```

## TLS Connections

The image includes Mozilla CA certificates for TLS. Control via `SSL_MODE`:

```bash
docker run --rm \
  -v ./db/migrations:/waypoint/sql \
  -e DB_HOST=my-rds-instance.amazonaws.com \
  -e DB_NAME=mydb \
  -e DB_USERNAME=admin \
  -e DB_PASSWORD=secret \
  -e SSL_MODE=require \
  mantissaman/waypoint
```

## CI/CD Examples

### GitHub Actions

```yaml
- name: Run migrations
  run: |
    docker run --rm \
      -v ${{ github.workspace }}/db/migrations:/waypoint/sql \
      -e DB_HOST=${{ secrets.DB_HOST }} \
      -e DB_NAME=${{ secrets.DB_NAME }} \
      -e DB_USERNAME=${{ secrets.DB_USERNAME }} \
      -e DB_PASSWORD=${{ secrets.DB_PASSWORD }} \
      -e SSL_MODE=require \
      mantissaman/waypoint:0.1.0
```

### GitLab CI

```yaml
migrate:
  image:
    name: mantissaman/waypoint:0.1.0
  variables:
    DB_HOST: $DB_HOST
    DB_NAME: $DB_NAME
    DB_USERNAME: $DB_USERNAME
    DB_PASSWORD: $DB_PASSWORD
    SSL_MODE: require
  only:
    - main
```

## Building the Image Locally

```bash
docker build -t mantissaman/waypoint:latest .
docker build -t mantissaman/waypoint:0.1.0 .
```

## Publishing to Docker Hub

```bash
# Log in
docker login

# Build and tag
docker build -t mantissaman/waypoint:latest -t mantissaman/waypoint:0.1.0 .

# Push
docker push mantissaman/waypoint:latest
docker push mantissaman/waypoint:0.1.0
```

### Multi-Architecture Build (amd64 + arm64)

```bash
# Create builder (one-time)
docker buildx create --name waypoint-builder --use

# Build and push for both architectures
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t mantissaman/waypoint:latest \
  -t mantissaman/waypoint:0.1.0 \
  --push .
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
