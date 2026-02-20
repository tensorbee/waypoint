# Stage 1: Build
FROM rust:1.87-bookworm AS builder

WORKDIR /usr/src/waypoint

# Install git for build.rs (captures commit hash)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY waypoint-core/Cargo.toml waypoint-core/Cargo.toml
COPY waypoint-cli/Cargo.toml waypoint-cli/Cargo.toml
COPY waypoint-cli/build.rs waypoint-cli/build.rs

# Create dummy source files to build dependencies
RUN mkdir -p waypoint-core/src waypoint-cli/src && \
    echo "pub fn lib() {}" > waypoint-core/src/lib.rs && \
    echo "fn main() {}" > waypoint-cli/src/main.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf waypoint-core/src waypoint-cli/src

# Copy actual source
COPY waypoint-core/ waypoint-core/
COPY waypoint-cli/ waypoint-cli/

# Copy .git if present (for build metadata), ignore failure
COPY .gi[t] .git

# Build release binary
RUN cargo build --release --bin waypoint

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/waypoint/target/release/waypoint /usr/local/bin/waypoint

# Match Flyway convention: migrations go in /waypoint/sql
RUN mkdir -p /waypoint/sql
WORKDIR /waypoint

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
