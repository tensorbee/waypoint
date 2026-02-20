#!/bin/bash
set -e

SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Default values
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-postgres}"
DB_USERNAME="${DB_USERNAME:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-}"
CONNECT_RETRIES="${CONNECT_RETRIES:-50}"
SSL_MODE="${SSL_MODE:-prefer}"
LOCATIONS="${LOCATIONS:-/waypoint/sql}"

echo "Rebuilding local DB started..."
base="$(date +%s)"

cd "$SCRIPT_HOME"

waypoint \
  --url "jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}?user=${DB_USERNAME}&password=${DB_PASSWORD}" \
  --locations "${LOCATIONS}" \
  --out-of-order \
  --connect-retries "${CONNECT_RETRIES}" \
  --ssl-mode "${SSL_MODE}" \
  migrate

after="$(date +%s)"
elapsed_seconds="$(expr $after - $base)"
echo "Full Database build successfully completed in ${elapsed_seconds} seconds."
