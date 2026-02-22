#!/usr/bin/env bash
set -euo pipefail

# bump-version.sh — Bump version, update all references, commit, and push a git tag.
#
# Usage:
#   ./bump-version.sh <new-version>
#   ./bump-version.sh 0.4.0
#   ./bump-version.sh 0.4.0 --dry-run

VERSION="${1:-}"
DRY_RUN="${2:-}"

if [ -z "$VERSION" ]; then
  echo "Usage: ./bump-version.sh <new-version> [--dry-run]"
  echo "Example: ./bump-version.sh 0.4.0"
  exit 1
fi

# Strip leading 'v' if provided (e.g., v0.4.0 -> 0.4.0)
VERSION="${VERSION#v}"

# Validate semver format
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$'; then
  echo "Error: '$VERSION' is not a valid semver version (expected X.Y.Z)"
  exit 1
fi

# Derive the major.minor for library dependency references (e.g., 0.4.0 -> 0.4)
MAJOR_MINOR=$(echo "$VERSION" | sed -E 's/^([0-9]+\.[0-9]+)\..*/\1/')

# Get current version from waypoint-core/Cargo.toml
CURRENT=$(grep '^version' waypoint-core/Cargo.toml | head -1 | sed 's/version = "//;s/"//')

if [ "$VERSION" = "$CURRENT" ]; then
  echo "Error: version is already $CURRENT"
  exit 1
fi

echo "Bumping version: $CURRENT -> $VERSION"
echo ""

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Error: working tree has uncommitted changes. Commit or stash them first."
  exit 1
fi

if [ "$DRY_RUN" = "--dry-run" ]; then
  echo "[dry-run] Would update the following files:"
  echo "  - waypoint-core/Cargo.toml (package version)"
  echo "  - waypoint-cli/Cargo.toml (package version + waypoint-core dependency)"
  echo "  - README.md (install pin version, library dependency version)"
  echo ""
  echo "[dry-run] Would run: cargo check (to update Cargo.lock)"
  echo "[dry-run] Would commit: Release v$VERSION"
  echo "[dry-run] Would tag: v$VERSION"
  echo "[dry-run] Would push: main + tag v$VERSION"
  exit 0
fi

# --- Update waypoint-core/Cargo.toml ---
echo "Updating waypoint-core/Cargo.toml..."
sed -i '' -E "0,/^version = \".*\"/s/^version = \".*\"/version = \"$VERSION\"/" waypoint-core/Cargo.toml

# --- Update waypoint-cli/Cargo.toml (package version) ---
echo "Updating waypoint-cli/Cargo.toml..."
sed -i '' -E "0,/^version = \".*\"/s/^version = \".*\"/version = \"$VERSION\"/" waypoint-cli/Cargo.toml

# --- Update waypoint-cli/Cargo.toml (waypoint-core dependency) ---
sed -i '' -E "s/waypoint-core = \{ version = \"[^\"]*\"/waypoint-core = { version = \"$VERSION\"/" waypoint-cli/Cargo.toml

# --- Update README.md ---
echo "Updating README.md..."

# Update install pin version: WAYPOINT_VERSION=v0.X.Y
sed -i '' -E "s/WAYPOINT_VERSION=v[0-9]+\.[0-9]+\.[0-9]+/WAYPOINT_VERSION=v$VERSION/" README.md

# Update library dependency version: waypoint-core = "0.X"
sed -i '' -E "s/waypoint-core = \"[0-9]+\.[0-9]+\"/waypoint-core = \"$MAJOR_MINOR\"/" README.md

# --- Update Cargo.lock ---
echo "Running cargo check to update Cargo.lock..."
cargo check --quiet 2>/dev/null || cargo check

# --- Verify ---
echo ""
echo "Updated versions:"
grep '^version' waypoint-core/Cargo.toml | head -1 | sed "s/^/  waypoint-core\/Cargo.toml: /"
grep '^version' waypoint-cli/Cargo.toml | head -1 | sed "s/^/  waypoint-cli\/Cargo.toml:  /"
grep 'waypoint-core = ' waypoint-cli/Cargo.toml | sed "s/^/  waypoint-cli dependency:   /"
grep 'WAYPOINT_VERSION' README.md | sed "s/^.*\(WAYPOINT_VERSION\)/  README.md install pin:     \1/"
grep 'waypoint-core = "' README.md | sed 's/^/  README.md library dep:     /'

# --- Git commit and tag ---
echo ""
echo "Committing and tagging..."
git add waypoint-core/Cargo.toml waypoint-cli/Cargo.toml Cargo.lock README.md
git commit -m "Release v$VERSION"
git tag -a "v$VERSION" -m "Release v$VERSION"

echo ""
echo "Pushing to origin..."
git push origin main
git push origin "v$VERSION"

echo ""
echo "Done! Released v$VERSION"
echo "  - GitHub Actions will create the release, publish to crates.io, and push Docker image."
