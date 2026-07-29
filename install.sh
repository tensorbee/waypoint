#!/bin/sh
# Waypoint installer
# Usage: curl -sSf https://raw.githubusercontent.com/tensorbee/waypoint/main/install.sh | sh

set -e

REPO="tensorbee/waypoint"
BINARY="waypoint"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Linux)  OS="linux" ;;
    Darwin) OS="macos" ;;
    *)      echo "Error: Unsupported OS: $OS"; exit 1 ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64)  ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)             echo "Error: Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Get latest version from GitHub
if [ -z "$WAYPOINT_VERSION" ]; then
    VERSION="$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
    if [ -z "$VERSION" ]; then
        echo "Error: Could not determine latest version"
        exit 1
    fi
else
    VERSION="$WAYPOINT_VERSION"
fi

echo "Installing waypoint ${VERSION} (${OS}/${ARCH})..."

# Download
TARBALL="${BINARY}-${VERSION}-${OS}-${ARCH}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${TARBALL}"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "Downloading ${URL}..."
curl -sSfL "$URL" -o "${TMPDIR}/${TARBALL}"

# Verify against the release SHA256SUMS file. Set WAYPOINT_SKIP_CHECKSUM=1 to
# bypass (e.g. for a release published before checksums were introduced).
if [ "${WAYPOINT_SKIP_CHECKSUM:-0}" != "1" ]; then
    SUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/SHA256SUMS"
    if curl -sSfL "$SUMS_URL" -o "${TMPDIR}/SHA256SUMS" 2>/dev/null; then
        if command -v sha256sum >/dev/null 2>&1; then
            ACTUAL="$(sha256sum "${TMPDIR}/${TARBALL}" | cut -d' ' -f1)"
        elif command -v shasum >/dev/null 2>&1; then
            ACTUAL="$(shasum -a 256 "${TMPDIR}/${TARBALL}" | cut -d' ' -f1)"
        else
            echo "Warning: no sha256sum/shasum available, skipping checksum verification"
            ACTUAL=""
        fi

        if [ -n "$ACTUAL" ]; then
            EXPECTED="$(grep " [ *]*${TARBALL}\$" "${TMPDIR}/SHA256SUMS" | cut -d' ' -f1)"
            if [ -z "$EXPECTED" ]; then
                echo "Error: ${TARBALL} is not listed in SHA256SUMS"
                exit 1
            fi
            if [ "$ACTUAL" != "$EXPECTED" ]; then
                echo "Error: checksum mismatch for ${TARBALL}"
                echo "  expected: ${EXPECTED}"
                echo "  actual:   ${ACTUAL}"
                exit 1
            fi
            echo "Checksum verified."
        fi
    else
        echo "Warning: no SHA256SUMS published for ${VERSION}, skipping verification"
    fi
fi

# Extract
tar -xzf "${TMPDIR}/${TARBALL}" -C "$TMPDIR"

# Install
if [ -w "$INSTALL_DIR" ]; then
    mv "${TMPDIR}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
else
    echo "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "${TMPDIR}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
fi

chmod +x "${INSTALL_DIR}/${BINARY}"

echo "waypoint installed to ${INSTALL_DIR}/${BINARY}"
"${INSTALL_DIR}/${BINARY}" --version
