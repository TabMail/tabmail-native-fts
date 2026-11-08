#!/bin/bash
# TabMail Native FTS Helper - Install Script
# Installs the Rust native host binary to user directory (no sudo required)
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/TabMail/tabmail-native-fts/main/scripts/install.sh | bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}TabMail Native FTS Helper - Installer${NC}"
echo ""

# Detect OS + arch
OS="unknown"
ARCH="unknown"
UNAME_M="$(uname -m || true)"

if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
    if [[ "$UNAME_M" == "arm64" ]]; then
        ARCH="arm64"
    else
        ARCH="x86_64"
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="linux"
    if [[ "$UNAME_M" == "x86_64" ]]; then
        ARCH="x86_64"
    else
        echo -e "${RED}Error: Unsupported architecture: $UNAME_M${NC}"
        exit 1
    fi
else
    echo -e "${RED}Error: Unsupported operating system: $OSTYPE${NC}"
    echo "This script supports macOS and Linux only."
    echo "For Windows, use the PowerShell installer."
    exit 1
fi

# macOS ships a single universal binary (arm64 + x86_64 combined) published
# under "macos-universal"; Linux is arch-specific. This must match the CDN
# layout produced by the release scripts (and the add-on's own platform key
# in tabmail-thunderbird fts/nativeEngine.js getNativeFtsPlatformKey()).
if [[ "$OS" == "macos" ]]; then
    PLATFORM_KEY="macos-universal"
else
    PLATFORM_KEY="${OS}-${ARCH}"
fi
echo -e "Detected platform: ${GREEN}${OS}-${ARCH}${NC} (artifact: ${PLATFORM_KEY})"

# Set paths based on OS
if [[ "$OS" == "macos" ]]; then
    INSTALL_DIR="$HOME/Library/Application Support/TabMail/native"
    # Thunderbird on macOS may read either of these user-level manifest directories.
    # We write BOTH to avoid ambiguous installs.
    MANIFEST_DIR="$HOME/Library/Application Support/Mozilla/NativeMessagingHosts"
    MANIFEST_DIR_ALT="$HOME/Library/Mozilla/NativeMessagingHosts"
elif [[ "$OS" == "linux" ]]; then
    INSTALL_DIR="$HOME/.local/share/tabmail/native"
    MANIFEST_DIR="$HOME/.mozilla/native-messaging-hosts"
    MANIFEST_DIR_ALT=""
fi

# Platform-first CDN layout (independent per platform):
#   https://cdn.tabmail.ai/releases/${PLATFORM_KEY}/update-manifest.json
#   https://cdn.tabmail.ai/releases/${PLATFORM_KEY}/fts_helper-<version>
#   https://cdn.tabmail.ai/releases/${PLATFORM_KEY}/fts_helper-<version>-THIRD_PARTY_NOTICES.txt
# The binary and its notices are taken from the release the update manifest names. A release
# publishes the manifest last, so both files of that version are on the CDN; the two "-latest"
# copies are replaced one at a time and can briefly belong to different releases.
CDN_URL="https://cdn.tabmail.ai/releases/${PLATFORM_KEY}"
HELPER_PATH="$INSTALL_DIR/fts_helper"
NOTICES_PATH="$INSTALL_DIR/fts_helper-THIRD_PARTY_NOTICES.txt"
MANIFEST_PATH="$MANIFEST_DIR/tabmail_fts.json"

# download URL FILE
if command -v curl &> /dev/null; then
    download() { curl -fsSL "$1" -o "$2"; }
elif command -v wget &> /dev/null; then
    download() { wget -q "$1" -O "$2"; }
else
    echo -e "${RED}Error: Neither curl nor wget found. Please install one of them.${NC}"
    exit 1
fi

# Create directories
echo -e "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$MANIFEST_DIR"
if [[ -n "${MANIFEST_DIR_ALT:-}" ]]; then
    mkdir -p "$MANIFEST_DIR_ALT"
fi

# Download the binary and its notices next to the install, then put both in place: a failed
# download leaves an earlier install as it was. The binary goes first, so a binary that cannot be
# replaced leaves the earlier pair; if the notices then fail, the new binary (which always repairs
# the notices beside it when it starts) puts its own there.
HELPER_DOWNLOAD="$INSTALL_DIR/fts_helper.download"
NOTICES_DOWNLOAD="$INSTALL_DIR/fts_helper-THIRD_PARTY_NOTICES.txt.download"
UPDATE_MANIFEST="$INSTALL_DIR/update-manifest.json.download"
trap 'rm -f "$HELPER_DOWNLOAD" "$NOTICES_DOWNLOAD" "$UPDATE_MANIFEST"' EXIT

echo -e "Reading the current release..."
download "$CDN_URL/update-manifest.json" "$UPDATE_MANIFEST"
VERSION="$(sed -n 's/^[[:space:]]*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$UPDATE_MANIFEST" | head -n 1)"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo -e "${RED}Error: The update manifest names no release version.${NC}"
    exit 1
fi

echo -e "Downloading native-fts ${VERSION} and its third-party notices..."
download "$CDN_URL/fts_helper-${VERSION}" "$HELPER_DOWNLOAD"
download "$CDN_URL/fts_helper-${VERSION}-THIRD_PARTY_NOTICES.txt" "$NOTICES_DOWNLOAD"
chmod +x "$HELPER_DOWNLOAD"
mv -f "$HELPER_DOWNLOAD" "$HELPER_PATH"
mv -f "$NOTICES_DOWNLOAD" "$NOTICES_PATH"

# Create native messaging manifest
echo -e "Creating native messaging manifest..."
cat > "$MANIFEST_PATH" <<EOF
{
  "name": "tabmail_fts",
  "description": "TabMail FTS Native Helper",
  "path": "$HELPER_PATH",
  "type": "stdio",
  "allowed_extensions": ["thunderbird@tabmail.ai"]
}
EOF

if [[ -n "${MANIFEST_DIR_ALT:-}" ]]; then
    MANIFEST_PATH_ALT="$MANIFEST_DIR_ALT/tabmail_fts.json"
    echo -e "Creating native messaging manifest (alternate dir)..."
    cat > "$MANIFEST_PATH_ALT" <<EOF
{
  "name": "tabmail_fts",
  "description": "TabMail FTS Native Helper",
  "path": "$HELPER_PATH",
  "type": "stdio",
  "allowed_extensions": ["thunderbird@tabmail.ai"]
}
EOF
fi

echo ""
echo -e "${GREEN}✓ Installation complete!${NC}"
echo ""
echo "Installed to:"
echo "  Helper:   $HELPER_PATH (${VERSION})"
echo "  Notices:  $NOTICES_PATH"
echo "  Manifest: $MANIFEST_PATH"
if [[ -n "${MANIFEST_PATH_ALT:-}" ]]; then
echo "  Manifest: $MANIFEST_PATH_ALT"
fi
echo ""
echo -e "${YELLOW}Please restart Thunderbird for changes to take effect.${NC}"
