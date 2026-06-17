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
#   https://cdn.tabmail.ai/releases/${PLATFORM_KEY}/fts_helper-latest
HELPER_URL="https://cdn.tabmail.ai/releases/${PLATFORM_KEY}/fts_helper-latest"
HELPER_PATH="$INSTALL_DIR/fts_helper"
MANIFEST_PATH="$MANIFEST_DIR/tabmail_fts.json"

# Create directories
echo -e "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$MANIFEST_DIR"
if [[ -n "${MANIFEST_DIR_ALT:-}" ]]; then
    mkdir -p "$MANIFEST_DIR_ALT"
fi

# Download binary
echo -e "Downloading native-fts binary..."
if command -v curl &> /dev/null; then
    curl -fsSL "$HELPER_URL" -o "$HELPER_PATH"
elif command -v wget &> /dev/null; then
    wget -q "$HELPER_URL" -O "$HELPER_PATH"
else
    echo -e "${RED}Error: Neither curl nor wget found. Please install one of them.${NC}"
    exit 1
fi

# Make executable
chmod +x "$HELPER_PATH"

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
echo "  Helper:   $HELPER_PATH"
echo "  Manifest: $MANIFEST_PATH"
if [[ -n "${MANIFEST_PATH_ALT:-}" ]]; then
echo "  Manifest: $MANIFEST_PATH_ALT"
fi
echo ""
echo -e "${YELLOW}Please restart Thunderbird for changes to take effect.${NC}"
