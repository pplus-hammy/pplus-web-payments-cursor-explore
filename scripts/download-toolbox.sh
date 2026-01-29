#!/usr/bin/env bash
# Download Google MCP Toolbox binary for BigQuery (macOS Apple Silicon).
# Run from project root: ./scripts/download-toolbox.sh

set -e
VERSION="v0.26.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$PROJECT_ROOT/bin"
URL="https://storage.googleapis.com/genai-toolbox/${VERSION}/darwin/arm64/toolbox"

mkdir -p "$BIN_DIR"
curl -sSLo "$BIN_DIR/toolbox" "$URL"
chmod +x "$BIN_DIR/toolbox"
echo "Installed toolbox to $BIN_DIR/toolbox"
"$BIN_DIR/toolbox" --version
