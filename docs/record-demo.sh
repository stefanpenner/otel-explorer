#!/bin/sh
# Records a demo of ote and converts to SVG.
#
# Usage: ./docs/record-demo.sh
#
# Prerequisites:
#   npm install -g svg-term-cli
#
# The Go driver creates a properly-sized pty, spawns ote,
# sends scripted keystrokes, and writes an asciinema v2 .cast file.
# No asciinema binary needed.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CAST_FILE="${SCRIPT_DIR}/demo.cast"
SVG_FILE="${SCRIPT_DIR}/demo.svg"
BINARY="ote"

# Ensure binary is built
if ! command -v "$BINARY" >/dev/null 2>&1; then
  echo "Building ${BINARY}..."
  (cd "${SCRIPT_DIR}/.." && go build -o "${BINARY}" ./cmd/ote)
  PATH="${SCRIPT_DIR}/..:${PATH}"
fi

echo "Recording demo..."
(cd "${SCRIPT_DIR}/.." && go run ./cmd/record-demo "$CAST_FILE")

echo "Converting to SVG..."
svg-term --in "$CAST_FILE" --out "$SVG_FILE" \
  --window \
  --no-cursor

echo "Done: ${SVG_FILE}"
