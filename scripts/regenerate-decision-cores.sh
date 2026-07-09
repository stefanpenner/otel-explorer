#!/usr/bin/env bash
# regenerate-decision-cores.sh — re-run decision-core codegen via Bazel.
#
# Preferred entrypoint (hermetic, matches CI):
#   bazel run //tools/decision:update
#
# This script is a thin wrapper for the same target. Optional subsystem
# names are ignored for now (Bazel regenerates all cores together).
#
# Usage:
#   scripts/regenerate-decision-cores.sh
#   scripts/regenerate-decision-cores.sh timing-clamp   # still regenerates all

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [ "$#" -gt 0 ]; then
  echo "note: Bazel update regenerates all decision cores (ignoring args: $*)"
fi

exec bazel run //tools/decision:update
