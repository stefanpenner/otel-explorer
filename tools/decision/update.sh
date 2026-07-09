#!/usr/bin/env bash
# Copy Bazel-generated decision cores into the source tree.
# Usage: bazel run //tools/decision:update
set -euo pipefail

if [ -z "${BUILD_WORKSPACE_DIRECTORY:-}" ]; then
  echo "error: run via 'bazel run //tools/decision:update'" >&2
  exit 1
fi

ROOT="$BUILD_WORKSPACE_DIRECTORY"
# Runfiles: each genrule produces <name>_gen/spec.go under tools/decision/
# When executed via bazel run, data files are in RUNFILES_DIR.
RF="${RUNFILES_DIR:-}/_main"
if [ ! -d "$RF" ]; then
  # Fallback for older layouts
  RF="${BASH_SOURCE[0]}.runfiles/_main"
fi

copy_one() {
  local name="$1" dest="$2"
  local gendir="$RF/tools/decision/${name}_gen"
  if [ ! -f "$gendir/spec.go" ]; then
    # Try rlocation-style path used by rules_shell / bash runfiles
    gendir="$RF/otel-explorer/tools/decision/${name}_gen"
  fi
  if [ ! -f "$gendir/spec.go" ]; then
    # Direct path from runfiles manifest discovery
    local found
    found=$(find "${RUNFILES_DIR:-/nonexistent}" -path "*/${name}_gen/spec.go" 2>/dev/null | head -1 || true)
    if [ -n "$found" ]; then
      gendir=$(dirname "$found")
    fi
  fi
  if [ ! -f "$gendir/spec.go" ] || [ ! -f "$gendir/spec_test.go" ]; then
    echo "error: missing generated files for $name (looked in $gendir)" >&2
    echo "RUNFILES_DIR=${RUNFILES_DIR:-}" >&2
    find "${RUNFILES_DIR:-/tmp}" -name 'spec.go' 2>/dev/null | head -20 >&2 || true
    exit 1
  fi
  mkdir -p "$ROOT/$dest"
  cp "$gendir/spec.go" "$ROOT/$dest/spec.go"
  cp "$gendir/spec_test.go" "$ROOT/$dest/spec_test.go"
  echo "updated $dest"
}

# Keep in sync with tools/decision/BUILD.bazel CORES.
copy_one tui_reload pkg/tui/results/tuireloadspec
copy_one rate_limit pkg/githubapi/ratelimitspec
copy_one timing_clamp pkg/analyzer/timingclampspec
copy_one sync_bounds pkg/store/syncboundsspec
copy_one gha_lifecycle pkg/analyzer/ghalifecyclespec
copy_one log_groups pkg/logparse/loggroupsspec
copy_one span_tree pkg/analyzer/spantreespec

echo "done. Review diffs and commit."
