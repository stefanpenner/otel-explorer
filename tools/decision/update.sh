#!/usr/bin/env bash
# Copy Bazel-generated decision cores into the source tree (Go + Rust).
# Usage: bazel run //tools/decision:update
set -euo pipefail

if [ -z "${BUILD_WORKSPACE_DIRECTORY:-}" ]; then
  echo "error: run via 'bazel run //tools/decision:update'" >&2
  exit 1
fi

ROOT="$BUILD_WORKSPACE_DIRECTORY"
RF="${RUNFILES_DIR:-}/_main"
if [ ! -d "$RF" ]; then
  RF="${BASH_SOURCE[0]}.runfiles/_main"
fi

find_gendir() {
  local name="$1"
  local gendir="$RF/tools/decision/${name}_gen"
  if [ -f "$gendir/spec.go" ]; then
    echo "$gendir"
    return 0
  fi
  gendir="$RF/otel-explorer/tools/decision/${name}_gen"
  if [ -f "$gendir/spec.go" ]; then
    echo "$gendir"
    return 0
  fi
  local found
  found=$(find "${RUNFILES_DIR:-/nonexistent}" -path "*/${name}_gen/spec.go" 2>/dev/null | head -1 || true)
  if [ -n "$found" ]; then
    dirname "$found"
    return 0
  fi
  return 1
}

copy_one() {
  local name="$1" dest="$2" rs_dest="$3"
  local gendir
  gendir=$(find_gendir "$name") || {
    echo "error: missing generated files for $name" >&2
    echo "RUNFILES_DIR=${RUNFILES_DIR:-}" >&2
    find "${RUNFILES_DIR:-/tmp}" -name 'spec.go' 2>/dev/null | head -20 >&2 || true
    exit 1
  }
  if [ ! -f "$gendir/spec.go" ] || [ ! -f "$gendir/spec_test.go" ] || [ ! -f "$gendir/spec.rs" ]; then
    echo "error: incomplete gen for $name in $gendir" >&2
    ls -la "$gendir" >&2 || true
    exit 1
  fi
  mkdir -p "$ROOT/$dest"
  cp "$gendir/spec.go" "$ROOT/$dest/spec.go"
  cp "$gendir/spec_test.go" "$ROOT/$dest/spec_test.go"
  mkdir -p "$(dirname "$ROOT/$rs_dest")"
  # Crate module: strip crate-level test module noise is fine; keep full gen.
  cp "$gendir/spec.rs" "$ROOT/$rs_dest"
  echo "updated $dest + $rs_dest"
}

# Go dest | Rust module path under crates/decision_cores
copy_one tui_reload pkg/tui/results/tuireloadspec crates/decision_cores/src/tui_reload.rs
copy_one rate_limit pkg/githubapi/ratelimitspec crates/decision_cores/src/rate_limit.rs
copy_one timing_clamp pkg/analyzer/timingclampspec crates/decision_cores/src/timing_clamp.rs
copy_one sync_bounds pkg/store/syncboundsspec crates/decision_cores/src/sync_bounds.rs
copy_one gha_lifecycle pkg/analyzer/ghalifecyclespec crates/decision_cores/src/gha_lifecycle.rs
copy_one log_groups pkg/logparse/loggroupsspec crates/decision_cores/src/log_groups.rs
copy_one span_tree pkg/analyzer/spantreespec crates/decision_cores/src/span_tree.rs

echo "done. Review diffs and commit."
