#!/usr/bin/env bash
# gen-decision-rust.sh — emit Rust decision modules from Decision.tla (PATH A).
#
# Uses JIT `specgen -lang rust` on PATH (dotai tool). Does NOT replace the
# hermetic Go Bazel pipeline (//tools/decision:update). Same .tla SSOT.
#
# Usage:
#   scripts/gen-decision-rust.sh              # all cores → gen/rust/<core>/
#   scripts/gen-decision-rust.sh log-groups   # one core
#   scripts/gen-decision-rust.sh --check      # rustc -D warnings each (if rustc)
#
# Requires: specgen with -lang rust (see ~/.ai/tools/specgen).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_ROOT="${RUST_DECISION_OUT:-gen/rust}"
CHECK=0
CORES=()

for arg in "$@"; do
  case "$arg" in
    --check) CHECK=1 ;;
    -h|--help)
      sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) CORES+=("$arg") ;;
  esac
done

if ! command -v specgen >/dev/null 2>&1; then
  echo "error: specgen not on PATH (install ~/.ai tools: bazel run //:install)" >&2
  exit 1
fi

# core_dir|const flags (space-separated Name=Value)
# Keep in sync with tools/decision/BUILD.bazel decision_core consts.
rows=(
  "tui-reload|MaxGen=2 MaxJobs=2 Bug=FALSE"
  "rate-limit|MaxRem=2 MaxClock=3 Bug=FALSE"
  "timing-clamp|Tmax=4 Bug=FALSE"
  "sync-bounds|Bug=FALSE"
  "gha-lifecycle|Bug=FALSE"
  "log-groups|MaxDepth=3 Bug=FALSE"
  "span-tree|Bug=FALSE"
)

want_core() {
  local c="$1"
  if [ ${#CORES[@]} -eq 0 ]; then
    return 0
  fi
  local x
  for x in "${CORES[@]}"; do
    [ "$x" = "$c" ] && return 0
  done
  return 1
}

fail=0
for row in "${rows[@]}"; do
  core="${row%%|*}"
  consts="${row#*|}"
  want_core "$core" || continue

  tla="specs/$core/decision/Decision.tla"
  if [ ! -f "$tla" ]; then
    echo "FAIL missing $tla"
    fail=1
    continue
  fi

  out="$OUT_ROOT/$core"
  mkdir -p "$out"
  args=(-lang rust -o "$out" -p "${core//-/_}spec")
  for c in $consts; do
    args+=(-const "$c")
  done
  args+=("$tla")

  echo "--- $core ---"
  if ! specgen "${args[@]}"; then
    echo "FAIL specgen $core"
    fail=1
    continue
  fi

  if [ "$CHECK" -eq 1 ]; then
    if ! command -v rustc >/dev/null 2>&1; then
      echo "  skip rustc (not on PATH)"
    else
      rlib="$out/lib.rlib"
      if rustc --edition 2021 --crate-type lib -D warnings -o "$rlib" "$out/spec.rs" 2>"$out/rustc.err"; then
        echo "  rustc ok"
        rm -f "$rlib" "$out/rustc.err"
      else
        echo "  FAIL rustc $core"
        cat "$out/rustc.err" | sed 's/^/    | /'
        fail=1
      fi
    fi
  fi
done

echo
if [ "$fail" -ne 0 ]; then
  echo "gen-decision-rust: FAIL"
  exit 1
fi
echo "gen-decision-rust: OK → $OUT_ROOT/"
exit 0
