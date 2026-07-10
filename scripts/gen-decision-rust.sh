#!/usr/bin/env bash
# gen-decision-rust.sh — emit Rust decision modules from Decision.tla (PATH A).
#
# JIT path uses `specgen -lang rust` on PATH (dotai tool). Does NOT replace the
# hermetic Bazel pipeline (//tools/decision:update). Same .tla SSOT.
#
# Usage:
#   scripts/gen-decision-rust.sh                 # all cores → gen/rust/<core>/
#   scripts/gen-decision-rust.sh log-groups      # one core
#   scripts/gen-decision-rust.sh --check         # rustc -D warnings each (if rustc)
#   scripts/gen-decision-rust.sh --parity        # after gen: Go↔JIT Rust names
#   scripts/gen-decision-rust.sh --parity-committed
#       # hermetic: Go *spec ↔ crates/decision_cores (no JIT; used by decision-check)
#   scripts/gen-decision-rust.sh --check --parity
#
# Requires: specgen for gen / --check / --parity (not for --parity-committed alone).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

OUT_ROOT="${RUST_DECISION_OUT:-gen/rust}"
CHECK=0
PARITY=0
PARITY_COMMITTED=0
CORES=()

for arg in "$@"; do
  case "$arg" in
    --check) CHECK=1 ;;
    --parity) PARITY=1 ;;
    --parity-committed) PARITY_COMMITTED=1 ;;
    -h|--help)
      sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) CORES+=("$arg") ;;
  esac
done

# core_dir|go_pkg|const flags|rust_module (snake, no .rs)
# Keep in sync with tools/decision/BUILD.bazel decision_core consts.
rows=(
  "tui-reload|pkg/tui/results/tuireloadspec|MaxGen=2 MaxJobs=2 Bug=FALSE|tui_reload"
  "rate-limit|pkg/githubapi/ratelimitspec|MaxRem=2 MaxClock=3 Bug=FALSE|rate_limit"
  "timing-clamp|pkg/analyzer/timingclampspec|Tmax=4 Bug=FALSE|timing_clamp"
  "sync-bounds|pkg/store/syncboundsspec|Bug=FALSE|sync_bounds"
  "gha-lifecycle|pkg/analyzer/ghalifecyclespec|Bug=FALSE|gha_lifecycle"
  "log-groups|pkg/logparse/loggroupsspec|MaxDepth=3 Bug=FALSE|log_groups"
  "span-tree|pkg/analyzer/spantreespec|Bug=FALSE|span_tree"
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

# PascalCase / CamelCase → snake_case (enough for action names).
to_snake() {
  echo "$1" | sed -E \
    -e 's/([a-z0-9])([A-Z])/\1_\2/g' \
    -e 's/([A-Z]+)([A-Z][a-z])/\1_\2/g' \
    | tr '[:upper:]' '[:lower:]'
}

# Extract Go CanX action base names (Open from CanOpen).
# grep exit 1 on no match must not abort under set -e / pipefail.
go_actions() {
  local go="$1"
  grep -oE 'func \(s State\) Can[A-Za-z0-9_]+\(' "$go" 2>/dev/null \
    | sed -E 's/.*Can([A-Za-z0-9_]+)\($/\1/' \
    | sort -u \
    || true
}

go_pures() {
  local go="$1"
  grep -oE 'Name: "[A-Za-z0-9_]+"' "$go" 2>/dev/null \
    | sed -E 's/Name: "//;s/"$//' \
    | sort -u \
    || true
}

rust_actions() {
  local rs="$1"
  grep -oE 'pub fn can_[a-z0-9_]+\(' "$rs" 2>/dev/null \
    | sed -E 's/.*pub fn can_([a-z0-9_]+)\($/\1/' \
    | sort -u \
    || true
}

rust_pures() {
  local rs="$1"
  grep -oE 'name: "[A-Za-z0-9_]+"' "$rs" 2>/dev/null \
    | sed -E 's/name: "//;s/"$//' \
    | sort -u \
    || true
}

# Compare Go *spec action/pure names to a Rust module (JIT or committed).
check_parity() {
  local core="$1"
  local go_file="$2"
  local rs_file="$3"
  local label="${4:-}"

  if [ ! -f "$go_file" ]; then
    echo "  FAIL parity: missing $go_file"
    return 1
  fi
  if [ ! -f "$rs_file" ]; then
    echo "  FAIL parity: missing $rs_file"
    return 1
  fi

  local go_act_snake rs_act go_pu rs_pu n local_fail=0
  go_act_snake=$(
    go_actions "$go_file" | while read -r a; do
      [ -n "$a" ] && to_snake "$a"
    done | sort -u
  )
  rs_act=$(rust_actions "$rs_file")
  if [ "$go_act_snake" != "$rs_act" ]; then
    echo "  FAIL action parity${label:+ ($label)}"
    echo "    go→snake: $(echo "$go_act_snake" | tr '\n' ' ')"
    echo "    rust:     $(echo "$rs_act" | tr '\n' ' ')"
    local_fail=1
  else
    echo "  actions ok ($(echo "$rs_act" | grep -c . || true))${label:+ [$label]}"
  fi

  go_pu=$(go_pures "$go_file")
  rs_pu=$(rust_pures "$rs_file")
  if [ "$go_pu" != "$rs_pu" ]; then
    echo "  FAIL pure parity${label:+ ($label)}"
    echo "    go:   $(echo "$go_pu" | tr '\n' ' ')"
    echo "    rust: $(echo "$rs_pu" | tr '\n' ' ')"
    local_fail=1
  else
    n=$(echo "$go_pu" | grep -c . || true)
    echo "  pures ok ($n)${label:+ [$label]}"
  fi
  return "$local_fail"
}

fail=0

# --- Hermetic path: committed Go *spec ↔ crates/decision_cores (no JIT) ---
if [ "$PARITY_COMMITTED" -eq 1 ]; then
  echo "=== Go↔Rust parity (committed) ==="
  for row in "${rows[@]}"; do
    core="${row%%|*}"
    rest="${row#*|}"
    gopkg="${rest%%|*}"
    rest2="${rest#*|}"
    # rest2 = consts|rs_mod
    rsmod="${rest2##*|}"
    want_core "$core" || continue

    echo "--- $core ---"
    if ! check_parity "$core" "$gopkg/spec.go" \
      "crates/decision_cores/src/${rsmod}.rs" "committed"; then
      fail=1
    fi
  done
  echo
  if [ "$fail" -ne 0 ]; then
    echo "gen-decision-rust --parity-committed: FAIL"
    exit 1
  fi
  # Without JIT flags, stop here (no specgen required).
  if [ "$CHECK" -eq 0 ] && [ "$PARITY" -eq 0 ]; then
    echo "gen-decision-rust --parity-committed: OK"
    exit 0
  fi
fi

# --- JIT path needs specgen ---
if ! command -v specgen >/dev/null 2>&1; then
  echo "error: specgen not on PATH (install ~/.ai tools: bazel run //:install)" >&2
  exit 1
fi

for row in "${rows[@]}"; do
  core="${row%%|*}"
  rest="${row#*|}"
  gopkg="${rest%%|*}"
  rest2="${rest#*|}"
  consts="${rest2%|*}"
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
        sed 's/^/    | /' "$out/rustc.err"
        fail=1
      fi
    fi
  fi

  if [ "$PARITY" -eq 1 ]; then
    if ! check_parity "$core" "$gopkg/spec.go" "$out/spec.rs" "jit"; then
      fail=1
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
