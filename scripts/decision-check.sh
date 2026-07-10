#!/usr/bin/env bash
# decision-check.sh — lean decision-core gate (no full-repo TLC by default).
#
# Fast path after gate/symbol edits:
#   1. Production wire symbols still exist
#   2. Bazel tests tagged decision (*spec duals + Go/Rust up_to_date)
#   3. Go↔Rust action/pure name parity (committed *spec ↔ crates)
#   4. Rust decision_cores duals when cargo is on PATH
#   5. Optional: TLC for named cores (full + decision for that core)
#
# Full design TLC for everything: scripts/check-specs.sh
#
# Usage:
#   scripts/decision-check.sh              # wires + bazel + parity + cargo
#   scripts/decision-check.sh --with-tlc   # also TLC every core with decision/
#   scripts/decision-check.sh <core> ...   # TLC only those cores (implies TLC)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

WITH_TLC=0
CORES=()

for arg in "$@"; do
  case "$arg" in
    --with-tlc) WITH_TLC=1 ;;
    -h|--help)
      sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      CORES+=("$arg")
      WITH_TLC=1
      ;;
  esac
done

fail=0

echo "=== decision wires ==="
if ! "$REPO_ROOT/scripts/verify-decision-wires.sh"; then
  fail=1
fi

echo
echo "=== bazel --test_tag_filters=decision ==="
if ! bazel test //... --test_tag_filters=decision --test_output=errors; then
  fail=1
fi

echo
echo "=== Go↔Rust parity (committed) ==="
if ! "$REPO_ROOT/scripts/gen-decision-rust.sh" --parity-committed; then
  fail=1
fi

echo
echo "=== cargo test -p decision_cores ==="
if command -v cargo >/dev/null 2>&1; then
  # Force system linker: hermetic_cc/zig from Bazel env breaks rustc link.
  # Always override RUSTFLAGS (do not inherit zig flags from the shell).
  if ! env CC=cc RUSTFLAGS='-C linker=/usr/bin/cc' \
    cargo test -p decision_cores --quiet; then
    fail=1
  fi
else
  echo "skip: cargo not on PATH"
fi

if [ "$WITH_TLC" -eq 1 ]; then
  echo
  echo "=== TLC (decision cores) ==="
  if [ ${#CORES[@]} -eq 0 ]; then
    while IFS= read -r core; do
      CORES+=("$core")
    done < <(
      find specs -path '*/decision/Decision.tla' -print \
        | sed 's|^specs/||;s|/decision/Decision.tla$||' \
        | sort
    )
  fi
  if [ ${#CORES[@]} -eq 0 ]; then
    echo "no decision cores found under specs/*/decision/"
    fail=1
  else
    # check-specs skips cleanly when no JVM/tlc; still runs package duals.
    if ! "$REPO_ROOT/scripts/check-specs.sh" "${CORES[@]}"; then
      fail=1
    fi
  fi
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "decision-check: FAIL"
  exit 1
fi
echo "decision-check: OK"
exit 0
