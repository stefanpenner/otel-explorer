#!/usr/bin/env bash
# regenerate-decision-cores.sh — re-run specgen for every decision core.
#
# Requires: specgen on PATH (dotai: ~/.ai/bin/specgen).
# Never hand-edit generated pkg/.../*spec code — change Decision.tla, re-run this.
#
# Usage: scripts/regenerate-decision-cores.sh [subsystem ...]
#   default: all specs/*/decision/Decision.tla

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v specgen >/dev/null 2>&1; then
  echo "ERROR: specgen not on PATH (install via bazel run //:install from ~/.ai)" >&2
  exit 1
fi

# subsystem → (pkg path, package name, extra -const flags)
# Keep in sync with specs/DECISION_CORES.md
regen_one() {
  local name="$1"
  local out pkg consts
  case "$name" in
    tui-reload)
      out=pkg/tui/results/tuireloadspec
      pkg=tuireloadspec
      consts=(-const MaxGen=2 -const MaxJobs=2 -const Bug=FALSE)
      ;;
    rate-limit)
      out=pkg/githubapi/ratelimitspec
      pkg=ratelimitspec
      consts=(-const MaxRem=2 -const MaxClock=3 -const Bug=FALSE)
      ;;
    timing-clamp)
      out=pkg/analyzer/timingclampspec
      pkg=timingclampspec
      consts=(-const Tmax=4 -const Bug=FALSE)
      ;;
    sync-bounds)
      out=pkg/store/syncboundsspec
      pkg=syncboundsspec
      consts=(-const Bug=FALSE)
      ;;
    gha-lifecycle)
      out=pkg/analyzer/ghalifecyclespec
      pkg=ghalifecyclespec
      consts=(-const Bug=FALSE)
      ;;
    log-groups)
      out=pkg/logparse/loggroupsspec
      pkg=loggroupsspec
      consts=(-const MaxDepth=3 -const Bug=FALSE)
      ;;
    span-tree)
      out=pkg/analyzer/spantreespec
      pkg=spantreespec
      consts=(-const Bug=FALSE)
      ;;
    *)
      echo "unknown subsystem: $name" >&2
      return 1
      ;;
  esac

  local tla="specs/$name/decision/Decision.tla"
  if [ ! -f "$tla" ]; then
    echo "missing $tla" >&2
    return 1
  fi

  echo "specgen $name → $out"
  # Write straight into the package dir so the Regenerate: header records the
  # stable -o path (not a temp dir). Only spec.go / spec_test.go are written;
  # handwritten BUILD.bazel and conform_test.go are left alone.
  mkdir -p "$out"
  specgen -o "$out" -p "$pkg" "${consts[@]}" "$tla"
  echo "  ok $out/spec.go"
}

names=("$@")
if [ ${#names[@]} -eq 0 ]; then
  for d in specs/*/decision/Decision.tla; do
    names+=("$(basename "$(dirname "$(dirname "$d")")")")
  done
fi

for n in "${names[@]}"; do
  regen_one "$n"
done

echo "done: ${#names[@]} core(s). Run: go test ./pkg/.../*spec/  (or bazel test)"
