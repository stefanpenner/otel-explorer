#!/usr/bin/env bash
# check-specs.sh — model-check every TLA+ spec under specs/ with TLC.
#
# Convention (enforced here):
#   specs/<name>/<Spec>.tla        full design model (records/quantifiers OK)
#   specs/<name>/MC*.cfg           configs; expected result by name:
#     MCBait*.cfg     MUST FAIL  (proves TLC actually explores)
#     MCMutation*.cfg MUST FAIL  (proves invariants have teeth)
#     MCFinding*.cfg  MUST FAIL  (documented real finding, kept on purpose)
#     anything else   MUST PASS
#
# Decision cores (specgen bridge — see specs/DECISION_CORES.md):
#   specs/<name>/decision/Decision.tla
#   specs/<name>/decision/MC*.cfg   same bait / mutation / pass convention
#
# Toolchain:
#   Prefer `tlc` on PATH (dotai CLI). Else fall back to java -cp tla2tools.jar.
#   After TLC, if `specgen` is on PATH, verify generated *spec packages exist
#   under pkg/ (optional: go test those packages). Does not re-codegen by
#   default — regenerate intentionally when Decision.tla changes.
#
# Skips gracefully (exit 0) when no TLC toolchain is available.
# Green TLC at these bounds = strong bug hunt, not proof.
# Does not break full-spec checking when decision cores are absent.
#
# Usage: scripts/check-specs.sh [spec-name ...]   # default: all specs

set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPECS_DIR="$REPO_ROOT/specs"
JAR="${TLA2TOOLS_JAR:-$HOME/.cache/tla2tools.jar}"

# --- toolchain ---------------------------------------------------------------
# Prefer dotai `tlc` CLI when present; else java + jar (legacy path).
USE_TLC_CLI=0
JAVA=""

if command -v tlc >/dev/null 2>&1; then
  USE_TLC_CLI=1
else
  for cand in /opt/homebrew/opt/openjdk/bin/java java; do
    if command -v "$cand" >/dev/null 2>&1 && "$cand" -version >/dev/null 2>&1; then
      JAVA="$(command -v "$cand")"
      break
    fi
  done
  if [ -z "$JAVA" ]; then
    echo "SKIP: no tlc on PATH and no working JVM (TLA+ specs not checked)"
    exit 0
  fi
  if [ ! -f "$JAR" ]; then
    mkdir -p "$(dirname "$JAR")"
    echo "tla2tools.jar not found; downloading to $JAR ..."
    if ! curl -fsSL -o "$JAR" \
      https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar; then
      echo "SKIP: could not download tla2tools.jar (TLA+ specs not checked)"
      rm -f "$JAR"
      exit 0
    fi
  fi
fi

# SANY parse. Args: <dir> <module.tla>. Returns 0 on success.
sany_parse() {
  local dir="$1" module="$2"
  if [ "$USE_TLC_CLI" -eq 1 ]; then
    (cd "$dir" && tlc --parse "$module" >/dev/null 2>&1)
  else
    (cd "$dir" && "$JAVA" -cp "$JAR" tla2sany.SANY "$module" >/dev/null 2>&1)
  fi
}

# Run TLC. Args: <dir> <config.cfg> <module.tla>.
# Prints full output on stdout; exit code is TLC's.
# --raw keeps classic "Error: Invariant ... is violated" lines for classify.
tlc_run() {
  local dir="$1" cfg="$2" module="$3"
  if [ "$USE_TLC_CLI" -eq 1 ]; then
    # --no-deadlock: safety-only specs; terminal stutter is expected.
    (cd "$dir" && tlc --raw --no-deadlock -c "$cfg" "$module" 2>&1)
  else
    (cd "$dir" && "$JAVA" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
      -workers auto -cleanup -deadlock -config "$cfg" "$module" 2>&1)
  fi
}

# Classify TLC output → green | violate | error(rc)
classify() {
  local rc="$1" out="$2"
  if [ "$rc" -eq 0 ]; then
    echo green
  elif echo "$out" | grep -qE 'Error: Invariant|Error: Action property|is violated|Invariant .* violated'; then
    echo violate
  else
    echo "error($rc)"
  fi
}

expected_for_cfg() {
  local cfgname="$1"
  case "$cfgname" in
    MCBait* | MCMutation* | MCFinding*) echo violate ;;
    *) echo green ;;
  esac
}

# Run all MC*.cfg in dir against module. Args: <label> <dir> <module.tla>
# Updates pass/fail counters; prints table rows.
run_mc_configs() {
  local label="$1" dir="$2" module="$3"

  if ! sany_parse "$dir" "$module"; then
    printf '%-22s %-18s %-10s %-10s %s\n' "$label" "(SANY)" parse error 'FAIL ✗'
    fail=$((fail + 1))
    return
  fi

  local cfg cfgname expected out rc actual
  for cfg in "$dir"/MC*.cfg; do
    [ -e "$cfg" ] || continue
    cfgname="$(basename "$cfg" .cfg)"
    expected="$(expected_for_cfg "$cfgname")"

    out="$(tlc_run "$dir" "$cfgname.cfg" "$module")"
    rc=$?
    actual="$(classify "$rc" "$out")"

    if [ "$actual" = "$expected" ]; then
      printf '%-22s %-18s %-10s %-10s %s\n' "$label" "$cfgname" "$expected" "$actual" 'ok'
      pass=$((pass + 1))
    else
      printf '%-22s %-18s %-10s %-10s %s\n' "$label" "$cfgname" "$expected" "$actual" 'FAIL ✗'
      echo "$out" | tail -20 | sed 's/^/    | /'
      fail=$((fail + 1))
    fi
  done

  rm -rf "$dir/states"
}

# --- run ---------------------------------------------------------------------
specs=("$@")
if [ ${#specs[@]} -eq 0 ]; then
  for d in "$SPECS_DIR"/*/; do
    specs+=("$(basename "$d")")
  done
fi

pass=0 fail=0
if [ "$USE_TLC_CLI" -eq 1 ]; then
  echo "toolchain: tlc CLI ($(command -v tlc))"
else
  echo "toolchain: java -cp $JAR"
fi
printf '%-22s %-18s %-10s %-10s %s\n' SPEC CONFIG EXPECTED ACTUAL RESULT

# Full design models (specs/<name>/*.tla, maxdepth 1 — not decision/)
for name in "${specs[@]}"; do
  dir="$SPECS_DIR/$name"
  if [ ! -d "$dir" ]; then
    echo "no such spec dir: $dir" >&2
    fail=$((fail + 1))
    continue
  fi

  tla="$(find "$dir" -maxdepth 1 -name '*.tla' | head -1)"
  if [ -z "$tla" ]; then
    echo "$name: no .tla file" >&2
    fail=$((fail + 1))
    continue
  fi
  module="$(basename "$tla")"
  run_mc_configs "$name" "$dir" "$module"
done

# Decision cores: specs/<name>/decision/Decision.tla + MC.cfg
echo
echo "--- decision cores ---"
for name in "${specs[@]}"; do
  ddir="$SPECS_DIR/$name/decision"
  if [ ! -f "$ddir/Decision.tla" ]; then
    continue
  fi
  if [ ! -f "$ddir/MC.cfg" ]; then
    printf '%-22s %-18s %-10s %-10s %s\n' "$name/decision" "(MC.cfg)" present missing 'FAIL ✗'
    fail=$((fail + 1))
    continue
  fi
  run_mc_configs "$name/decision" "$ddir" "Decision.tla"
done

# Optional: if specgen on PATH, verify generated packages exist for known cores.
# Does not auto-regenerate (avoids overwriting intentional bindings). Document
# regenerate commands in each specs/*/decision/README.md.
echo
echo "--- specgen packages ---"
if command -v specgen >/dev/null 2>&1; then
  # name → pkg path (relative to REPO_ROOT)
  declare -a SPEC_PKGS=(
    "tui-reload:pkg/tui/results/tuireloadspec"
    "rate-limit:pkg/githubapi/ratelimitspec"
    "timing-clamp:pkg/analyzer/timingclampspec"
    "sync-bounds:pkg/store/syncboundsspec"
    "gha-lifecycle:pkg/analyzer/ghalifecyclespec"
    "log-groups:pkg/logparse/loggroupsspec"
    "span-tree:pkg/analyzer/spantreespec"
  )
  for entry in "${SPEC_PKGS[@]}"; do
    sname="${entry%%:*}"
    pkg="${entry#*:}"
    # Only check cores we were asked to run and that have a Decision.tla
    skip=1
    for n in "${specs[@]}"; do
      if [ "$n" = "$sname" ]; then skip=0; break; fi
    done
    [ "$skip" -eq 0 ] || continue
    [ -f "$SPECS_DIR/$sname/decision/Decision.tla" ] || continue

    if [ -f "$REPO_ROOT/$pkg/spec.go" ] && [ -f "$REPO_ROOT/$pkg/spec_test.go" ]; then
      printf '%-22s %-40s %s\n' "$sname" "$pkg" 'ok'
      # Optional go test of the generated package (fast BFS).
      if command -v go >/dev/null 2>&1; then
        if ! (cd "$REPO_ROOT" && go test "./$pkg/" -count=1 >/dev/null 2>&1); then
          printf '%-22s %-40s %s\n' "$sname" "go test ./$pkg/" 'FAIL ✗'
          fail=$((fail + 1))
        fi
      fi
    else
      printf '%-22s %-40s %s\n' "$sname" "$pkg" 'MISSING ✗'
      echo "    | regenerate via specs/$sname/decision/README.md (specgen on PATH)"
      fail=$((fail + 1))
    fi
  done
else
  echo "specgen: not on PATH (skip package verify; install via ~/.ai or PATH)"
fi

echo
echo "specs: $pass ok, $fail failed"
[ $fail -eq 0 ]
