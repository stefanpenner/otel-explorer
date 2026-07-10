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
#   Order: SSOT wires + *spec duals + Rust peer first (fast), TLC last (slow).
#   Does not re-codegen by default — use //tools/decision:update.
#
# Without TLC toolchain: still runs wires/duals/Rust parity; skips model-check
# (exit 0 if those pass). Green TLC at these bounds = strong bug hunt, not proof.
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
SKIP_TLC=0

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
    echo "note: no tlc/JVM — SSOT wires/duals only (TLC skipped)"
    SKIP_TLC=1
  elif [ ! -f "$JAR" ]; then
    mkdir -p "$(dirname "$JAR")"
    echo "tla2tools.jar not found; downloading to $JAR ..."
    if ! curl -fsSL -o "$JAR" \
      https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar; then
      echo "note: could not download tla2tools.jar — SSOT wires/duals only"
      rm -f "$JAR"
      SKIP_TLC=1
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

# --- 1) Fast SSOT first (no JVM) — fail before multi-minute TLC ------------
echo "=== SSOT / duals (fast) ==="

echo
echo "--- production wires ---"
if [ -x "$REPO_ROOT/scripts/verify-decision-wires.sh" ]; then
  if ! "$REPO_ROOT/scripts/verify-decision-wires.sh"; then
    fail=$((fail + 1))
  fi
else
  echo "scripts/verify-decision-wires.sh missing or not executable"
  fail=$((fail + 1))
fi

# Generated decision modules are committed — verify they exist and pass
# go tests whenever `go` is available (CI has go; local may not).
echo
echo "--- decision packages (committed *spec) ---"
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
    printf '%-22s %-40s %s\n' "$sname" "$pkg" 'present'
    if command -v go >/dev/null 2>&1; then
      if (cd "$REPO_ROOT" && go test "./$pkg/" -count=1 >/dev/null 2>&1); then
        printf '%-22s %-40s %s\n' "$sname" "go test ./$pkg/" 'ok'
      else
        printf '%-22s %-40s %s\n' "$sname" "go test ./$pkg/" 'FAIL ✗'
        fail=$((fail + 1))
      fi
    else
      printf '%-22s %-40s %s\n' "$sname" "go test" 'skip (no go)'
    fi
  else
    printf '%-22s %-40s %s\n' "$sname" "$pkg" 'MISSING ✗'
    echo "    | regenerate: scripts/regenerate-decision-cores.sh $sname"
    fail=$((fail + 1))
  fi
done
if ! command -v specgen >/dev/null 2>&1; then
  echo "(specgen not on PATH — packages not re-codegen'd; committed *spec still tested)"
fi

# Dual tests pin production gates to generated Can*/pure preds.
echo
echo "--- dual tests (production ↔ decision) ---"
if command -v go >/dev/null 2>&1; then
  dual_run='Decision|PurePredicates|Dual|GroupStack|LogFetchResultFresh|DropAPI|ClampDecision|RateLimit|SyncBounds'
  declare -a DUAL_PKGS=(
    "pkg/analyzer"
    "pkg/githubapi"
    "pkg/store"
    "pkg/logparse"
    "pkg/tui/results"
  )
  for dpkg in "${DUAL_PKGS[@]}"; do
    if (cd "$REPO_ROOT" && go test "./$dpkg/" -run "$dual_run" -count=1 >/dev/null 2>&1); then
      printf '%-22s %-40s %s\n' "dual" "./$dpkg/" 'ok'
    else
      printf '%-22s %-40s %s\n' "dual" "./$dpkg/" 'FAIL ✗'
      fail=$((fail + 1))
    fi
  done
else
  echo "go not on PATH — skip dual package tests"
fi

# Rust peer (same Decision.tla SSOT): name parity + duals when tools present.
echo
echo "--- rust peer (Go↔Rust parity + cargo duals) ---"
if [ -x "$REPO_ROOT/scripts/gen-decision-rust.sh" ]; then
  if ! "$REPO_ROOT/scripts/gen-decision-rust.sh" --parity-committed; then
    fail=$((fail + 1))
  fi
else
  echo "scripts/gen-decision-rust.sh missing — skip parity"
  fail=$((fail + 1))
fi
if command -v cargo >/dev/null 2>&1; then
  if ! env CC=cc RUSTFLAGS='-C linker=/usr/bin/cc' \
    cargo test -p decision_cores --quiet; then
    echo "cargo test -p decision_cores FAIL ✗"
    fail=$((fail + 1))
  else
    echo "cargo test -p decision_cores ok"
  fi
else
  echo "cargo not on PATH — skip rust duals"
fi

if [ "$SKIP_TLC" -eq 1 ]; then
  echo
  echo "specs: $pass ok (TLC skipped), $fail failed (SSOT/duals)"
  [ $fail -eq 0 ]
  exit $?
fi

# --- 2) TLC last (slow) ----------------------------------------------------
echo
echo "=== TLC (slow) ==="
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

echo
echo "specs: $pass ok, $fail failed"
[ $fail -eq 0 ]
