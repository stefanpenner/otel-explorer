#!/usr/bin/env bash
# check-specs.sh — model-check every TLA+ spec under specs/ with TLC.
#
# Convention (enforced here):
#   specs/<name>/<Spec>.tla        one spec module per dir
#   specs/<name>/MC*.cfg           configs; expected result by name:
#     MCBait*.cfg     MUST FAIL  (proves TLC actually explores)
#     MCMutation*.cfg MUST FAIL  (proves invariants have teeth)
#     MCFinding*.cfg  MUST FAIL  (documented real finding, kept on purpose)
#     anything else   MUST PASS
#
# Skips gracefully (exit 0) when no JVM is available, per repo policy.
# Green TLC at these bounds = strong bug hunt, not proof.
#
# Usage: scripts/check-specs.sh [spec-name ...]   # default: all specs

set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPECS_DIR="$REPO_ROOT/specs"
JAR="${TLA2TOOLS_JAR:-$HOME/.cache/tla2tools.jar}"

# --- toolchain (skip, don't fail, when absent) -------------------------------
JAVA=""
for cand in /opt/homebrew/opt/openjdk/bin/java java; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -version >/dev/null 2>&1; then
    JAVA="$(command -v "$cand")"
    break
  fi
done
if [ -z "$JAVA" ]; then
  echo "SKIP: no working JVM found (TLA+ specs not checked)"
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

TLC() {
  # -deadlock: all specs are safety-only; several model terminating pipelines
  #            where reaching a terminal state is expected, not an error.
  "$JAVA" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
    -workers auto -cleanup -deadlock "$@"
}

# --- run ---------------------------------------------------------------------
specs=("$@")
if [ ${#specs[@]} -eq 0 ]; then
  for d in "$SPECS_DIR"/*/; do
    specs+=("$(basename "$d")")
  done
fi

pass=0 fail=0
printf '%-14s %-18s %-10s %-10s %s\n' SPEC CONFIG EXPECTED ACTUAL RESULT

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

  # SANY parse gate first — a spec that does not parse fails everything.
  if ! (cd "$dir" && "$JAVA" -cp "$JAR" tla2sany.SANY "$module" >/dev/null 2>&1); then
    printf '%-14s %-18s %-10s %-10s %s\n' "$name" "(SANY)" parse error 'FAIL ✗'
    fail=$((fail + 1))
    continue
  fi

  for cfg in "$dir"/MC*.cfg; do
    [ -e "$cfg" ] || continue
    cfgname="$(basename "$cfg" .cfg)"
    case "$cfgname" in
      MCBait* | MCMutation* | MCFinding*) expected=violate ;;
      *) expected=green ;;
    esac

    out="$(cd "$dir" && TLC -config "$cfgname.cfg" "$module" 2>&1)"
    rc=$?
    if [ $rc -eq 0 ]; then
      actual=green
    elif echo "$out" | grep -q 'Error: Invariant\|Error: Action property\|is violated'; then
      actual=violate
    else
      actual="error($rc)"
    fi

    if [ "$actual" = "$expected" ]; then
      printf '%-14s %-18s %-10s %-10s %s\n' "$name" "$cfgname" "$expected" "$actual" 'ok'
      pass=$((pass + 1))
    else
      printf '%-14s %-18s %-10s %-10s %s\n' "$name" "$cfgname" "$expected" "$actual" 'FAIL ✗'
      echo "$out" | tail -20 | sed 's/^/    | /'
      fail=$((fail + 1))
    fi
  done

  rm -rf "$dir/states"
done

echo
echo "specs: $pass ok, $fail failed"
[ $fail -eq 0 ]
