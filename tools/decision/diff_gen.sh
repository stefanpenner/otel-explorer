#!/usr/bin/env bash
# Compare generated (arg1, arg2) against committed (arg3, arg4).
# Strips the DO-NOT-EDIT header so absolute sandbox paths in comments
# cannot cause false staleness failures.
set -euo pipefail

gen_go="$1"
gen_test="$2"
src_go="$3"
src_test="$4"

strip_header() {
  # Keep from the package clause onward (stable, path-free).
  sed -n '/^package /,$p' "$1"
}

fail=0
if ! diff -u <(strip_header "$src_go") <(strip_header "$gen_go"); then
  echo >&2 "STALE: $src_go does not match codegen from Decision.tla"
  fail=1
fi
if ! diff -u <(strip_header "$src_test") <(strip_header "$gen_test"); then
  echo >&2 "STALE: $src_test does not match codegen from Decision.tla"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo >&2 "Update with: bazel run //tools/decision:update"
  exit 1
fi
