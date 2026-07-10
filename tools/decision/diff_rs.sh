#!/usr/bin/env bash
# Compare generated Rust decision module to the committed crate source.
# Args: <gen_spec.rs> <committed.rs>
set -euo pipefail
gen="$1"
src="$2"
if ! diff -u "$src" "$gen"; then
  echo "Rust decision core stale: $src" >&2
  echo "Run: bazel run //tools/decision:update" >&2
  exit 1
fi
