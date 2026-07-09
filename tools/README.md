# tools/

## Decision-core codegen (TLA+ → Go)

Hermetic Bazel pipeline (no PATH `specgen` required):

| Target | Purpose |
|--------|---------|
| `//tools/specgen` | Codegen binary (vendored) |
| `//tools/decision:up_to_date` | Fail if committed `*spec` ≠ codegen |
| `//tools/decision:update` | Write codegen into `pkg/.../*spec/` |

```bash
# After editing specs/*/decision/Decision.tla:
bazel run //tools/decision:update
bazel test //tools/decision:up_to_date
bazel test //...
```

`bazel test //...` includes the up-to-date suite (CI unit-test job).

See `specs/DECISION_CORES.md`.
