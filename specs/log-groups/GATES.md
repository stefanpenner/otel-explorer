# log-groups — gates (agent SSOT)

Load this for code changes. Full TLC for timestamps/gaps: `LogGroups.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| May open group | `canOpenGroup` | Open | — |
| May close group | `canCloseGroup` | Close | `Inv_DepthNonNeg` |
| Package | `pkg/logparse` | `decision/Decision.tla` | `loggroupsspec` |

**Rule:** never underflow depth (stray endgroup is no-op).

**Check:** `bazel test //pkg/logparse:logparse_test --test_filter=GroupStack|LogGroups|Pure`
