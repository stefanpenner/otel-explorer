# tui-reload — gates (agent SSOT)

Load this for code changes. Full TLC only for race redesign: `TuiReload.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Fresh log-fetch result | `logFetchResultFresh` | FetchAccept / FetchDiscard | `NoStaleAccepted` |
| Package | `pkg/tui/results` | `decision/Decision.tla` | `tuireloadspec` |

**Rule:** accept only if `msgJob == fetchingJob` and `msgGen == reloadGen`.

**Check:** `bazel test //pkg/tui/results:results_test --test_filter=LogFetch|TuiReload|Pure`
