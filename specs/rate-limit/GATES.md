# rate-limit — gates (agent SSOT)

Load this for code changes. Full TLC only for multi-goroutine races: `RateLimit.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Must wait before send | `rateLimitWaitNeeded` | StartSleep / WaitNeeded | `WaitNeeded` |
| Recheck after sleep | `waitIfNeeded` loop | WakeRecheckSend / Resleep | — |
| Package | `pkg/githubapi` | `decision/Decision.tla` | `ratelimitspec` |

**Rule:** after sleep, recheck; do not fire blind while still exhausted.

**Check:** `bazel test //pkg/githubapi:githubapi_test --test_filter=RateLimit|Pure`
