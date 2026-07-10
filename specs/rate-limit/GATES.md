# rate-limit — gates (agent SSOT)

Load this for code changes. Full TLC only for multi-goroutine races: `RateLimit.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Must wait before send | `rateLimitWaitNeeded` → **WaitNeeded** | StartSleep / WaitNeeded | `WaitNeeded` |
| Recheck after sleep | `waitIfNeeded` loop | WakeRecheckSend / Resleep | — |
| Package | `pkg/githubapi` | `decision/Decision.tla` | `ratelimitspec` |

**SSOT:** production `rateLimitWaitNeeded` calls generated `WaitNeeded`.  
**Rule:** after sleep, recheck; do not fire blind while still exhausted.

**Check:** `scripts/decision-check.sh`
