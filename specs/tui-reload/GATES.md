# tui-reload — gates (agent SSOT)

Load this for code changes. Full TLC only for race redesign: `TuiReload.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Fresh log-fetch result | `logFetchResultFresh` → **CanFetchAccept** | FetchAccept / FetchDiscard | `NoStaleAccepted` |
| Package | `pkg/tui/results` | `decision/Decision.tla` | `tuireloadspec` |

**SSOT:** production calls generated `CanFetchAccept` (after msg job match).  
**Rule:** accept only if job matches in-flight and `msgGen == reloadGen`.

**Rust peer:** `gates::log_fetch_result_fresh` → `tui_reload` module.  
**Check:** `scripts/decision-check.sh`
