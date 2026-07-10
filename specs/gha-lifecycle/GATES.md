# gha-lifecycle — gates (agent SSOT)

Load this for code changes. Full TLC for multi-attempt runs: `GhaLifecycle.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Job still pending | `isJobPending` | ClassifyPending | — |
| Count as failed | `countsFailed` → **CanClassifyFailed** | ClassifyFailed | `PendingNeverFailed` |
| Count queue time | `countsQueue` → **CanClassifyQueue** (+ skip/cancel) | ClassifyQueue | `QueueOnlyNotPending` |
| Package | `pkg/analyzer` | `decision/Decision.tla` | `ghalifecyclespec` |

**SSOT:** fail/queue pending-half via generated guards (`hasCompletedAt = !pending`).  
Skip/cancel filter stays in production (outside decision domain).  
**Rule:** pending never failed; queue only non-pending (and not skipped/cancelled).

**Check:** `scripts/decision-check.sh`
