# gha-lifecycle — gates (agent SSOT)

Load this for code changes. Full TLC for multi-attempt runs: `GhaLifecycle.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Job still pending | `isJobPending` | ClassifyPending | — |
| Count as failed | `countsFailed` | ClassifyFailed | `PendingNeverFailed` |
| Count queue time | `countsQueue` | ClassifyQueue | `QueueOnlyNotPending` |
| Package | `pkg/analyzer` | `decision/Decision.tla` | `ghalifecyclespec` |

**Rule:** pending never failed; queue only non-pending (and not skipped/cancelled).

**Check:** `scripts/decision-check.sh`
