# sync-bounds — gates (agent SSOT)

Load this for code changes. Full TLC for multi-run sync: `SyncBounds.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Accept jobs write | `acceptJobsAttempt` → **AcceptAllowed** (+ SQL) | OfferNewer / OfferOlder | `AcceptAllowed`, `NoStaleAccepted` |
| Package | `pkg/store` | `decision/Decision.tla` | `syncboundsspec` |

**SSOT:** production `acceptJobsAttempt` calls generated `AcceptAllowed`.  
**Rule:** accept if `incoming==0` (unknown) or `incoming==stored`. SQL is atomic twin.

**Check:** `scripts/decision-check.sh`
