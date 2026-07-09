# sync-bounds — gates (agent SSOT)

Load this for code changes. Full TLC for multi-run sync: `SyncBounds.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Accept jobs write | `acceptJobsAttempt` (+ SQL) | OfferNewer / OfferOlder | `NoStaleAccepted` |
| Package | `pkg/store` | `decision/Decision.tla` | `syncboundsspec` |

**Rule:** accept if `incoming==0` (unknown) or `incoming==stored`. SQL is atomic twin.

**Check:** `bazel test //pkg/store:store_test --test_filter=SyncBounds|Pure`
