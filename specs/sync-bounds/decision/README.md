# SyncBoundsDecision — UpsertJobs attempt-guard decision core

Scalar state machine for **one** stale-vs-current attempt decision in
`UpsertJobs` / `acceptJobsAttempt`. Specgen-compatible subset.

Full multi-run design model: `../SyncBounds.tla`.

## Purpose

Pin the pure guard so production code cannot drift:

1. TLC checks faithful no-stomp (MC.cfg green)
2. Bait fails (MCBait) — proves exploration
3. Bug accepts older (MCMutation) — invariant teeth
4. `specgen` → `pkg/store/syncboundsspec` (never hand-edit)
5. Dual: store attempt-guard scenarios vs OfferOlder / OfferNewer

## Action → code map

| Action | Code |
|--------|------|
| `Store1` / `Store2` / `Store3` | `UpsertRuns` writes `run_attempt` |
| `OfferNewer` | `UpsertJobs` with `attempt == run_attempt` → accept |
| `OfferOlder` | `UpsertJobs` with stale attempt → discard (Bug=TRUE: stomp) |
| pure `AcceptAllowed` | production `acceptJobsAttempt` (SSOT) |

## Correct sequence (no stomp)

1. `Store1` — store attempt 1
2. `Store2` — concurrent bump to attempt 2
3. `OfferOlder` — stale worker still holding attempt 1 → `accepted=FALSE`

Matching write: `Store2` → `OfferNewer` → `accepted=TRUE`, `incoming=2`.

Under `Bug=TRUE`, step 3 sets `accepted=TRUE` with `incoming < stored`
→ `NoStaleAccepted` fails (`MCMutation.cfg`).

## Invariants

| Invariant | Faithful | Bait | Mutation (Bug) |
|-----------|----------|------|----------------|
| TypeOK | green | — | — |
| NoStaleAccepted | green | — | **RED** |
| BaitNeverAccepted | — | **RED** | — |

## Reproduce

```sh
cd specs/sync-bounds/decision
tlc --parse Decision.tla
tlc -c MC.cfg Decision.tla          # green
tlc -c MCBait.cfg Decision.tla      # MUST FAIL
tlc -c MCMutation.cfg Decision.tla  # MUST FAIL

specgen -const Bug=FALSE \
  -o ../../../pkg/store/syncboundsspec -p syncboundsspec \
  Decision.tla
```

## Generated module

`pkg/store/syncboundsspec` — regenerate, never hand-edit.
Dual: `pkg/store/sync_bounds_decision_dual_test.go`.
