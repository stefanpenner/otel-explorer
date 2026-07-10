# GhaLifecycleDecision — job classification gates

Scalar state machine for **one** job's pending / failed / queue gates in
`processJob` (`pkg/analyzer/analyzer.go`). Specgen-compatible subset.

Full multi-attempt design model: `../GhaLifecycle.tla`.

## Purpose

Pin the pure classification guards:

1. TLC checks pending never failed + queue only completed (MC.cfg green)
2. Bait fails (MCBait) — proves exploration
3. Bug drops `!isPending` (MCMutation) — invariant teeth
4. `specgen` → `pkg/analyzer/ghalifecyclespec` (never hand-edit)
5. Dual: `isJobPending` / FailedJobs / countsQueueTime vs Classify*

## Action → code map

| Action | Code |
|--------|------|
| `ClassifyPending` | `isJobPending` → `PendingJobs` |
| `ClassifyFailed` | `countsFailed` (`!isPending && failure\|timed_out`) |
| `ClassifyQueue` | `countsQueue` / `countsQueueTime` |
| `Reset` | clear counts; advance scenario to completed failure/timeout/success |

`IsPending` here is `~hasCompletedAt` — models the completed-but-no-timestamp
fault (`isJobPending`: `status != completed \|\| CompletedAt == ""`).

## Correct sequence

1. Init — failure, no `completed_at` → pending
2. `ClassifyPending` — lands in PendingJobs
3. `ClassifyFailed` / `ClassifyQueue` **disabled** (faithful)
4. `Reset` — completed + timed_out
5. `ClassifyFailed` — FailedJobs++
6. `ClassifyQueue` — queue sample recorded

Under `Bug=TRUE`, step 3 enables fail+queue while still pending
→ `PendingNeverFailed` fails (`MCMutation.cfg`).

## Invariants

| Invariant | Faithful | Bait | Mutation (Bug) |
|-----------|----------|------|----------------|
| TypeOK | green | — | — |
| PendingNeverFailed | green | — | **RED** |
| QueueOnlyNotPending | green | — | (fails if checked) |
| BaitNeverPending | — | **RED** | — |

## Reproduce

```sh
cd specs/gha-lifecycle/decision
tlc --parse Decision.tla
tlc -c MC.cfg Decision.tla          # green
tlc -c MCBait.cfg Decision.tla      # MUST FAIL
tlc -c MCMutation.cfg Decision.tla  # MUST FAIL

specgen -const Bug=FALSE \
  -o ../../../pkg/analyzer/ghalifecyclespec -p ghalifecyclespec \
  Decision.tla
```

## Generated module

`pkg/analyzer/ghalifecyclespec` — regenerate, never hand-edit.
Dual: `pkg/analyzer/gha_lifecycle_decision_dual_test.go`.
