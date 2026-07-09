# GhaLifecycle — GHA run/job lifecycle → analyzer processing

TLA+ bug hunt for `pkg/analyzer/analyzer.go`:
metric derivation (Pending / Failed / Successful, queue time)
and retry-attempt sequencing / span-chain linking,
under a generous GitHub environment.

**Green TLC at these bounds = strong bug hunt, not proof.**

**Status 2026-07-06: findings 1–4 FIXED in code.** The spec now models
the fixed code; each finding's invariant moved into MC.cfg (green) and
the old buggy behavior stays reachable behind a `Bug*` constant so the
`MCMutation1–6` configs keep failing (invariants have teeth).

## Purpose

- Environment = GitHub: run/job status advances
  (queued → in_progress → completed + conclusion),
  empty conclusion on completed (fault),
  jobs still advancing after run completion (cancellation stragglers),
  re-runs bumping RunAttempt.
- Processor = analyzer: fetch run, fetch latest jobs,
  fetch attempts 1..N-1 (each fetch may fail → silent skip,
  analyzer.go:333), emit attempt spans with retry links
  (post-fix: only to spans that exist), derive job metrics.

Steps are abstracted (same status/conclusion machinery as jobs).
`neutral` conclusion dropped — same equivalence class as `success`
in every modeled branch. Timestamps abstracted to booleans
(`started`, `hasCompletedAt`).

## One correct behavior (atomicity grain)

1. GH: run queued → in_progress; attempt-1 job starts,
   completes (failure); run completes (failure).
2. GH: re-run → attempt 2; job completes (success);
   run completes (success).
3. Analyzer: fetch run snapshot + classify run (one action —
   classification is a pure function of the snapshot).
4. Analyzer: fetch latest jobs.
5. Analyzer: fetch + process attempt 1 (one action = loop body
   analyzer.go:329-341): emit attempt-1 span, count metrics.
6. Analyzer: process latest attempt, emit span with retry link
   → attempt 1. Done.

Grain: each HTTP fetch is one action; pure in-memory processing is
folded into the fetch that feeds it (reads only snapshots, so it
commutes with every environment action).

State reduction: environment actions are disabled once the analyzer
is `done` — no property reads live GitHub state after that point.

## Action → code map (line numbers as of the 2026-07-06 fixes)

| Action            | Code                                                        |
|-------------------|-------------------------------------------------------------|
| GhRunStart        | GitHub API behavior (run status advance)                    |
| GhRunComplete     | GitHub API; empty conclusion = fault the code must survive  |
| GhJobStart        | GitHub API (job starts; also after run completion)          |
| GhJobComplete     | GitHub API; faults: empty conclusion, missing completed_at  |
| GhReRun           | GitHub re-run bumps RunAttempt (analyzer assumes 1..N seq)  |
| AFetchRun         | analyzer.go:289-305 (fetch + three-way classification 295-305) |
| AFetchJobsLatest  | analyzer.go:316 (`FetchJobsPaginated` on /jobs)             |
| AFetchPrevOk      | analyzer.go:327-341 (emittedAttempts tracking) + processPreviousAttempt 697-780 (link gate :750) |
| AFetchPrevFail    | analyzer.go:333 `continue` (silent skip; 698 empty-result return is the same skip class — both return false, so no link ever targets them) |
| AProcessLatest    | analyzer.go:511-514 (jobs), 542-548 (conclusion fallback), 628-643 (gated link), 650-659 (span) |

Helper predicates: `IsPending` = isJobPending analyzer.go:816-818,
`CountsFailed` = 877 (Bug=TRUE drops the `!isPending` guard),
`CountsQueue` = countsQueueTime 904-906 (BugQueueUngated drops `!isPending`),
`Started` = 867, `DerivedConcl` = conclusionFromJobs 796-811
(BugTimedOutSuccess drops the timed_out arm),
`PendingOf` = 822-834 (classified before the never-started return;
BugUnstartedInvisible restores the old order).

## Invariants

| Invariant                     | Source           | Citation / basis                                   | Result    |
|-------------------------------|------------------|----------------------------------------------------|-----------|
| TypeInvariant                 | —                | typing                                             | green     |
| InvPendingNeverFailed         | code-documented  | comment analyzer.go:903 "Only count genuine failures" | green (fails w/ Bug=TRUE, MCMutation) |
| InvEmittedHaveConcl           | proposed         | sanity: emitted spans carry a conclusion           | green     |
| InvPendingRunNotFailed        | proposed         | Finding 2 — **fixed 2026-07-06** (analyzer.go:295-305) | green (fails w/ BugPendingRunFailed, MCMutation2) |
| InvLinkTargetsEmitted         | proposed         | Finding 1 — **fixed 2026-07-06** (analyzer.go:327/:629/:750) | green (fails w/ BugDanglingLink, MCMutation1) |
| InvQueueOnlyCompletedJobs     | proposed         | Finding 3 — **fixed 2026-07-06** (analyzer.go:924-926) | green (fails w/ BugQueueUngated, MCMutation3) |
| InvTimedOutAttemptNotSuccess  | proposed         | Finding 4 — **fixed 2026-07-06** (conclusionFromJobs :796-811) | green (fails w/ BugTimedOutSuccess, MCMutation4) |
| InvPendingJobsVisible         | proposed         | discovery note — **fixed 2026-07-06** (analyzer.go:853-864) | green (fails w/ BugUnstartedInvisible, MCMutation5) |
| InvCompletedRunSpanHasConclusion | proposed      | empty-conclusion fallback, part of Finding 4 fix (analyzer.go:546-548) | green (fails w/ BugNoConclFallback, MCMutation6) |
| InvBaitNoRetrySpan            | bait             | must fail (proves exploration)                     | fails ✓   |

The user owns correctness properties: all `proposed` invariants are
author-proposed and labeled as such; none were weakened to pass.
Code tests derived from the witness traces:
`pkg/analyzer/gha_lifecycle_spec_test.go` (TestGhaLifecycleSpec_*).

## Configs, bounds, results (TLC, 4 workers, `-deadlock`)

Numbers re-run 2026-07-06 against the fixed-code model.

| Config          | Bounds                  | Expected | Actual | States gen / distinct | Time |
|-----------------|-------------------------|----------|--------|------------------------|------|
| MC.cfg          | 2 attempts × 1 job, all findings' invariants | green | green | 1,437,669 / 644,256 (depth 13) | 2s |
| MC2.cfg         | 1 attempt × 2 jobs, symmetry | green | green | 171,355 / 94,851 (depth 10) | <1s |
| MCBait.cfg      | 2 attempts × 1 job      | FAIL     | FAIL   | 53,950 / 23,498        | <1s |
| MCMutation.cfg  | Bug=TRUE, 1 att × 1 job | FAIL     | FAIL   | 3,143 / 1,846          | <1s |
| MCMutation1.cfg | BugDanglingLink, 2 att × 1 job | FAIL | FAIL | 56,155 / 23,973       | <1s |
| MCMutation2.cfg | BugPendingRunFailed, 1 att × 1 job | FAIL | FAIL | 159 / 146          | <1s |
| MCMutation3.cfg | BugQueueUngated, 1 att × 1 job | FAIL | FAIL | 2,057 / 1,133          | <1s |
| MCMutation4.cfg | BugTimedOutSuccess, 2 att × 1 job | FAIL | FAIL | 30,947 / 14,755     | <1s |
| MCMutation5.cfg | BugUnstartedInvisible, 1 att × 1 job | FAIL | FAIL | 1,377 / 702      | <1s |
| MCMutation6.cfg | BugNoConclFallback, 1 att × 1 job | FAIL | FAIL | 1,996 / 1,104       | <1s |

Note: 2 attempts × 2 jobs was attempted and abandoned: >16M distinct
states at depth 10 with the queue still growing. The two green
configs jointly cover the retry dimension and the job-pair
dimension; the per-job logic has no cross-job coupling.

Run (inside this dir):

```
/opt/homebrew/opt/openjdk/bin/java -XX:+UseParallelGC \
  -cp ~/.cache/tla2tools.jar tlc2.TLC \
  -workers 4 -deadlock -cleanup -config MC.cfg GhaLifecycle.tla
```

(`-deadlock`: terminal states — run over, analyzer done — are
expected, not errors.)

## Bait witness (MCBait, 7 states)

run completes → GhReRun (attempt 2) → AFetchRun →
AFetchJobsLatest → AFetchPrevOk (emits attempt-1 span) →
AProcessLatest (emits attempt-2 span) ⇒ `emitted ⊇ {2}`,
violating "no retry span is ever emitted". Bait behaves.

## Mutation witness (MCMutation, 7 states)

Job completes with conclusion=failure but `completed_at` missing
(fault) → `IsPending` is TRUE; with the guard deleted (Bug=TRUE)
the job lands in both `countedPending` and `countedFailed`:
`countedPending = countedFailed = {<<1, j1>>}`. The `!isPending`
guard at analyzer.go:903 is load-bearing.

## Findings — ALL FIXED 2026-07-06 (kept as mutations)

1. **Dangling retry link / broken attempt chain** (now MCMutation1).
   Trace: re-run → analyzer fetches attempt 1, fetch FAILS
   (`continue`) → latest attempt span still linked to
   `previousAttemptSpanID(runID, 1)` which was never emitted.
   **Fix**: `emittedAttempts` map (analyzer.go:327); the latest
   link (:629) and previous-attempt links (`linkPrev`, :750) are
   gated on the target span existing; the empty-jobs return also
   reports not-emitted.
2. **Pending run counted as failed** (now MCMutation2).
   Trace (2 states): analyzer fetches a run still `queued` →
   `runCounted = "failed"`.
   **Fix**: three-way classification (analyzer.go:295-305) with a
   new additive `Metrics.PendingRuns` bucket; completed runs with
   empty conclusion still count failed.
3. **Queue time counted for non-completed jobs** (now MCMutation3).
   Trace: job starts (in_progress, conclusion "") → analyzer
   processes it → queue time counted.
   **Fix**: shared `countsQueueTime` gate (analyzer.go:924-926)
   adds `!isPending`, used by both the metric and the Queued-span
   emission — matching the FailedJobs discipline at :877.
4. **Timed-out attempt reported as success** (now MCMutation4).
   Trace: attempt-1 job completes `timed_out` → re-run →
   attempt span said `success` while FailedJobs counted it failed.
   **Fix**: shared `conclusionFromJobs` (analyzer.go:796-811),
   precedence failure > timed_out > cancelled > success; also used
   as the empty-conclusion fallback for the latest attempt's span
   (:542-548, MCMutation6 keeps that path's teeth).

Discovery note, also fixed: never-started (still queued) jobs were
invisible to PendingJobs (early return before classification).
processJob now classifies pending first (analyzer.go:853-864);
InvPendingJobsVisible / MCMutation5 pin it.

## Spec-derived simplification notes (implemented 2026-07-06)

- `isJobPending` is now one helper (analyzer.go:816-818), used by
  the pending classification, FailedJobs, and countsQueueTime.
- `conclusionFromJobs` is the single conclusion derivation for
  previous attempts and the latest-attempt empty-conclusion
  fallback — `AProcessLatest` and `AFetchPrevOk` now share
  `DerivedConcl` in the model too.
