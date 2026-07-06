# TimingClamp — run/job/step time-clamping spec

**Status 2026-07-06: FINDINGS #8, #9 FIXED; the spec models the fixed
code.** All five finding invariants are GREEN in `MC.cfg`. Each finding
keeps a mutation knob (`Bug*` constant) that restores its specific
pre-fix behavior and turns its invariant RED (`MCMutation1..5`).

- **FIXED (Finding #8): a shared `clampSpanToParent` helper**
  (analyzer.go:826) is now applied on BOTH the OTel path AND the
  ms/flamechart path, for jobs (analyzer.go:884) and steps
  (analyzer.go:1120). It forces a child into `[parentStart, parentEnd-1]`
  for start and `[start+1, parentEnd]` for end — a >=1 ms sliver at the
  parent edge, never escaping, never zero-width. This closes the OTel
  end-escape (`ExportedChildEndInParent`), the missing start clamp
  (`ExportedChildStartInParent`), and the unbounded ms-path escape
  (`MsChildWithinParent`). The 1 ms sliver also leaves room for the
  perfetto exporter's own 1 ms minimum, so the export never re-escapes.
- **FIXED (Finding #9): the 24h sanity recalc excludes poisoned ends.**
  When re-deriving `maxJobEnd` (analyzer.go:390-403) the loop only
  considers job ends `<= threshold` (`runStartTs + 24h`), and caps at
  the threshold when no valid end remains — so a completed job's own
  bogus CompletedAt (the value that tripped the anomaly) can no longer
  defeat the heal. Closes `SanityHeals24h`.
- **Also fixed earlier (Finding #4): bounds heals inverted UpdatedAt.**
  `runEnd` is reset to `runStart+1ms` when UpdatedAt is missing OR not
  after runStart (analyzer.go:352-357). Closes `RunBoundsOrdered`.

## Purpose

Model the time-clamping pipeline that turns raw GitHub run/job/step
timestamps into emitted spans:

- `pkg/analyzer/analyzer.go:341-404` (run bounds, extend, 24h sanity)
- `pkg/analyzer/analyzer.go:826` (`clampSpanToParent`, the shared clamp)
- `pkg/analyzer/analyzer.go:867-885,1092-1093,1009-1015` (job clamp + spans)
- `pkg/analyzer/analyzer.go:1101-1123,1138-1139,1173-1179` (step clamp + spans)
- `pkg/perfetto/perfetto.go:44-54` (export clamp + 1ms minimum)

Question checked: does ANY reachable hostile input (missing, inverted,
future-dated, child-escaping-parent times; clock skew) make the pipeline
emit a span violating containment (start <= end, child inside parent)?

Contract source: commit 9ad24f9 "Clamp child span times to parent
bounds in flamechart" + the `clampSpanToParent` contract at
analyzer.go:820-825.

## Behavior sketch (fixes the atomicity grain)

1. bounds:  runStart=CreatedAt=1, runEnd=UpdatedAt=3
2. extend:  completed job CompletedAt=4 > 3 → runEnd:=4
3. sanity:  run completed, 4-1 <= Threshold → no recalc
4. jobs:    clampSpanToParent([2,4],[1,4]) → [2,4]; both paths [2,4]
5. steps:   clampSpanToParent([2,3],[2,4]) → [2,3]; both paths [2,3]
6. export:  perfetto passes spans through (each already a sliver inside)

Grain: one atomic action per pipeline phase (real code is
single-threaded over an immutable snapshot; per-job/per-step loops
commute). All hostile-input nondeterminism is in Init; the pipeline is
deterministic in its input.

Times: tiny int domain 0..Tmax, 0 = missing. `now` free in 1..Tmax
(clock skew both ways). Threshold models the 24h bound.

`ClampSpan(start, end, pStart, pEnd)` mirrors `clampSpanToParent`
(analyzer.go:826) exactly:

```
pe  = IF pEnd <= pStart THEN pStart + 1 ELSE pEnd   (degenerate parent gets room)
cs  = IF start < pStart THEN pStart
      ELSE IF start > pe - 1 THEN pe - 1
      ELSE start
ce0 = IF end > pe THEN pe ELSE end
ce  = IF ce0 < cs + 1 THEN cs + 1 ELSE ce0
return <<cs, ce>>
```

## Action → code map (line numbers as of 2026-07-06)

| Action    | Code |
|-----------|------|
| Init      | raw API fields + `time.Now()` (hostile input) |
| DoBounds  | analyzer.go:341-357 (runStart=CreatedAt; runEnd=UpdatedAt, or runStart+1ms if missing OR `!After(runStart)` — Finding #4 fix) |
| DoExtend  | analyzer.go:374-383 (completed jobs extend runEnd) |
| DoSanity  | analyzer.go:385-404 (24h recalc; scans ALL jobs, no status filter; Finding #9 fix excludes ends > threshold at :393, caps at threshold :401-403) |
| DoJobs    | analyzer.go:867-885 (skip if no StartedAt; CompletedAt→now fallback :875-880; `clampSpanToParent` :884 drives BOTH the OTel span :1092-1093 AND the ms event :1009-1015 — identical clamped values) |
| DoSteps   | analyzer.go:1101-1123 (skip if missing times; `clampSpanToParent` :1120 into [jobStartTs,jobEndTs]; OTel span :1138-1139; ms event :1173-1179) |
| DoExport  | perfetto.go:44-54 (endNs<startNs → startNs :48-49; non-marker 1ms minimum :51-52) |

Not modeled (documented drops):

- RunAttempt>1 RunStartedAt override (analyzer.go:347-351) — single-attempt runs
- processPreviousAttempt path — same clamp shape, wfEnd from max job CompletedAt
- earliestTime/earliestNs shift — subtracting a common constant preserves containment
- concurrent snapshot mutation — impossible (single-threaded over fetched data)
- unparseable-but-present strings ≡ missing (same skip/fallback paths)
- queued OTel spans (workflow-queued, job-queued) use raw unclamped
  times — out of scope here

## Invariants

| Invariant | Kind | Citation | Result |
|-----------|------|----------|--------|
| TypeInvariant | — | — | green |
| ExportedSpansOrdered | code-documented | perfetto.go:48-52 | green |
| JobClampedToRun | code-documented | analyzer.go:820-825 (clampSpanToParent) | green; RED under Bug=TRUE (MCMutation) |
| ExportedChildEndInParent | code-documented | commit 9ad24f9 contract | **Finding #1 fixed 2026-07-06** — green; teeth via MCMutation1 (BugClamp=TRUE) |
| ExportedChildStartInParent | PROPOSED (inferred from commit 9ad24f9 title/intent; body only claims end-time clamping) | commit 9ad24f9 title | **Finding #2 fixed 2026-07-06** — green; teeth via MCMutation2 (BugClamp=TRUE) |
| MsChildWithinParent | code-documented | analyzer.go:820-825, 881-883, 1118-1119 comments | **Finding #3 fixed 2026-07-06** — green; teeth via MCMutation3 (BugClamp=TRUE) |
| RunBoundsOrdered | proposed | intent of :352-357 | **Finding #4 fixed 2026-07-06** — green; teeth via MCMutation4 (BugBounds=TRUE) |
| SanityHeals24h | proposed | intent of comment :386-389 | **Finding #5 fixed 2026-07-06** — green; teeth via MCMutation5 (BugSanity=TRUE) |
| SanityNeverBelowValidCompletedJobEnd | proposed | — | green (recalc keeps max end <= threshold ⊇ any threshold-valid completed-job end) |
| JobSkipIsDeadCode | proposed | — | green (job skip guard unreachable) |
| StepSkipIsDeadCode | proposed | — | green (step skip guard unreachable) |

Note on `SanityNeverBelowValidCompletedJobEnd`: the pre-#9 form (no
`<= threshold` qualifier) is INTENTIONALLY false under the fix — the #9
fix deliberately drops a completed job's own poisoned (>threshold)
CompletedAt. That dropping IS Finding #5's fix, so the invariant is
qualified to threshold-valid ends, not weakened away.

## Mutation knobs (pre-fix behaviors; faithful = all off)

| Knob | TRUE restores | Trips |
|------|---------------|-------|
| `Bug` | job phase does NO parent clamp (raw passthrough, both paths) | `JobClampedToRun` (MCMutation) |
| `BugClamp` | pre-#8 clamp: end-only Min2 to runE, RAW ms start, +1 floors | `ExportedChildEndInParent`, `ExportedChildStartInParent`, `MsChildWithinParent` (MCMutation1/2/3) |
| `BugBounds` | pre-fix bounds: only a MISSING UpdatedAt healed (inverted survives) | `RunBoundsOrdered` (MCMutation4) |
| `BugSanity` | pre-#9 recalc: all ends counted, no threshold cap | `SanityHeals24h` (MCMutation5) |

`BugClamp` is one flag tripping three invariants (findings #1/#2/#3 all
came from not using the shared clamp); each keeps its own config
asserting its own invariant.

## Configs, bounds, results (2026-07-06, TLC2 / tla2tools.jar, 4 workers)

All: Tmax=4 (times 0..4, 0=missing), Threshold=2.
State space: 2,000,000 initial states × 7 phases.

| Config | Knob | Expected | Actual | States gen / distinct | Time |
|--------|------|----------|--------|-----------------------|------|
| MC.cfg | faithful (all off) | green | green (all 11 invariants) | 14,000,000 / 14,000,000 | 14s |
| MCBait.cfg | faithful | FAIL (bait) | FAIL `BaitNoStepEmitted` | ~12,002,564 | 12s |
| MCMutation.cfg | Bug=TRUE | FAIL | FAIL `JobClampedToRun` | ~12,004,004 | 11s |
| MCMutation1.cfg | BugClamp=TRUE | FAIL | FAIL `ExportedChildEndInParent` | ~12,002,613 | 11s |
| MCMutation2.cfg | BugClamp=TRUE | FAIL | FAIL `ExportedChildStartInParent` | ~12,005,064 | 11s |
| MCMutation3.cfg | BugClamp=TRUE | FAIL | FAIL `MsChildWithinParent` | ~12,002,611 | 13s |
| MCMutation4.cfg | BugBounds=TRUE | FAIL | FAIL `RunBoundsOrdered` | ~12,150,004 | 13s |
| MCMutation5.cfg | BugSanity=TRUE | FAIL | FAIL `SanityHeals24h` | ~12,012,542 | 12s |

State counts for FAILING runs are approximate: with 4 workers, TLC
stops early on violation, so the generated-state count varies run to run.

**Green TLC at these bounds = strong bug hunt, not proof.**

## Bait witness (BaitNoStepEmitted MUST FAIL — proves non-vacuity)

Input: rCreated=1, rUpdated=0(missing), jStart=1, jEnd=0(in-progress),
now=1, sStart=1, sEnd=1. Pipeline: runS=1, runE=2 (UpdatedAt missing →
+1); job emitted, step emitted. Final state has stepEmit=TRUE → bait
violated. One full correct behavior, 7 states deep.

## Mutation witnesses (essence)

**MCMutation (Bug=TRUE) — job clamp disabled.**
A raw job end past runEnd (e.g. runE=2, jEnd=4) passes through
unclamped: jobE=4 > runE=2 → `JobClampedToRun` violated.

**MCMutation1 (BugClamp) — exported child end escapes parent.**
With the pre-fix clamp, a child clamped to EXACTLY the parent end
(jobS=jobE=runE) hits the perfetto 1 ms minimum (perfetto.go:51-52),
which bumps xJobE to runE+1 > xWfE=runE.

**MCMutation2 (BugClamp) — child starts before parent.**
The pre-fix clamp only bounded the end side (Min2 to parent), so a
child start below the parent start (e.g. rCreated=2, jStart=1) survives:
xJobS=1 < xWfS=2.

**MCMutation3 (BugClamp) — ms/flamechart escape (UNBOUNDED).**
The pre-fix ms path used the RAW job start and a +1-floor end, so a raw
StartedAt past runEnd puts the whole ms event beyond the parent —
escape magnitude is raw-start minus runEnd, not 1 ms.

**MCMutation4 (BugBounds) — run bounds stay inverted.**
rCreated=3, rUpdated=1 (UpdatedAt < CreatedAt), no completed job:
runS=3, runE=1 at done → `RunBoundsOrdered` violated.

**MCMutation5 (BugSanity) — 24h recalc reuses poisoned data.**
rDone, rCreated=1, completed job with CompletedAt=4: extend → runE=4;
sanity triggers (4-1 > Threshold=2) but the pre-fix recalc recomputes
maxJobEnd=4 — the same poisoned value → `SanityHeals24h` violated.

## Assessment

All five findings are closed by the shipped code and the faithful model
proves it: `MC.cfg` checks all five finding invariants plus the existing
green invariants and passes at these bounds; each mutation config still
fails its target invariant, so the invariants keep their teeth. Unlike
the rate-limit residual, no finding here survives the fix — the shared
`clampSpanToParent` and the threshold-excluding recalc close them
outright.

## Reproduce

```sh
cd specs/timing-clamp
java -cp ~/.cache/tla2tools.jar tla2sany.SANY TimingClamp.tla
java -XX:+UseParallelGC -cp ~/.cache/tla2tools.jar tlc2.TLC \
  -workers 4 -cleanup -config MC.cfg -deadlock TimingClamp.tla
# likewise for MCBait.cfg, MCMutation.cfg, MCMutation{1..5}.cfg
```

(`-deadlock` because the pipeline terminates by design at pc="done".)

Or run the whole gate: `scripts/check-specs.sh timing-clamp`.
