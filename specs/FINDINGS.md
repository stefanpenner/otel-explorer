# TLA+ Campaign Findings — 2026-07-05 (fixed 2026-07-06)

7 specs, all model-checked with TLC.
Every finding below had a failing config with a witness trace in the spec's
README. Every finding was independently re-run and graded by two adversarial
auditors (TLC re-run + spec↔code conformance).

Verdicts: **CONFIRMED** = auditor named a concrete real-world trigger.
**PLAUSIBLE** = mechanism verified in code, trigger less certain.

Green TLC at these bounds = strong bug hunt, not proof.

## Fix status — 2026-07-06

All 14 findings addressed via TDD (a failing Go test derived from each TLC
witness trace, then the fix). Every spec was re-aligned to model the **fixed**
code: fix invariants moved into the green `MC.cfg`, the old bug kept reachable
behind a `Bug*` constant so an `MCMutation*` config still fails — the invariant
keeps its teeth. `scripts/check-specs.sh` → **56 configs, 0 failed**.
Whole repo: `go test -race` clean, `bazel test` clean, e2e smoke 11/11.

| # | Finding | Status | Regression test |
|---|---------|--------|-----------------|
| 1 | Stale jobs forever after re-attempt | ✅ fixed — `UpsertJobs` attempt guard | `TestSyncBoundsSpec_StaleWorkerCannotStompNewerAttempt` |
| 2 | Permanent listing gap on clock jump | ✅ fixed — `since` computed once | `TestSyncBoundsSpec_ClockJumpMidSyncKeepsListingWindow` |
| 3 | Stale log-fetch mis-attributed | ✅ fixed — job-id+generation tag | `TestTuiReloadSpec_*` |
| 4 | Pending runs counted as failed | ✅ fixed — three-way classify | `TestGhaLifecycleSpec_*` |
| 5 | Dangling retry links | ✅ fixed — link only emitted attempts | `TestGhaLifecycleSpec_*` |
| 6 | Queue time for non-completed jobs | ✅ fixed — gate on completion | `TestGhaLifecycleSpec_*` |
| 7 | Timed-out attempt = success | ✅ fixed — shared conclusion helper | `TestGhaLifecycleSpec_*` |
| 8 | Child spans escape parent bounds | ✅ fixed — `clampSpanToParent` both paths | `TestTimingClampSpec_*` |
| 9 | 24h heal reuses poisoned data | ✅ fixed — exclude ends > threshold | `TestTimingClampSpec_*` |
| 10 | Dedup not idempotent / order-sensitive | ✅ fixed — per-group resolution | `TestDedupSpec_Idempotent`, `TestDedupSpec_OrderInsensitive` |
| 11 | Parent-cycle spans vanish | ✅ fixed — reachability promotion | `TestTreeSpec_ParentCycleSpansReachable` |
| 12 | Log spans inverted on non-monotonic ts | ✅ fixed — sort+clamp+forward-only | `TestLogGroupsSpec_*` |
| 13 | Limiter stampede / sleeper no-recheck | ⚠️ partial — recheck loop added | `TestRateLimitSpec_WakeRechecksLimiter` |
| 14 | Negative rate-limit header | ✅ fixed — clamp to 0 | `TestRateLimitSpec_NegativeHeadersHardened` |

**Finding 13 is intentionally partial.** The sleeper-recheck half is fixed
(`WakeRechecksLimiter` green). The residual — a post-reset stampede where a
woken sleeper rechecks a *stale* limiter because a competing consumer's headers
haven't been delivered yet — **cannot be fixed client-side** and is kept
honestly as failing `rate-limit/MCFinding1` + `MCFinding2`. Blast radius is low
(bounded by the semaphore, self-corrects via backoff). See the rate-limit README.

The simplifications and verified-safe results below still hold; the analyzer
simplifications (shared `isPending`, shared conclusion helper, dead-code
removal) were folded into the fixes for findings 4–9.

---

## Bugs (fix recommended)

### 1. Stale jobs served forever after re-attempt — CONFIRMED
Spec: `sync-bounds` (MCFinding1)
Code: `pkg/store/store.go` UpsertJobs (no attempt guard), `pkg/store/sync.go:109-112`

- In-flight job-fetch worker holds captured attempt N.
- Run re-attempts to N+1; another writer stores correct N+1 jobs.
- Stale worker lands: deletes good jobs, writes stale/none, sets `jobs_fetched=1`.
- Store attempt == upstream attempt now, so the reset CASE (store.go:164-166)
  can never fire again. Stale detail served forever.
- Trigger: `ote sync` concurrent with URL analysis, or two ote processes
  (RWMutex is per-process; SQLite is shared).

**Fix**: pass the fetched-for attempt into `UpsertJobs`;
guard the DELETE and the `jobs_fetched=1` UPDATE with `WHERE run_attempt = ?`.

### 2. Permanent listing gap on clock jump mid-sync — CONFIRMED (marginal)
Spec: `sync-bounds` (MCFinding2)
Code: `pkg/githubapi/client.go:721` (fresh `time.Now()` in listing), `sync.go:36`

- `since` is recomputed from a fresh `time.Now()` inside the listing call,
  not the `now` captured at sync start.
- If the clock advances ≥ ~1 day in between (laptop suspend mid-sync),
  a day of runs is skipped; the watermark then clamps it out permanently.
- Diagnostic config proves it: same bounds with since-computed-once are green.

**Fix**: compute `since` once in `Sync`, pass it down.

### 3. Stale log-fetch result mis-attributed — CONFIRMED
Spec: `tui-reload` (MCFinding1)
Code: `pkg/tui/results/model.go:175-178, 360`, `logfetch.go:62-65`

- `LogFetchResultMsg` carries no job ID and no generation.
- Handler attributes any arriving result to the *current* `logFetchingJobID`.
- Reload completion resets the fetched-set, a new fetch starts,
  then the OLD goroutine's result arrives and is accepted:
  log spans computed against the old span set, applied to the new one.

**Fix**: tag `LogFetchResultMsg` with job ID + reload generation;
discard on mismatch. (No cancellation needed.)

### 4. Pending runs counted as failed — CONFIRMED
Spec: `gha-lifecycle` (MCFinding2, 2-state witness)
Code: `pkg/analyzer/analyzer.go:296-300`

- Any run still queued / in_progress lands in the `else` branch → `FailedRuns`.
- Completed runs with empty conclusion too.
- Job level has careful `isPending` handling; run level has none.

**Fix**: three-way classify: pending / failed (completed && !success) / success.

### 5. Dangling retry links when an attempt fetch fails — CONFIRMED
Spec: `gha-lifecycle` (MCFinding1)
Code: `pkg/analyzer/analyzer.go:324` (silent `continue`), `:609-623`, `:732-746`

- `/attempts/{n}/jobs` fetch fails → attempt span never emitted.
- Retry link to that span is added unconditionally anyway.
- Emitted trace contains a link to a span that does not exist;
  at RunAttempt ≥ 3 the chain also loses contiguity.

**Fix**: track emitted attempts; only link to spans that exist (or log + skip).

### 6. Queue time counted for jobs that never completed — CONFIRMED
Spec: `gha-lifecycle` (MCFinding3)
Code: `pkg/analyzer/analyzer.go:852, 890` (comment at :851 says the opposite)

**Fix**: add `!isPending` (or `Status == "completed"`) to the queue-time
and Queued-span gates, matching the discipline at :826.

### 7. Timed-out previous attempt reported as success — CONFIRMED
Spec: `gha-lifecycle` (MCFinding4)
Code: `pkg/analyzer/analyzer.go:697-701`

- Attempt-conclusion derivation checks failure/cancelled but not `timed_out`.
- A previous attempt whose jobs timed out gets a success span,
  inconsistent with :826 which counts timed_out as failed.

**Fix**: handle `timed_out` in the derivation; extract shared
`conclusionFromJobs` helper (also used by the latest-attempt path).

### 8. Exported child spans escape parent bounds — CONFIRMED
Spec: `timing-clamp` (MCFinding1–3)
Code: `pkg/analyzer/analyzer.go:784-786, 831, 1046-1051, 1092`; ms path :818/:830/:934

- Only upper bounds are clamped; starts are never raised to parent start.
- The `+1` end floors can push ends past the parent after clamping.
- The ms/flamechart path uses the RAW job start (analyzer.go:818) —
  unclamped anywhere — so the escape there is unbounded.
- This is the same bug family commit 9ad24f9 fixed one instance of.

**Fix**: shared `clamp(start, end, parentStart, parentEnd)` used by
OTel, ms, and perfetto paths; re-clamp after the `+1` floors.

### 9. 24h sanity recalc reuses poisoned data — CONFIRMED
Spec: `timing-clamp` (MCFinding5)
Code: `pkg/analyzer/analyzer.go:376-382`

- The recalc re-maxes over the same `CompletedAt` field that
  triggered the >24h anomaly. A poisoned job end defeats the heal.

**Fix**: cap `maxJobEnd` at `runStart + 24h` (or exclude ends past threshold).

### 10. Runner dedup is not idempotent and is order-sensitive — CONFIRMED
Spec: `span-tree` (MCFinding1, MCFinding3)
Code: `pkg/analyzer/dedup.go:24-41`; comment `tree.go:93` ("Idempotent") is false

- Second dedup pass drops the api twin that the first pass kept.
- Survivor set depends on the arrival order of same-ID spans.
- Real bug family: commits c676de2, 1a5310c fought this before.

**Fix**: resolve per (traceID, spanID) GROUP: collapse only when the group
is exactly {1 api, 1 runner}; else keep all. Deterministic + idempotent.
Pin with a property test `dedup(dedup(x)) == dedup(x)`.

### 11. Parent-cycle spans vanish silently — CONFIRMED
Spec: `span-tree` (MCFinding2)
Code: `pkg/analyzer/tree.go:257-271`

- Two spans naming each other as parent link as each other's children.
- Neither is a root → unreachable from the forest → silently invisible.
- Hostile/corrupt OTLP input can produce this.

**Fix**: after linking, reachability pass from roots;
promote one node per unreachable cycle (or drop with a log line).

### 12. Log group spans inverted / mis-nested on non-monotonic timestamps — CONFIRMED
Spec: `log-groups` (MCFinding1–4)
Code: `pkg/logparse/timestamp.go:50-56, 109-117, 131-140, 218`

- Last group span end set to stepEnd unconditionally → span [start 2, end 1].
- Nested `##[group]` silently discards the outer group and its lines.
- Gap-parsed children extend past their parent.
- Out-of-order line drags the gap baseline backward → spurious splits.

**Fixes**: sort groups by start in parseWithGroups + clamp the override;
implicit-close outer group (end = line time) instead of discarding;
clamp children into [start, endTime]; only advance `current.end` forward.

---

## Design findings — low severity, fix optional

### 13. Rate limiter: post-reset stampede + sleeper no-recheck
Spec: `rate-limit` (MCFinding1, MCFinding2)
Code: `pkg/githubapi/client.go:371, 428`

- All waiters stampede into a fresh window with no unit reservation;
  a woken sleeper fires without rechecking the limiter.
- Blast radius LOW: bounded by semaphore (≤5), self-corrects via backoff.

**Optional fix**: loop sleep→recheck in `waitIfNeeded`,
or reserve a unit under the lock before sending.

### 14. Hardening note (fuzz backlog, not a state-machine bug)
- Limiter stores negative rate-limit headers verbatim
  (`strconv.Atoi("-1")`); `waitDuration` tests `== 0` exactly,
  so a negative header disables waiting.

---

## Verified-safe (negative results worth keeping)

- **Seeded runs cannot corrupt sync bounds** (`sync-bounds` MCSeedGap green):
  the synced=0 discipline holds at model scope once the window slide is fixed.
- **TUI channel-supersession race is unreachable** (`tui-reload` mutations):
  the `isLoading` key guard (model.go:463-469) is load-bearing —
  removing it deadlocks listeners and reverts displays. Do not remove.
- **Sync retry-reset works** (`sync-bounds` MCSingle green + mutation):
  the store.go:164-166 CASE reset is the load-bearing protection.
- **Analyzer skip-branches at :833-835 and :1095-1097 are dead code**
  (`timing-clamp` greens): the `+1` floors make `end <= start` impossible.

---

## Simplifications (spec-backed)

- `analyzer.go`: define `isPending` once (inlined at :807, re-derived :773/:852/:890).
- `analyzer.go:638` vs `:689-701`: one shared conclusion-from-jobs function.
- `analyzer.go:833-835, 1095-1097`: dead code — delete (or fix the floors first, see #8).
- Sanity scan vs extend scan (`:361-370` / `:376-382`): recalc set is a proven
  superset for the lower bound only — factor with care (see audit note in spec README).
- `logparse/timestamp.go:180`: `regionStart` param of parseGapBased is unused — delete.
- `dedup.go` group-based rewrite (fix #10) removes the order-dependence special cases.
- span-tree: the discovery's "re-validate parents after filtering" hint is
  NOT needed — nodes map is already built post-filter (proven in spec).

---

## Property ownership

Invariants labeled `code-documented` cite an assert / test / comment.
Everything labeled `proposed` is the campaign's suggestion —
**the user owns these**; review before treating a proposed-invariant
violation as a must-fix.

## How to re-check

```
scripts/check-specs.sh              # all specs
scripts/check-specs.sh sync-bounds  # one spec
```

Skips gracefully when no JVM is present.
