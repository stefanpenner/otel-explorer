# SyncBounds — store sync-bounds & attempt-invalidation spec

TLA+ model of the incremental-sync protocol in
`pkg/store/store.go` + `pkg/store/sync.go`.

**Status 2026-07-06: both findings FIXED in code; the spec now models the
fixed code.** The pre-fix behaviors are kept reachable behind mutation
knobs (`Bug2`, `FreshNowAtListing`) so MCMutation1/MCMutation2 prove the
invariants keep their teeth.

## Purpose

Three design questions:

- Can seeded (`synced=0`) runs clamp future listing windows
  and create permanent history gaps?
- Can a concurrent writer leave stale attempt-N job detail
  marked `jobs_fetched=1` forever after a re-attempt?
- Does a crash between listing and job-fetch corrupt the bounds?
  (Partial progress persists — every SQL statement is its own tx.)

## One correct behavior (fixes the grain)

1. Run appears upstream (day 1).
2. Sync reads watermark, then oldest        — 2 SQL queries.
3. Sync lists upstream window `[floor, now]` — 1 HTTP snapshot.
4. Upserts listed runs, `synced=1`           — 1 SQL tx.
5. Reads `RunsNeedingJobs`                   — 1 SQL query.
6. Fetches jobs per run, `jobs_fetched=1`    — HTTP + 1 SQL tx each.
7. Run re-attempted upstream.
8. Next listing re-lists it at attempt 2 inside the overlap window;
   the upsert CASE resets `jobs_fetched=0`; jobs refetched.

## Atomicity grain

- Each SQLite transaction = one atomic action (SQLite serializes).
- The HTTP listing = one atomic snapshot of upstream
  (pagination tearing abstracted).
- Job fetch (HTTP) + `UpsertJobs` (tx) coarsened into ONE action.
  Sound because the only visible effect is the written row, and the
  split form's extra interleavings only change WHICH wrong
  jobs-attempt value is written (stale vs filtered-empty) — the
  properties treat both identically.
- `Watermark` and `OldestRun` are separate queries → two actions
  (their TOCTOU gap is modeled).

## Data abstraction

`created_at` is a concrete tiny int (1..3) — the window arithmetic
IS the bug target. Status, ids, durations, job content abstracted.
All runs are completed at appearance.

## Action → code map (line numbers as of 2026-07-06)

| Action            | Code                                             |
|-------------------|--------------------------------------------------|
| `Tick`            | wall clock (1 unit = 1 day = 24h overlap unit)   |
| `Appear(r)`       | new run on GitHub, `created = clock`             |
| `ReAttempt(r)`    | run re-run, `run_attempt+1`                      |
| `Seed(r)`         | `SeedRuns` store.go:137-139, cmd/ote/main.go:1955 (payload has jobs: `CollectCompletedRunData`) |
| `ReadWM(s)`       | sync.go:39 (`now := timeNow()`), sync.go:50 → store.go:295-305 |
| `ReadOldest(s)`   | sync.go:54 → store.go:429-441                    |
| `ListRuns(s)`     | window calc sync.go:58-70 + `since` computed ONCE from the captured now sync.go:74-76, passed to `FetchWorkflowRunsSince` (sync.go:77, client.go:724-737) |
| `UpsertListed(s)` | sync.go:86 → store.go:141-225 (one tx, `synced=1`, no jobs payload) |
| `ReadNeedJobs(s)` | sync.go:92-96 → store.go:309-330 (NB: SQL has NO `synced=1` filter) |
| `FetchJob(s,r)`   | worker sync.go:110-147 + store.go:379-425 (attempt guard 390-400); attempt filter trends.go:756-761 |
| `FetchJobFail(s,r)` | sync.go:128-134 (failed fetch, run stays `jf=0`) |
| `SyncDone(s)`     | sync.go:148-157                                  |
| `Crash(s)`        | process crash fault; committed txs persist       |

`UpsertRow` mirrors the SQL upsert store.go:154-168
(CASE reset 164-166, `run_attempt=excluded` 167, `synced=max` 168).

## Mutation knobs (pre-fix behaviors, faithful = all off)

| Knob | TRUE restores | Fixed |
|------|---------------|-------|
| `Bug` | attempt-bump `jobs_fetched` reset removed (store.go:164-166 CASE) | never a real bug (mutation only) |
| `Bug2` | `UpsertJobs` attempt guard removed (pre-fix FINDING 1) | 2026-07-06, store.go:390-400 |
| `FreshNowAtListing` | listing `since` from a fresh `time.Now()` (pre-fix FINDING 2) | 2026-07-06, sync.go:74-77 + client.go:724-737 |

## Faults modeled

- Crash at any mid-sync point (partial progress persists).
- Two concurrent sync processes (`Syncers = {s1, s2}`).
- Re-attempt while a job fetch is in flight.
- Seeder writing between watermark read and listing (TOCTOU).
- Job fetch failure mid-loop.
- Clock advancing days mid-sync (suspend); the window slide it used
  to cause is fixed — reachable only via `FreshNowAtListing=TRUE`.

Not modeled: clock skew (wm > now impossible here: `created <= clock`
by construction; sync.go:62-66 guard not exercised), status
transitions (all runs completed), SQLite WAL crash mid-tx (ACID
assumed: a tx is atomic or absent), pagination tearing, run deletion
upstream.

## Invariants / properties

| Name | Kind | Source |
|------|------|--------|
| `TypeInvariant` | invariant | — |
| `NoStaleFetchedJobs` | invariant | code-documented: store.go:150-153, store_test.go:243-268; **FINDING 1 fixed 2026-07-06** (store.go:390-400 attempt guard) — green in MC/MCSingle, teeth via MCMutation1 |
| `NoPermanentGap` | invariant | PROPOSED; emergent from the store.go:101-105 comment; **FINDING 2 fixed 2026-07-06** (`since` computed once) — green in MC/MCSeedGap, teeth via MCMutation2 |
| `SyncedMonotone` | action prop | code-documented: store.go:168 `synced=max(...)`, runs never deleted |
| `AttemptBumpNoStaleCarry` | action prop | code-documented: store.go:150-153, 164-166 |
| `BaitNoRunEverNeedsJobs` | bait | must fail — proves exploration |

## Configs and results (2026-07-06, TLC2 2.19 / tla2tools.jar, 4 workers)

| Config | Bounds | Expected | Actual | States gen / distinct | Time |
|--------|--------|----------|--------|-----------------------|------|
| MC.cfg | 2 runs, 2 syncers, seeding, faithful | green | green | 34,884,485 / 4,818,863 | 30s |
| MCBait.cfg | as MC | FAIL (bait) | FAIL `BaitNoRunEverNeedsJobs` | 5,923 / 2,405 | <1s |
| MCSingle.cfg | 2 runs, 1 syncer, no seeder | green | green | 53,170 / 17,581 | <1s |
| MCMutation.cfg | as MCSingle, Bug=TRUE | FAIL | FAIL `NoStaleFetchedJobs` | 24,756 / 9,110 | <1s |
| MCMutation1.cfg | 2 runs, 1 syncer, seeder on, Bug2=TRUE | FAIL | FAIL `NoStaleFetchedJobs` | 21,534 / 7,385 | <1s |
| MCMutation2.cfg | 3 runs, 1 syncer, FreshNowAtListing=TRUE | FAIL | FAIL `NoPermanentGap` | 403,859 / 138,856 | 1s |
| MCSeedGap.cfg | 3 runs, seeder on, faithful | green | green | 13,988,427 / 2,180,601 | 11s |

All bounds: `MaxTime=3, MaxAttempt=2, Days=3, Overlap=1`.

**Green TLC at these bounds = strong bug hunt, not proof.**

## Bait trace (MCBait)

`Appear(r1)` → `ReadWM` → `ReadOldest` → `ListRuns` →
`UpsertListed` → `ReadNeedJobs` ⇒ `pend[s] = {r1}` — the full
pipeline is explored.

## Mutation check (MCMutation, Bug=TRUE)

Disables the store.go:164-166 CASE reset (jf always `max`).
Single syncer, no seeder: list r1@att1 → fetch jobs (ja=1) →
upstream re-attempt → next listing @att2 keeps `jf=1` →
`att=2=upstream, ja=1` ⇒ `NoStaleFetchedJobs` violated. Teeth
confirmed.

## FINDING 1 — UpsertJobs stomps a concurrent attempt bump — FIXED 2026-07-06

Pre-fix, `UpsertJobs` set `jobs_fetched=1` unconditionally with no
attempt guard. Witness (now MCMutation1, Bug2=TRUE):

1. r1 appears (att 1). Sync lists it @att1, upserts, reads
   `RunsNeedingJobs = {r1}`.
2. Upstream re-attempts r1 (att 2).
3. URL analysis seeds r1 @att2 WITH att-2 jobs
   (`SeedRuns`; upsert CASE fires: `att=2, jf=1, ja=2`). Store correct.
4. The sync's in-flight worker now fetches jobs for r1 with captured
   attempt 1: API returns latest-attempt jobs, `ConvertJobs(_, 1)`
   filters them ALL out (trends.go:759), and pre-fix `UpsertJobs`
   deleted the good att-2 jobs, inserted nothing, set `jobs_fetched=1`.
5. Final: `att=2 = upstream att, jf=1, ja=∅`. The CASE reset could
   never fire again ⇒ empty/stale job detail served forever.

**Fix** (store.go:379-425): `UpsertJobs` takes the fetched-for attempt;
one guarded statement `UPDATE runs SET jobs_fetched=1 WHERE ... AND
(?=0 OR run_attempt=?)` runs FIRST, and when it matches no row the
whole write (including the jobs DELETE) is skipped. attempt 0 =
unknown (run not in this sync's listing; the payload is unfiltered
latest-attempt jobs) → accepted unconditionally, which the model shows
is safe: `ja=up.att` at write time can never disagree with an equal
`db.att`. Regression tests:
`store_test.go TestSyncBoundsSpec_StaleWorkerCannotStompNewerAttempt`
(both witness shapes; failed pre-fix).

`NoStaleFetchedJobs` is now green in MC.cfg (2 syncers + seeder + full
fault model) and MCSingle; MCMutation1 (Bug2=TRUE) still fails it.

## FINDING 2 — listing window slides mid-sync — FIXED 2026-07-06

Pre-fix, `fetchDays` was derived from the `now` captured at sync start
but the listing recomputed `since` from a fresh `time.Now()` inside
the client. Witness (now MCMutation2, FreshNowAtListing=TRUE):

1. r1 appears day 1; a sync lists+upserts it (crashes before job
   fetch — irrelevant). r2 appears day 1 AFTER that listing snapshot.
2. New sync captures `now=day2`, reads `wm=1`; window floor 1,
   span 2 days.
3. Machine sleeps: clock → day 3; r3 appears (day 3).
4. Listing runs with `since = day3 - 2 = day 2` → lists only r3,
   misses r2 (day 1).
5. Upsert → `wm=3, oldest=1`. Every future window clamps to
   `wm-overlap = 2` > r2.created ⇒ r2 permanently unsyncable.

**Fix**: the listing API is now
`FetchWorkflowRunsSince(ctx, owner, repo, since, ...)`
(client.go:724-737; interface pkg/githubapi/provider.go); `Sync`
computes `since` once from its captured now (sync.go:74-77) and passes
it down. The trends path (`AnalyzeTrends`, trends.go:271) computes its
own since at call time — same behavior as before, and it has no
watermark to poison. Regression tests:
`sync_test.go TestSyncBoundsSpec_ClockJumpMidSyncKeepsListingWindow`
(timeNow seam simulates the suspend; failed pre-fix) and
`client_test.go TestSyncBoundsSpec_ListingWindowFromCaller`
(the `created` qualifier is the caller's since as a UTC date).

`NoPermanentGap` is now green in MC.cfg and MCSeedGap; MCMutation2
(FreshNowAtListing=TRUE) still fails it.

## Positive result — seeded runs never create gaps (MCSeedGap)

With the (now shipped) slide fix and the seeder active,
`NoPermanentGap` holds: the `synced=0` seeding discipline
(store.go:101-106, watermark/oldest filter `synced=1`) does its job.
The store_test.go behavior generalizes to arbitrary interleavings at
these bounds.

## Running

```sh
cd specs/sync-bounds
java -cp ~/.cache/tla2tools.jar tla2sany.SANY SyncBounds.tla
java -XX:+UseParallelGC -cp ~/.cache/tla2tools.jar tlc2.TLC \
     -workers 4 -cleanup -config MC.cfg SyncBounds.tla
```

(java = `/opt/homebrew/opt/openjdk/bin/java`. Do not commit `states/`.)
