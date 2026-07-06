# TuiReload — TLA+ spec for the results-TUI reload / log-fetch protocol

Models `pkg/tui/results` (bubbletea) as ONE message-driven state machine.

## Purpose

Two async ops race:

- reload — `doReload` goroutine + `listenForProgress` command chain
- log fetch — `fetchLogsForCurrentItem` command

Design question:

- Is the documented intent enforced —
  "any in-flight fetch result is ignored" after reload (model.go:407-409)?
- Does the `isLoading` key guard (model.go:473-479) fully prevent the
  stale-channel hazard when a second reload would overwrite
  `progressCh` / `resultCh`?

**Answer (updated 2026-07-06): the fetch-supersession intent is now
ENFORCED in code — campaign finding #3 was FIXED by tagging
`LogFetchResultMsg` with (jobID, reload generation) at command creation
(logfetch.go:59-66) and discarding mismatched results (model.go:365-372).
The faithful model is `MC.cfg` and passes; the pre-fix behavior is kept
reachable behind `Bug2 = TRUE` (`MCMutation3.cfg`, must FAIL).
The channel hazard IS prevented by the key guard (`MC*` pass; removing
the guard in `MCMutation*.cfg` breaks it).**

## One correct behavior (informal)

1. Press `L`: fetch job X starts, `logFetchingJobID = X`,
   command captures current spans and tags the msg (X, gen 0).
2. Press `r`: reload gen 1 starts (`isLoading = TRUE`, fresh channels,
   one listen command).
3. Reload goroutine done; listen delivers `ReloadResultMsg`.
4. Handler: spans := gen-1 data, `reloadGen := 1`,
   `logFetchingJobID := 0`, `logFetchedJobIDs := nil`, channels nil'd.
5. Stale fetch-X result arrives tagged (X, gen 0):
   `gen 0 != reloadGen 1` → discarded — even if a NEW fetch of X is
   already in flight.

## Atomicity grain

- Each `Update(msg)` handler = one atomic action (bubbletea guarantee).
- Concurrency = which in-flight command delivers next.
- Coarsened (commutes with everything else):
  - channel read + Update = one action (read value fixed at read time)
  - fetch compute + deliver + Update = one action
    (msg content — job, spans, gen — fixed at command CREATION,
    logfetch.go:59-66)
- Kept separate: reload goroutine completion (`GoroutineDone`) — it
  changes what a listen reads (progress vs result).

Spans modeled by PROVENANCE, not count:
`displayedBase` (generation) + `displayedLogs` as `[job, base]` records.
Abstraction: every successful fetch returns >= 1 span.

## Action → code map

| Action            | Code                                                            |
|-------------------|-----------------------------------------------------------------|
| `PressReload`     | model.go:844-849, 705-711 (keys) + doReload model.go:1878-1901  |
| `PressFetch(j)`   | model.go:851-855 + logfetch.go:18-68 (tag stamped :59-66)       |
| `GoroutineDone`   | model.go:1888-1897 (close progressCh, buffer result)            |
| `ListenProgress`  | model.go:1904-1924 select + model.go:455-466 (re-listen on CURRENT chans) |
| `ListenResultOK`  | model.go:395-445 success path (replace spans, reloadGen++ :410, reset fetch state) |
| `ListenResultErr` | model.go:402-405 early return — fetch state NOT reset            |
| `FetchDeliverOK`  | model.go:364-393: staleness discard :365-372, mark job :384, append msg spans :387 |
| `FetchDeliverErr` | model.go:374-380 (flag cleared at 374 AFTER the discard, BEFORE err check) |

Constants:

- `Bug = TRUE` → mutation: removes the isLoading key guard
  (model.go:473-479).
- `Bug2 = TRUE` → mutation: strips the (job, gen) tag check —
  the pre-2026-07-06 code (finding #3, formerly `GenCheck = FALSE`).

## Invariants

| Invariant                  | Source          | Citation                          |
|----------------------------|-----------------|-----------------------------------|
| `TypeInvariant`            | —               | —                                 |
| `AtMostOneReloadRunning`   | code-documented | guard model.go:473-479, 707       |
| `NoStaleLogSpans`          | code-documented | model.go:366-370, 407-409; was finding #3, FIXED 2026-07-06 |
| `FetchedJobsHaveTheirSpans`| PROPOSED        | no cross-attribution of results   |
| `DisplayedIsLatestSuccess` | PROPOSED        | display = latest successful reload |
| `NoStuckListen`            | PROPOSED        | no listen blocked forever (resultCh never closed, model.go:1881) |
| `BaitNoLogsDisplayed`      | bait            | must fail                         |

## Configs, bounds, results (TLC 2.19, 4 workers, 2026-07-06)

All runs < 1 s.  Bounds: `Jobs = {j1,j2}`, `MaxGen = 2`
(MCLarge: `{j1,j2,j3}`, `MaxGen = 3`).

| Config            | Models                     | Expected | Actual                                | States gen/distinct | Depth |
|-------------------|----------------------------|----------|----------------------------------------|---------------------|-------|
| `MC.cfg`          | **code as-is (fixed)**     | pass     | pass                                    | 885 / 248           | 13    |
| `MCLarge.cfg`     | fixed code, bigger bounds  | pass     | pass                                    | 23,165 / 4,360      | 19    |
| `MCMutation3.cfg` | pre-fix code (Bug2=TRUE)   | FAIL     | FAIL `NoStaleLogSpans`                  | 140 / 79            | 7-state trace |
| `MCMutation.cfg`  | fixed, key guard removed   | FAIL     | FAIL `AtMostOneReloadRunning`           | 20 / 14             | 3-state trace |
| `MCMutation2.cfg` | fixed, guard removed, direct guard invariant omitted | FAIL | FAIL `NoStuckListen` | 146 / 88 | 6-state trace |
| `MCBait.cfg`      | fixed code                 | FAIL     | FAIL `BaitNoLogsDisplayed`              | 18 / 15             | 3-state trace |

The config inversion from the 2026-07-05 campaign is resolved: `MC.cfg`
IS the faithful model of the code, and the finding's config
(`MCFinding1.cfg`) became the mutation `MCMutation3.cfg`.

## FIXED finding #3 (was MCFinding1, now MCMutation3): stale log-fetch result attributed to a new fetch

Pre-fix, `LogFetchResultMsg` carried no job ID and no generation.  The
handler attributed ANY arriving result to whatever fetch was currently
in flight.  Reload completion reset `logFetchingJobID` to 0, which
UNBLOCKED starting a new fetch while the old fetch goroutine still ran
(nothing cancels it).

Witness trace (TLC, 7 states — encoded as
`TestTuiReloadSpec_StaleFetchResultDiscarded` in
`pkg/tui/results/logfetch_test.go`):

1. `PressFetch(j1)` — fetch j1 starts against base gen 0
2. `PressReload` — reload gen 1 starts
3. `GoroutineDone(1)`
4. `ListenResultOK(1)` — spans := gen 1, `logFetchingJobID := 0`,
   `logFetchedJobIDs := nil`
5. `PressFetch(j1)` — NEW fetch of j1 against base gen 1
   (allowed: fetched-set was reset)
6. `FetchDeliverOK([job j1, base 0])` — the STALE result from step 1
   arrives first: pre-fix it was accepted →
   `displayedLogs = {[job j1, base 0]}` with `displayedBase = 1`
   → **`NoStaleLogSpans` violated**

Pre-fix consequences in the real code:

- Log spans computed against the OLD span set were appended to the NEW
  reloaded spans.
- Cross-job variant: stale j1 result marked j2 as fetched
  (`FetchedJobsHaveTheirSpans` violated) and j2's REAL result was later
  discarded (`TestTuiReloadSpec_StaleFetchCrossJob`).
- Fetch-error variant: a stale FAILED result cleared `logFetchingJobID`
  so the new fetch's real result was silently dropped
  (`TestTuiReloadSpec_StaleFetchErrorDiscarded`).

THE FIX (2026-07-06): `LogFetchResultMsg` carries `jobID` + `gen`
stamped at command creation (logfetch.go:59-66); `m.reloadGen` is
bumped on every successful reload (model.go:410); the handler discards
any result whose tag mismatches (model.go:365-372).  No cancellation
needed.  `MC.cfg` / `MCLarge.cfg` green; `MCMutation3.cfg` (Bug2=TRUE)
still FAILS, proving the invariant keeps its teeth.

Also captured but NOT a violation: the discovery-claimed simpler race
(fetch result appended, then `ReloadResultMsg` replaces spans) reaches a
consistent final state — spans lost = wasted work only, since the reset
at model.go:411 allows re-fetch.  The model confirms "reload supersedes"
works in THAT ordering; pre-fix it failed only in the delayed-delivery
ordering above.

## Mutation traces (guard removed — discovery race (b))

`MCMutation`: `PressReload`, `PressReload` → two reload goroutines
running → `AtMostOneReloadRunning` fails.

`MCMutation2` (direct invariant omitted to show deeper damage):
reload 1 and reload 2 in flight; gen-1's listen command delivers a
progress msg and RE-LISTENS on the CURRENT channels — gen 2's
(model.go:465-466 + 1904-1906).  Now two listens on gen 2, zero on
gen 1.  Gen 2's result consumed by one; the other blocks forever
(progressCh closed/drained, resultCh never closed) →
`NoStuckListen` fails.  A separate run with only
`DisplayedIsLatestSuccess` shows gen-1's result applied AFTER gen-2's:
display reverts to a stale generation permanently.
Real code prevents ALL of this via the isLoading key guard.

## Bait trace (MCBait)

`PressFetch(j2)` → `FetchDeliverOK` → `displayedLogs ≠ {}` —
bait fails in 3 states.  TLC really explores fetch delivery.

## Modeling artifacts / dropped detail

- `fetches` is a SET: two in-flight fetches with identical (job, base)
  would collapse.  Unreachable in faithful configs (generations strictly
  increase, so a re-fetch always has a new base); only a corner in
  mutation-land.
- Progress msg CONTENT (phase/detail/url text) dropped — rendering only.
- Spinner (`spinner.TickMsg`, `logFetchInline`) dropped — rendering only.
- Buffered progress msgs remaining after channel close: coarsened away
  (they only extend the listen chain; result delivery is unaffected).
- `reloadError` string dropped; error/success kept as separate actions.

Green TLC at these bounds = strong bug hunt, not proof.

## Running

```sh
cd specs/tui-reload
/opt/homebrew/opt/openjdk/bin/java -cp ~/.cache/tla2tools.jar tla2sany.SANY TuiReload.tla
/opt/homebrew/opt/openjdk/bin/java -XX:+UseParallelGC -cp ~/.cache/tla2tools.jar \
  tlc2.TLC -workers 4 -cleanup -deadlock -config MC.cfg TuiReload.tla
```
