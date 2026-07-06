# RateLimit — shared rate limiter + retry/backoff spec

Models `rateLimiter` + `RateLimitedTransport.RoundTrip` in
`pkg/githubapi/client.go`.

**Status 2026-07-06:**
- **FIXED (FINDING 13, sleeper no-recheck half): `waitIfNeeded` now loops
  sleep → recheck** (client.go:361-379). Property `WakeRechecksLimiter`
  is green (MCRecheck); the pre-fix behavior lives behind `Bug2`
  (MCMutation2 must fail).
- **FIXED (FINDING 14, hardening): negative `x-ratelimit-remaining`
  clamps to 0, non-positive reset ignored** (client.go:404-417).
  `RemainingNonNegative` is green under the `NegHeader` fault (MC);
  the pre-fix behavior lives behind `Bug3` (MCMutation3 must fail).
- **STILL REAL: the stale-limiter stampede.** `NoOvershoot`
  (MCFinding1) and `NoSleeperOvershoot` (MCFinding2) remain violated —
  see the honest assessment below. No client-side fix can see a
  response that has not been delivered yet.

## Purpose

Design question:
can concurrent RoundTrip goroutines defeat the rate limiter?

The (partially fixed) suspect design:
- `waitDuration()` reads the limiter under the mutex (client.go:349-359)
- the caller sleeps OUTSIDE the lock (client.go:361-379)
- after waking, the loop RECHECKS the limiter (fixed 2026-07-06);
  pre-fix the request fired blind after the sleep

## One correct behavior (grain-fixing sketch)

1. g1 takes a semaphore slot; limiter unknown; sends.
2. Server consumes its last unit; headers say remaining=0, reset=R.
3. g1's response delivered; limiter := {remaining:0, reset:R}.
4. g2 takes a slot, sees exhausted + clock<R, sleeps until R.
5. Clock reaches R; server refills.
6. g2 wakes, rechecks (no wait needed), sends; server has quota; ok.

## Atomicity grain

- check-limiter + send = ONE action (`StartSend` / `RetrySend`).
  Sound: the real check→send window only adds interleavings of the
  same shape; the races of interest all survive the coarsening.
- check-limiter + go-to-sleep = ONE action (real `waitDuration` holds the mutex).
- wake + recheck + send = ONE action (`Wake`); the loop iterating back
  to sleep is `WakeResleep`. With `Bug2` the recheck is dropped
  (pre-fix wake-and-fire).
- send = arrival at the server (latency folded into scheduling).
- response COMPUTE (at arrival) and response DELIVERY are separate
  actions → stale-header delivery is representable.
- Tick advances a logical clock and refills the server window
  atomically when crossing `srvReset` (mirrors the +1s slack,
  client.go:355).

## Action → code map (line numbers as of 2026-07-06)

| Action          | Code                                                        |
|-----------------|-------------------------------------------------------------|
| `StartSend`     | client.go:434-440 (sem acquire; waitIfNeeded no-wait; send) |
| `StartSleep`    | client.go:434-437, 361-379 (sem acquire; sleep outside lock)|
| `Wake`          | client.go:361-379 loop: sleep returns, recheck finds no wait → send |
| `WakeResleep`   | client.go:361-379 loop: recheck finds a new exhausted window → sleep again |
| `DeliverOk`     | client.go:444 (updateFromHeaders), 466 (return)             |
| `DeliverRetry`  | client.go:444-453 (updateFromHeaders; shouldRetry; retryDelay 505-525) |
| `DeliverGiveUp` | client.go:446 loop exit at maxRetries; 466                  |
| `RetrySend`     | client.go:456-459 (pre-retry waitIfNeeded no-wait; retry)   |
| `RetrySleep`    | client.go:456 (pre-retry waitIfNeeded sleeps)               |
| `Cancel`        | client.go:382-391 (sleepContext ctx cancel; deferred sem release) |
| `Tick`          | wall clock + GitHub window reset                            |

## Mutation / fault knobs (faithful = all off)

| Knob | TRUE restores / injects | Config |
|------|-------------------------|--------|
| `Bug`  | waitIfNeeded check removed entirely | MCMutation (must fail `NoOvershoot`) |
| `Bug2` | no recheck after waking (pre-fix FINDING 13) | MCMutation2 (must fail `WakeRechecksLimiter`) |
| `Bug3` | no negative-header clamp (pre-fix FINDING 14) | MCMutation3 (must fail `RemainingNonNegative`) |
| `NegHeader` | fault: responses may carry `remaining = -1` | on in MC (clamped code stays green) + MCMutation3 |

## Faults modeled

- wake-together race (several sleepers, same wake time)
- stale response delivery (unconditional `updateFromHeaders`
  overwrite, client.go:401-424 — no staleness guard)
- context cancellation mid-sleep / mid-backoff
- server clock skew: header reset may be `Bogus` (beyond horizon);
  a sleeper on it never wakes — only Cancel frees it
- hostile negative `x-ratelimit-remaining` header (`NegHeader`)
- limited (429 / 403+headers) responses driving the retry loop

## Faults dropped

- `Retry-After: 0` → immediate retry: code filters it
  (`seconds > 0`, client.go:490) and falls back to exponential
  backoff, so modeled delays are always ≥ 1 tick.
- negative/zero `x-ratelimit-reset`: code ignores it
  (`seconds > 0`, client.go:415); the model's server only emits
  positive resets, so nothing to model.
- backoff shift overflow: data-level; maxRetries=5 makes it
  unreachable. Fuzz territory.
- 503 retries: `shouldRetry` (client.go:473-486) does NOT retry 503
  (contrary to earlier discovery notes) — only 429 and 403 variants.
  Both abstracted as one "limited" status.

## Invariants

| Invariant             | Source          | Citation / note                                   |
|-----------------------|-----------------|---------------------------------------------------|
| `TypeInvariant`       | spec hygiene    | `limRemaining ∈ -1..Quota` (the -1 only reachable via Bug3) |
| `RemainingNonNegative`| code-documented | **fixed 2026-07-06 (FINDING 14)**: clamp at client.go:404-410; green in MC under `NegHeader`; teeth via MCMutation3 |
| `SemaphoreBounded`    | code-documented | client.go:29, 149, 434-435                        |
| `BackoffCapped`       | code-documented | client.go:511-513, 519-521 (modeled as the clamp itself; low teeth by construction) |
| `WakeRechecksLimiter` | code-documented | **fixed 2026-07-06 (FINDING 13, recheck half)**: waitIfNeeded loop client.go:361-379; green in MCRecheck; teeth via MCMutation2 |
| `NoOvershoot`         | **PROPOSED**    | once headers seen, no request arrives at an exhausted server. STILL VIOLATED — kept as MCFinding1. |
| `NoSleeperOvershoot`  | **PROPOSED**    | narrower: no sleep-wake sender hits an exhausted server. STILL VIOLATED (stale-limiter variant) — kept as MCFinding2, see below. |
| `BaitNoSleeper`       | bait only       | must fail; proves exploration                     |

## Mutations

- `MCMutation` (`Bug=TRUE`): removes the `waitIfNeeded` check entirely.
  `MCSolo.cfg` (1 goroutine, Bug=FALSE, `NoOvershoot` ON) passes;
  `MCMutation.cfg` (identical, Bug=TRUE) fails. `waitIfNeeded` is the
  load-bearing client-side protection. (1-goroutine world because with
  ≥2 goroutines `NoOvershoot` already fails without the bug — that IS
  MCFinding1.)
- `MCMutation2` (`Bug2=TRUE`): drops the post-sleep recheck. Fails
  `WakeRechecksLimiter`: g1 exhausts the window; g2 sleeps on its
  reset; the window resets, g3 consumes it and its headers ARE
  delivered (limiter now says exhausted until the new reset); g2 wakes
  and fires anyway. The faithful model re-sleeps here (MCRecheck green).
- `MCMutation3` (`Bug3=TRUE` + `NegHeader`): stores a hostile `-1`
  verbatim. Fails `RemainingNonNegative` at depth ~3; a stored -1 also
  disables waiting entirely (`waitDuration` tests `== 0`).

## Configs, bounds, results (2026-07-06, tla2tools, 4 workers)

Shared bounds unless noted: Gs={g1,g2,g3} (symmetry set where legal),
MaxConcurrency=2, MaxClock=5, Quota=1, ResetDelay=2, BackoffCap=2,
MaxAttempts=1, MaxReqs=1, deadlock check off (terminal states are
expected: clock horizon + all goroutines done).

| Config          | Expected | Actual | States gen / distinct | Time |
|-----------------|----------|--------|-----------------------|------|
| `MC.cfg`        | pass     | PASS (NegHeader on)  | 36,152 / 11,113 | <1s |
| `MCBait.cfg`    | fail     | FAIL `BaitNoSleeper` | 146 / 88 | <1s |
| `MCSolo.cfg`    | pass     | PASS   | 354 / 214 (Gs={g1}, MaxConcurrency=1, MaxReqs=2) | <1s |
| `MCMutation.cfg`| fail     | FAIL `NoOvershoot` | 66 / 50 (solo bounds, Bug=TRUE) | <1s |
| `MCMutation2.cfg`| fail    | FAIL `WakeRechecksLimiter` | 22,573 / 10,463 (no symmetry, Bug2=TRUE) | <1s |
| `MCMutation3.cfg`| fail    | FAIL `RemainingNonNegative` | 34 / 34 (solo bounds, Bug3+NegHeader) | <1s |
| `MCRecheck.cfg` | pass     | PASS   | 81,906 / 26,702 (no symmetry) | <1s |
| `MCFinding1.cfg`| fail     | FAIL `NoOvershoot` | 1,420 / 781 | <1s |
| `MCFinding2.cfg`| fail     | FAIL `NoSleeperOvershoot` | 2,873 / 1,470 | <1s |

Green TLC at these bounds = strong bug hunt, not proof.
(No SYMMETRY in MCRecheck/MCMutation2: TLC forbids symmetry with
temporal/action properties.)

## Finding traces (still-real residuals)

### MCFinding1 — fresh-window stampede

1. clock 0: g1 sends; server 1→0; response headers (remaining=0, reset=2).
2. Tick, Tick → clock 2; server refills to 1.
3. g1's (now stale) response delivered: limiter = {0, reset=2}, known.
4. clock ≥ reset → `waitDuration` returns 0 for everyone.
5. g2 sends; server 1→0 (new window reset=4).
6. g3 sends; server exhausted; limiter informed → **overshoot**.

### MCFinding2 — woken sleeper on a STALE limiter (post-fix residual)

1. clock 0: g1 sends; server 1→0; headers (remaining=0, reset=2).
2. g1's response delivered; limiter = {0, reset=2}.
3. g2 starts: exhausted + clock<2 → sleeps until 2 (outside the lock).
4. Tick → clock 2; server refills to 1.
5. g3 starts fresh: `waitDuration`=0 (reset passed) → sends; consumes
   the unit. Its headers are NOT yet delivered.
6. g2 wakes, RECHECKS: limiter still says {0, reset=2}, reset passed →
   no wait → fires → arrives at the exhausted server →
   **sleepOvershoot**.

The recheck fix (2026-07-06) closed the *informed* wake-together race
(g3's headers delivered before g2's wake → g2 re-sleeps; that is
exactly the MCMutation2/MCRecheck pair). The residual violation above
requires the competing consumer's response to be undelivered at the
recheck instant — a stale-limiter stampede, the same family as
MCFinding1, and unfixable client-side: the client cannot act on a
response it has not received. Honest verdict: `NoSleeperOvershoot` as
literally stated does NOT go green from the recheck alone; it is kept
failing on purpose rather than weakened.

## Assessment / blast radius (residual findings)

The violated properties are PROPOSED, not code-documented. Severity
against GitHub in practice: **low, self-correcting**.

- Overshoot is bounded by the semaphore (≤ MaxConcurrency = 5
  extra requests per reset boundary, worst case).
- GitHub answers such requests with 403/429 + headers; the transport
  backs off and retries; the limiter re-learns the true state on the
  first delivered response.
- The recheck fix removes the worst repeat offender: sleeping herds
  now re-queue instead of firing into a window the limiter already
  knows is exhausted.
- If further tightening is ever wanted: reserve a unit under the lock
  (decrement local `remaining` before sending) — shrinks but cannot
  eliminate the undelivered-response window.

## Reproduce

```sh
cd specs/rate-limit
java -XX:+UseParallelGC -cp ~/.cache/tla2tools.jar tlc2.TLC \
  -workers 4 -cleanup -config MC.cfg RateLimit.tla
# likewise for MCBait / MCSolo / MCRecheck / MCMutation* / MCFinding*
```

(`java` = `/opt/homebrew/opt/openjdk/bin/java`; do not commit `states/`.)
