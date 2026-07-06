-------------------------------- MODULE RateLimit --------------------------------
(***************************************************************************)
(* PURPOSE                                                                 *)
(*                                                                         *)
(* Model the shared GitHub rate limiter + retry/backoff protocol in        *)
(* pkg/githubapi/client.go (rateLimiter, RateLimitedTransport.RoundTrip).  *)
(*                                                                         *)
(* Design question: can concurrent RoundTrip goroutines defeat the rate    *)
(* limiter?  The sleep-outside-lock design: waitDuration() reads           *)
(* {remaining, resetTime} under the mutex (client.go:349-359) and the      *)
(* caller sleeps OUTSIDE the lock (client.go:361-379).                     *)
(* FIXED 2026-07-06 (FINDING 13, partial): waitIfNeeded now LOOPS          *)
(* sleep -> recheck (client.go:361-379), so a woken sleeper re-reads the   *)
(* limiter and sleeps again if a newly learned window is exhausted.        *)
(* Bug2=TRUE restores the pre-fix fire-without-recheck.  The recheck can   *)
(* NOT close the stale-limiter stampede (NoOvershoot / the undelivered-    *)
(* response variant of NoSleeperOvershoot) -- those remain documented      *)
(* findings, see MCFinding1/MCFinding2.                                    *)
(*                                                                         *)
(* ONE CORRECT BEHAVIOR (informal, fixes the atomicity grain):             *)
(*   1. g1 acquires a semaphore slot; limiter unknown -> sends.            *)
(*   2. Server consumes its last unit; response headers say               *)
(*      remaining=0, reset=R (future).                                     *)
(*   3. g1's response is delivered; limiter := {remaining:0, reset:R}.     *)
(*   4. g2 acquires a slot, sees remaining=0 & clock<R, sleeps til R.      *)
(*   5. Clock reaches R; server refills.                                   *)
(*   6. g2 wakes, sends; server has quota; response ok.                    *)
(*                                                                         *)
(* ATOMICITY GRAIN                                                         *)
(* - "check limiter + send" is ONE atomic action (StartSend/RetrySend).    *)
(*   The real code has a window between waitIfNeeded returning and         *)
(*   Base.RoundTrip; collapsing it is sound for the properties here        *)
(*   because the races of interest (wake-together, no-recheck-after-       *)
(*   sleep, stale delivery) all survive the coarsening, and the window     *)
(*   only ever ADDS interleavings of the same shape.                       *)
(* - "check limiter + go to sleep" is ONE atomic action (StartSleep):      *)
(*   waitDuration() really is atomic (holds the mutex).                    *)
(* - Wake(g): the loop's recheck + send is ONE atomic action (fixed code   *)
(*   rechecks waitDuration() under the mutex before firing); WakeResleep   *)
(*   models the loop iterating back to sleep.  With Bug2 the recheck is    *)
(*   dropped and Wake fires unconditionally (pre-fix).                     *)
(* - request send = arrival at server (network latency folded into         *)
(*   scheduling nondeterminism).  Response compute and response DELIVERY   *)
(*   are separate actions, so stale-header delivery is modeled: a          *)
(*   response computed at an older clock can be applied to the limiter     *)
(*   after the server state has moved on.                                  *)
(* - Time is a logical clock advanced by Tick; the server refill happens   *)
(*   atomically inside Tick when the clock crosses srvReset (mirrors the   *)
(*   +1s slack in client.go:355 -- a woken sleeper always observes a       *)
(*   refilled window if the header reset was accurate).                    *)
(*                                                                         *)
(* FAULTS MODELED                                                          *)
(* - wake-together race (2+ sleepers, same wakeAt)                         *)
(* - stale response delivery (limiter overwritten by old headers;          *)
(*   updateFromHeaders has no staleness guard, client.go:401-424)          *)
(* - context cancellation mid-sleep / mid-backoff (Cancel)                 *)
(* - server clock skew: header reset can be Bogus (beyond the horizon);    *)
(*   a sleeper on a Bogus reset never wakes (only Cancel frees it)         *)
(* - 429/403-style "limited" responses driving the retry loop              *)
(*                                                                         *)
(* FAULTS DROPPED (and why)                                                *)
(* - Retry-After: 0 -> immediate retry: the code filters it                *)
(*   (seconds > 0, client.go:490) and falls back to exponential backoff,   *)
(*   so backoff delays are always >= 1 tick here.                          *)
(* - backoff shift overflow (1<<attempt): data-level bug, maxRetries=5     *)
(*   makes it unreachable; fuzz territory, not a state-machine concern.    *)
(* - 503: shouldRetry (client.go:473-486) does NOT retry 503, contrary     *)
(*   to the discovery notes; only 429 and 403 variants. Both are modeled   *)
(*   as one abstract "limited" status.                                     *)
(*                                                                         *)
(* MUTATION KNOBS (each restores a pre-fix behavior; faithful = all off):  *)
(*   Bug  = TRUE disables the waitIfNeeded check entirely -- goroutines    *)
(*          send even when the limiter says exhausted (the most            *)
(*          load-bearing client-side protection).                          *)
(*   Bug2 = TRUE drops the recheck after waking (pre-fix FINDING 13,       *)
(*          fixed 2026-07-06: waitIfNeeded loops sleep -> recheck).        *)
(*   Bug3 = TRUE drops the negative-header clamp in updateFromHeaders      *)
(*          (pre-fix FINDING 14, fixed 2026-07-06: client.go:404-410       *)
(*          clamps remaining < 0 to 0; a stored negative would disable     *)
(*          waiting since waitDuration tests remaining == 0 exactly).      *)
(* FAULT KNOB: NegHeader = TRUE lets any response carry a hostile/buggy    *)
(*   x-ratelimit-remaining of -1 (proxy/server corruption).                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Gs,             \* set of RoundTrip goroutines (model values)
    MaxConcurrency, \* semaphore size (code: defaultMaxConcurrency=5, client.go:29,149)
    MaxClock,       \* logical-time horizon
    Quota,          \* server units per rate-limit window
    ResetDelay,     \* ticks from exhaustion to window reset
    BackoffCap,     \* retry backoff cap in ticks (code: maxRetryDelay, client.go:28)
    MaxAttempts,    \* retry budget (code: maxRetries=5, client.go:27)
    MaxReqs,        \* logical requests per goroutine (bounds the run)
    Bug,            \* TRUE => waitIfNeeded check removed (mutation)
    Bug2,           \* TRUE => no recheck after waking (pre-fix, mutation)
    Bug3,           \* TRUE => no negative-header clamp (pre-fix, mutation)
    NegHeader       \* TRUE => responses may carry remaining = -1 (fault)

ASSUME MaxConcurrency \in Nat \ {0}
ASSUME Quota \in Nat \ {0}
ASSUME ResetDelay \in Nat \ {0}
ASSUME BackoffCap \in Nat \ {0} /\ BackoffCap <= ResetDelay + 1
ASSUME Bug \in BOOLEAN /\ Bug2 \in BOOLEAN /\ Bug3 \in BOOLEAN
ASSUME NegHeader \in BOOLEAN

\* A reset value beyond the horizon: "server clock skew / bogus far-future
\* reset".  A sleeper waiting on it can never be woken by Tick.
Bogus == MaxClock + ResetDelay + 1

NoResp == [status |-> "none", remaining |-> 0, reset |-> 0]

\* updateFromHeaders stores the header remaining, clamping negatives to 0
\* (client.go:404-410, fixed 2026-07-06). Bug3 removes the clamp: a stored
\* -1 makes WaitNeeded false (remaining = 0 tested exactly) -- waiting off.
StoredRem(v) == IF ~Bug3 /\ v < 0 THEN 0 ELSE v

VARIABLES
    clock,          \* logical time, advanced by Tick
    limRemaining,   \* client rateLimiter.remaining   (client.go:342)
    limReset,       \* client rateLimiter.resetTime   (client.go:343); 0 = zero time
    limKnown,       \* TRUE once any response has populated the limiter
    srvRemaining,   \* server-side true remaining quota
    srvReset,       \* server-side true window reset time
    pc,             \* per-goroutine control state
    wakeAt,         \* per-goroutine wake deadline (sleeping/backoff)
    attempts,       \* per-goroutine retry attempts this request
    reqs,           \* per-goroutine completed/started logical requests
    resp,           \* per-goroutine buffered (undelivered) response
    overshoot,      \* PROPOSED-property witness: a request arrived at an
                    \* exhausted server after the limiter was informed
    sleepOvershoot  \* same, but the sender came straight from a sleep-wake

vars == <<clock, limRemaining, limReset, limKnown, srvRemaining, srvReset,
          pc, wakeAt, attempts, reqs, resp, overshoot, sleepOvershoot>>

States == {"idle", "sleeping", "waitresp", "backoff"}
\* remaining may be -1: the NegHeader fault (and, via Bug3, the limiter).
RespType == [status: {"none", "ok", "limited"}, remaining: -1..Quota, reset: 0..Bogus]

TypeInvariant ==
    /\ clock \in 0..MaxClock
    /\ limRemaining \in -1..Quota
    /\ limReset \in 0..Bogus
    /\ limKnown \in BOOLEAN
    /\ srvRemaining \in 0..Quota
    /\ srvReset \in 0..(MaxClock + ResetDelay)
    /\ pc \in [Gs -> States]
    /\ wakeAt \in [Gs -> 0..Bogus]
    /\ attempts \in [Gs -> 0..MaxAttempts]
    /\ reqs \in [Gs -> 0..MaxReqs]
    /\ resp \in [Gs -> RespType]
    /\ overshoot \in BOOLEAN
    /\ sleepOvershoot \in BOOLEAN

\* Goroutines holding a semaphore slot: everything past the acquire at
\* client.go:434 up to the deferred release.  Sleepers hold their slot.
Active == {g \in Gs : pc[g] # "idle"}

\* rateLimiter.waitDuration() > 0  (client.go:349-359): exhausted, reset
\* known (non-zero), reset still in the future.
WaitNeeded == limRemaining = 0 /\ limReset # 0 /\ clock < limReset

Init ==
    /\ clock = 0
    /\ limRemaining = 0        \* Go zero value; harmless because limReset=0
    /\ limReset = 0            \* zero time => waitDuration returns 0
    /\ limKnown = FALSE
    /\ srvRemaining = Quota
    /\ srvReset = 0
    /\ pc = [g \in Gs |-> "idle"]
    /\ wakeAt = [g \in Gs |-> 0]
    /\ attempts = [g \in Gs |-> 0]
    /\ reqs = [g \in Gs |-> 0]
    /\ resp = [g \in Gs |-> NoResp]
    /\ overshoot = FALSE
    /\ sleepOvershoot = FALSE

(***************************************************************************)
(* Arrive(g, fromSleep): the request hits the server.  The server either   *)
(* consumes a unit (ok) or answers "limited" (429 / 403+headers).  The     *)
(* response is COMPUTED now but only applied to the limiter later by a     *)
(* Deliver* action, so it can be stale by then.  The header reset value    *)
(* may nondeterministically be Bogus (clock skew fault).                   *)
(***************************************************************************)
Arrive(g, fromSleep) ==
    LET hit     == srvRemaining = 0
        newSrv  == IF hit THEN 0 ELSE srvRemaining - 1
        newRst  == IF ~hit /\ newSrv = 0 THEN clock + ResetDelay ELSE srvReset
        status  == IF hit THEN "limited" ELSE "ok"
    IN  /\ srvRemaining' = newSrv
        /\ srvReset' = newRst
        /\ \E hdrReset \in {newRst, Bogus},
              hdrRem \in (IF NegHeader THEN {newSrv, -1} ELSE {newSrv}) :
              resp' = [resp EXCEPT ![g] =
                  [status |-> status, remaining |-> hdrRem, reset |-> hdrReset]]
        /\ overshoot' = (overshoot \/ (hit /\ limKnown))
        /\ sleepOvershoot' = (sleepOvershoot \/ (hit /\ limKnown /\ fromSleep))

\* Semaphore acquire + waitIfNeeded finds no wait + send (client.go:434-440).
StartSend(g) ==
    /\ pc[g] = "idle"
    /\ reqs[g] < MaxReqs
    /\ Cardinality(Active) < MaxConcurrency
    /\ Bug \/ ~WaitNeeded
    /\ pc' = [pc EXCEPT ![g] = "waitresp"]
    /\ reqs' = [reqs EXCEPT ![g] = @ + 1]
    /\ Arrive(g, FALSE)
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, wakeAt, attempts>>

\* Semaphore acquire + waitIfNeeded computes d>0 and sleeps OUTSIDE the
\* lock until reset (client.go:361-379).  Slot stays held while sleeping.
StartSleep(g) ==
    /\ ~Bug
    /\ pc[g] = "idle"
    /\ reqs[g] < MaxReqs
    /\ Cardinality(Active) < MaxConcurrency
    /\ WaitNeeded
    /\ pc' = [pc EXCEPT ![g] = "sleeping"]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = limReset]
    /\ reqs' = [reqs EXCEPT ![g] = @ + 1]
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, srvRemaining,
                   srvReset, attempts, resp, overshoot, sleepOvershoot>>

\* Sleep timer fires; the waitIfNeeded loop RECHECKS the limiter
\* (client.go:361-379, fixed 2026-07-06) and only fires when no further
\* wait is needed. Bug2 restores the pre-fix fire-without-recheck.
Wake(g) ==
    /\ pc[g] = "sleeping"
    /\ clock >= wakeAt[g]
    /\ Bug2 \/ ~WaitNeeded
    /\ pc' = [pc EXCEPT ![g] = "waitresp"]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = 0]
    /\ Arrive(g, TRUE)
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, attempts, reqs>>

\* Sleep timer fires but the recheck finds the limiter exhausted again
\* (another response reported a new window mid-sleep): loop back to sleep
\* until the limiter's new reset (client.go:361-379).
WakeResleep(g) ==
    /\ ~Bug2
    /\ pc[g] = "sleeping"
    /\ clock >= wakeAt[g]
    /\ WaitNeeded
    /\ wakeAt' = [wakeAt EXCEPT ![g] = limReset]
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, srvRemaining,
                   srvReset, pc, attempts, reqs, resp, overshoot,
                   sleepOvershoot>>

\* updateFromHeaders (client.go:401-424): unconditional overwrite of the
\* limiter from the (possibly stale) response.  ok => RoundTrip returns,
\* slot released.
DeliverOk(g) ==
    /\ pc[g] = "waitresp"
    /\ resp[g].status = "ok"
    /\ limRemaining' = StoredRem(resp[g].remaining)
    /\ limReset' = resp[g].reset
    /\ limKnown' = TRUE
    /\ resp' = [resp EXCEPT ![g] = NoResp]
    /\ pc' = [pc EXCEPT ![g] = "idle"]
    /\ attempts' = [attempts EXCEPT ![g] = 0]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = 0]
    /\ UNCHANGED <<clock, srvRemaining, srvReset, reqs, overshoot, sleepOvershoot>>

\* limited + retry budget left: compute a capped backoff delay
\* (retryDelay, client.go:505-525; clamp at client.go:511-513/519-521)
\* and sleep.  Delay is abstract: any 1..BackoffCap; a Bogus header reset
\* would yield a huge header-derived delay, which the clamp reduces to
\* BackoffCap -- both covered by the nondeterministic choice.
DeliverRetry(g) ==
    /\ pc[g] = "waitresp"
    /\ resp[g].status = "limited"
    /\ attempts[g] < MaxAttempts
    /\ limRemaining' = StoredRem(resp[g].remaining)
    /\ limReset' = resp[g].reset
    /\ limKnown' = TRUE
    /\ resp' = [resp EXCEPT ![g] = NoResp]
    /\ pc' = [pc EXCEPT ![g] = "backoff"]
    /\ attempts' = [attempts EXCEPT ![g] = @ + 1]
    /\ \E d \in 1..BackoffCap :
          wakeAt' = [wakeAt EXCEPT ![g] = clock + d]
    /\ UNCHANGED <<clock, srvRemaining, srvReset, reqs, overshoot, sleepOvershoot>>

\* limited + retries exhausted: RoundTrip returns the limited response
\* (client.go:446 loop exits, client.go:466).  Slot released.
DeliverGiveUp(g) ==
    /\ pc[g] = "waitresp"
    /\ resp[g].status = "limited"
    /\ attempts[g] >= MaxAttempts
    /\ limRemaining' = StoredRem(resp[g].remaining)
    /\ limReset' = resp[g].reset
    /\ limKnown' = TRUE
    /\ resp' = [resp EXCEPT ![g] = NoResp]
    /\ pc' = [pc EXCEPT ![g] = "idle"]
    /\ attempts' = [attempts EXCEPT ![g] = 0]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = 0]
    /\ UNCHANGED <<clock, srvRemaining, srvReset, reqs, overshoot, sleepOvershoot>>

\* Backoff timer fires; waitIfNeeded is called AGAIN before the retry
\* (client.go:456).  No wait needed (or Bug) => send.
RetrySend(g) ==
    /\ pc[g] = "backoff"
    /\ clock >= wakeAt[g]
    /\ Bug \/ ~WaitNeeded
    /\ pc' = [pc EXCEPT ![g] = "waitresp"]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = 0]
    /\ Arrive(g, FALSE)
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, attempts, reqs>>

\* Backoff timer fires but the limiter now says exhausted-in-future:
\* the pre-retry waitIfNeeded (client.go:456) puts the goroutine back to
\* sleep until the limiter's reset.
RetrySleep(g) ==
    /\ ~Bug
    /\ pc[g] = "backoff"
    /\ clock >= wakeAt[g]
    /\ WaitNeeded
    /\ pc' = [pc EXCEPT ![g] = "sleeping"]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = limReset]
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, srvRemaining,
                   srvReset, attempts, reqs, resp, overshoot, sleepOvershoot>>

\* Context cancelled mid-sleep or mid-backoff (sleepContext, client.go:
\* 382-391): RoundTrip returns ctx.Err(), deferred release frees the slot.
Cancel(g) ==
    /\ pc[g] \in {"sleeping", "backoff"}
    /\ pc' = [pc EXCEPT ![g] = "idle"]
    /\ wakeAt' = [wakeAt EXCEPT ![g] = 0]
    /\ attempts' = [attempts EXCEPT ![g] = 0]
    /\ UNCHANGED <<clock, limRemaining, limReset, limKnown, srvRemaining,
                   srvReset, reqs, resp, overshoot, sleepOvershoot>>

\* Logical time advances; the server refills its window atomically when
\* the clock crosses srvReset (woken sleepers with an accurate header
\* reset therefore observe a fresh window -- the +1s slack of client.go:355).
Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ srvRemaining' = IF srvRemaining = 0 /\ clock + 1 >= srvReset
                       THEN Quota ELSE srvRemaining
    /\ UNCHANGED <<limRemaining, limReset, limKnown, srvReset, pc, wakeAt,
                   attempts, reqs, resp, overshoot, sleepOvershoot>>

Next ==
    \/ Tick
    \/ \E g \in Gs :
          \/ StartSend(g)
          \/ StartSleep(g)
          \/ Wake(g)
          \/ WakeResleep(g)
          \/ DeliverOk(g)
          \/ DeliverRetry(g)
          \/ DeliverGiveUp(g)
          \/ RetrySend(g)
          \/ RetrySleep(g)
          \/ Cancel(g)

Spec == Init /\ [][Next]_vars

Perms == Permutations(Gs)

(***************************************************************************)
(* INVARIANTS                                                              *)
(***************************************************************************)

\* CODE-DOCUMENTED (fixed 2026-07-06, FINDING 14): updateFromHeaders clamps
\* a negative header remaining to 0 (client.go:404-410), so the stored
\* remaining is never negative even under the NegHeader fault. Pre-fix the
\* value was stored verbatim, which DISABLED waiting (waitDuration tests
\* remaining == 0 exactly) -- Bug3 + NegHeader reproduce that (MCMutation3).
RemainingNonNegative == limRemaining >= 0

\* CODE-DOCUMENTED: the semaphore bounds concurrent RoundTrips
\* (client.go:434-435, defaultMaxConcurrency client.go:29).
SemaphoreBounded == Cardinality(Active) <= MaxConcurrency

\* CODE-DOCUMENTED: retry backoff is capped at maxRetryDelay
\* (client.go:511-513 header-derived clamp, client.go:519-521 exponential cap).
BackoffCapped ==
    \A g \in Gs : pc[g] = "backoff" => wakeAt[g] <= clock + BackoffCap

\* PROPOSED: once the client has seen rate-limit headers, no request
\* arrives at the server while the server-side window is exhausted.
\* Expected to be VIOLATED by the wake-together / no-recheck design.
NoOvershoot == ~overshoot

\* PROPOSED (narrower): no request coming straight from a sleep-wake
\* arrives at an exhausted server.  STILL VIOLATED after the recheck fix:
\* the residual trace needs another goroutine to consume the fresh window
\* while its headers are UNDELIVERED, so the woken sleeper's recheck reads
\* a stale limiter -- that is the NoOvershoot stampede in sleeper clothing,
\* not the no-recheck defect.  Unfixable client-side; kept as MCFinding2.
NoSleeperOvershoot == ~sleepOvershoot

\* CODE-DOCUMENTED (fixed 2026-07-06, FINDING 13): waitIfNeeded loops
\* sleep -> recheck, so a sleeper never transitions to sending while the
\* limiter (as of the wake instant) still demands waiting.  Bug2 (recheck
\* dropped) violates it -- MCMutation2.
WakeRechecksLimiter ==
    [][\A g \in Gs :
         (pc[g] = "sleeping" /\ pc'[g] = "waitresp") => ~WaitNeeded]_vars

\* BAIT (MCBait.cfg only): claims no goroutine ever sleeps.  Clearly
\* reachable; MUST fail, proving TLC explores the exhaustion path.
BaitNoSleeper == \A g \in Gs : pc[g] # "sleeping"

===================================================================================
