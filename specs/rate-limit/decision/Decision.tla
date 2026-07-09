----------------------------- MODULE Decision -----------------------------
(***************************************************************************
Logical name: RateLimitDecision
File is Decision.tla so SANY module name matches the path.

PURPOSE — decision core (specgen subset) for rate-limit wake recheck
(FINDING 13).

Full design model: specs/rate-limit/RateLimit.tla (records, quantifiers,
multi-goroutine stampede). This core pins the pure one-client decision:

  After sleeping on an exhausted window, waitIfNeeded must RECHECK the
  limiter on wake. If still exhausted, stay sleeping. Pre-fix (Bug=TRUE)
  fired blind after sleepContext returned → sentWhileExhausted.

Maps to pkg/githubapi/client.go rateLimiter:
  LearnExhausted      → updateFromHeaders remaining=0 + reset in future
  StartSleep          → waitIfNeeded: waitDuration()>0, sleep outside lock
  WakeRecheckSend     → loop: recheck, waitDuration()==0 → return (send OK)
  WakeRecheckResleep  → loop: recheck, waitDuration()>0 → sleep again
  WakeBugSend         → pre-fix: sleep once, fire without recheck (Bug)
  Tick                → wall clock; may observe window past resetAt
  SendOk              → remaining>0 and awake → consume one unit

Scalar-only: no records, no quantifiers in actions, no sequences.
***************************************************************************)
EXTENDS Naturals

CONSTANTS
  MaxRem,    \* max remaining units (e.g. 2)
  MaxClock,  \* logical-time horizon (e.g. 3)
  Bug        \* TRUE: enable pre-fix no-recheck wake (mutation)

\* No ASSUME: specgen skips Init when ASSUME uses \in (unsupported op).
\* Bounds are pinned by MC*.cfg / -const bindings.

VARIABLES
  remaining,           \* rateLimiter.remaining (0 = exhausted)
  sleeping,            \* waitIfNeeded is inside sleepContext
  clock,               \* logical time
  resetAt,             \* rateLimiter.resetTime as logical tick (0 = unset)
  sentWhileExhausted   \* monitor: TRUE if a bug-path wake fired while WaitNeeded

vars == <<remaining, sleeping, clock, resetAt, sentWhileExhausted>>

\* Client-side waitDuration() > 0 (client.go:349-359): remaining exhausted,
\* reset known (non-zero), reset still in the future.
WaitNeeded == remaining = 0 /\ resetAt > 0 /\ clock < resetAt

TypeOK ==
  /\ remaining \in 0..MaxRem
  /\ sleeping \in BOOLEAN
  /\ clock \in 0..MaxClock
  /\ resetAt \in 0..MaxClock
  /\ sentWhileExhausted \in BOOLEAN

\* remaining starts at 2 (= MaxRem in MC.cfg / -const). Literal so
\* specgen can emit Init (CONSTANT refs in Init are skipped).
Init ==
  /\ remaining = 2
  /\ sleeping = FALSE
  /\ clock = 0
  /\ resetAt = 0
  /\ sentWhileExhausted = FALSE

(* ACTIONS *)

(* A delivered response reports the primary limit exhausted with a future
   reset (updateFromHeaders, client.go:401-418). Models learning remaining=0
   from headers while still holding some local remaining count. *)
LearnExhausted ==
  /\ remaining > 0
  /\ clock < MaxClock
  /\ remaining' = 0
  /\ resetAt' = clock + 1
  /\ UNCHANGED <<sleeping, clock, sentWhileExhausted>>

(* waitIfNeeded sees WaitNeeded, sleeps outside the lock
   (client.go:361-379). *)
StartSleep ==
  /\ remaining = 0
  /\ ~sleeping
  /\ clock < resetAt
  /\ resetAt > 0
  /\ sleeping' = TRUE
  /\ UNCHANGED <<remaining, clock, resetAt, sentWhileExhausted>>

(* Faithful recheck: sleep timer / loop iteration finds WaitNeeded still
   true → stay sleeping (client.go:361-379 for-loop continues).
   Disabled under Bug so the mutation path is forced through WakeBugSend. *)
WakeRecheckResleep ==
  /\ ~Bug
  /\ sleeping
  /\ WaitNeeded
  /\ UNCHANGED vars

(* Faithful recheck: no further wait needed (reset reached) → leave sleep
   and treat send as OK. Refills remaining to model a fresh window so the
   machine is not stuck with remaining=0 forever after reset. *)
WakeRecheckSend ==
  /\ sleeping
  /\ ~WaitNeeded
  /\ sleeping' = FALSE
  /\ remaining' = MaxRem
  /\ UNCHANGED <<clock, resetAt, sentWhileExhausted>>

(* Mutation / pre-fix FINDING 13: after any sleep, fire without rechecking
   waitDuration(). If WaitNeeded still holds, set the fault flag so
   NoSendWhileExhausted fails (MCMutation). *)
WakeBugSend ==
  /\ Bug
  /\ sleeping
  /\ sleeping' = FALSE
  /\ sentWhileExhausted' = WaitNeeded
  /\ remaining' = IF WaitNeeded THEN remaining ELSE MaxRem
  /\ UNCHANGED <<clock, resetAt>>

(* Logical clock advances. When the tick crosses resetAt with remaining
   still 0, refill (server window open); the client would observe
   waitDuration()==0 on the next recheck either way. *)
Tick ==
  /\ clock < MaxClock
  /\ clock' = clock + 1
  /\ remaining' =
       IF remaining = 0 /\ resetAt > 0 /\ (clock + 1) >= resetAt
       THEN MaxRem
       ELSE remaining
  /\ UNCHANGED <<sleeping, resetAt, sentWhileExhausted>>

(* Awake with quota → consume one unit (StartSend with no wait). *)
SendOk ==
  /\ remaining > 0
  /\ ~sleeping
  /\ remaining' = remaining - 1
  /\ UNCHANGED <<sleeping, clock, resetAt, sentWhileExhausted>>

Next ==
  \/ LearnExhausted
  \/ StartSleep
  \/ WakeRecheckSend
  \/ WakeRecheckResleep
  \/ WakeBugSend
  \/ Tick
  \/ SendOk

Spec == Init /\ [][Next]_vars

(* PROPERTIES *)

(* Load-bearing (FINDING 13): never send while the limiter still demands
   waiting. Only WakeBugSend can set the flag. *)
NoSendWhileExhausted == ~sentWhileExhausted

(* Bait: asserts the client never sleeps. MUST FAIL after StartSleep —
   proves TLC explores the exhaustion path. *)
BaitNeverSleep == ~sleeping

=============================================================================
