# RateLimitDecision — wake-recheck decision core (FINDING 13)

Scalar state machine for **one** client vs the shared `rateLimiter`.

Full multi-goroutine design model: `../RateLimit.tla`.
This core is the **specgen bridge** — pure recheck decision only.

## Purpose

Pin the load-bearing rule fixed 2026-07-06:

> After sleeping on an exhausted window, `waitIfNeeded` must **recheck**
> the limiter. If still exhausted, sleep again — never fire blind.

Maps to:

| Action | Code |
|--------|------|
| `LearnExhausted` | `updateFromHeaders` → remaining=0, future reset |
| `StartSleep` | `rateLimitWaitNeeded` / `waitIfNeeded` + `sleepContext` |
| `WakeRecheckSend` | loop recheck: `waitDuration()==0` → return |
| `WakeRecheckResleep` | loop recheck: still waiting → sleep again |
| `WakeBugSend` | pre-fix fire-without-recheck (`Bug=TRUE`) |
| `Tick` | wall clock; may cross `resetAt` |
| `SendOk` | awake with `remaining>0` → consume one unit |

## State (scalars)

| Var | Meaning |
|-----|---------|
| `remaining` | `rateLimiter.remaining` (0 = exhausted) |
| `sleeping` | inside `sleepContext` |
| `clock` | logical time (0..MaxClock) |
| `resetAt` | logical reset tick (0 = unset) |
| `sentWhileExhausted` | monitor: bug-path wake while `WaitNeeded` |

`WaitNeeded == remaining=0 /\ resetAt>0 /\ clock<resetAt`
mirrors `waitDuration > 0` / production `rateLimitWaitNeeded`.

## Correct sequence (recheck)

1. `LearnExhausted` — remaining=0, resetAt=clock+1
2. `StartSleep` — sleep outside the lock
3. While `WaitNeeded`: only `WakeRecheckResleep` / `Tick` (no send)
4. After clock crosses resetAt: `WakeRecheckSend` — awake, send OK

Under `Bug=TRUE`, step 3 can be `WakeBugSend` → `sentWhileExhausted`
→ `NoSendWhileExhausted` fails (`MCMutation.cfg`).

## TLC

```sh
cd specs/rate-limit/decision
tlc --parse Decision.tla
tlc -c MC.cfg Decision.tla          # PASS
tlc -c MCBait.cfg Decision.tla      # FAIL (bait)
tlc -c MCMutation.cfg Decision.tla  # FAIL (NoSendWhileExhausted)
```

## specgen

```sh
specgen \
  -o pkg/githubapi/ratelimitspec \
  -p ratelimitspec \
  -const MaxRem=2 \
  -const MaxClock=3 \
  -const Bug=FALSE \
  specs/rate-limit/decision/Decision.tla
```

Never hand-edit generated `spec.go` / `spec_test.go`.

## conform / dual

```sh
# from a Go test that writes NDJSON via State.Trace(...):
conform \
  -spec specs/rate-limit/decision/Decision.tla \
  -config specs/rate-limit/decision/MC.cfg \
  /tmp/ratelimit-trace.ndjson
```

See `pkg/githubapi/ratelimitspec/conform_test.go` for Trace+conform and
the dual link to `waitIfNeeded` recheck (`client_test.go`
`TestRateLimitSpec_WakeRechecksLimiter`).
