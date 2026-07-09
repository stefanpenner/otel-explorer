# TuiReloadDecision — specgen decision core

Scalar state machine for reload generation + stale log-fetch discard.

Full TLC model (records/quantifiers): `../TuiReload.tla`.
This core is the **specgen bridge** — pure decisions only.

## Purpose

Pin the load-bearing rule:

> After a successful reload bumps `reloadGen`, any in-flight log-fetch
> result stamped with an older gen is **discarded** (never applied).

Maps to:

| Action | Code |
|--------|------|
| `PressReload` | `model.go` key `r` + `doReload`; isLoading guard |
| `ReloadDone` | `ReloadResultMsg` success: `reloadGen++` (`model.go:410`) |
| `PressFetch1/2` | `fetchLogsForCurrentItem` stamps `gen` (`logfetch.go:61`) |
| `FetchAccept` | `LogFetchResultMsg` when `msg.gen == m.reloadGen` |
| `FetchDiscard` | discard path `model.go:365-372` |
| `FetchStaleBug` | mutation: pre-fix "accept any result" (Bug=TRUE) |

## State (scalars)

| Var | Meaning |
|-----|---------|
| `isLoading` | reload in flight |
| `reloadGen` | current generation (0..MaxGen) |
| `fetchJob` | in-flight fetch job (0 = none) |
| `fetchGen` | gen captured when fetch started |
| `staleAccepted` | monitor: TRUE if stale wrongly accepted |

## Correct sequence (witness of supersession)

1. `PressFetch1` — fetch job 1 at gen 0
2. `PressReload` — isLoading
3. `ReloadDone` — reloadGen = 1; fetch still in flight with fetchGen=0
4. `FetchDiscard` — fetchGen ≠ reloadGen → clear fetchJob, no apply

Under `Bug=TRUE`, step 4 can be `FetchStaleBug` → `staleAccepted` →
`NoStaleAccepted` fails (`MCMutation.cfg`).

## TLC

```sh
cd specs/tui-reload/decision
tlc --parse Decision.tla
tlc -c MC.cfg Decision.tla          # PASS
tlc -c MCBait.cfg Decision.tla      # FAIL (bait)
tlc -c MCMutation.cfg Decision.tla  # FAIL (NoStaleAccepted)
```

## specgen

```sh
specgen \
  -o pkg/tui/results/tuireloadspec \
  -p tuireloadspec \
  -const MaxGen=2 \
  -const MaxJobs=2 \
  -const Bug=FALSE \
  specs/tui-reload/decision/Decision.tla
```

Never hand-edit generated `spec.go` / `spec_test.go`.

## conform

```sh
# from a Go test that writes NDJSON via State.Trace(...):
conform \
  -spec specs/tui-reload/decision/Decision.tla \
  -config specs/tui-reload/decision/MC.cfg \
  /tmp/tuireload-trace.ndjson
```

See `pkg/tui/results/tuireloadspec/conform_test.go`.

## Dual-test note (model.go)

Real discard rule (`model.go:365-372`):

```go
if msg.jobID == 0 || msg.jobID != m.logFetchingJobID || msg.gen != m.reloadGen {
    return m, nil  // discard
}
```

Decision-core `FetchDiscard` abstracts the gen half: `fetchGen # reloadGen`.
(Reload also zeros `logFetchingJobID`, which alone discards; the gen tag
covers the case where a *new* fetch of the same job is already in flight.)
