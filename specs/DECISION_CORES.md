# Decision cores (specgen bridge)

The full TLC specs under `specs/*/` use records, multi-object interleavings,
and other shapes that stay outside the **pure decision-module** core of
`specgen`. They remain the **design model-checkers**.

**Decision cores** are scalar state machines that capture the load-bearing
guards/transitions production code should obey. They:

1. Parse under SANY and pass TLC (bait MUST fail, main MUST pass)
2. Generate with `specgen` into `pkg/.../<name>spec/`
3. Provide `Trace()` NDJSON for `conform`
4. Dual-checked by Go tests; production may call the generated module
## Layout

```
specs/<subsystem>/decision/
  Decision.tla      # MODULE name = <Subsystem>Decision e.g. TuiReloadDecision
  MC.cfg            # faithful: PASS
  MCBait.cfg        # MUST FAIL (proves exploration)
  MCMutation.cfg    # optional: known-bug MUST FAIL
  README.md         # purpose, action→code map

pkg/<area>/<name>spec/   # generated — never hand-edit
  spec.go
  spec_test.go
  BUILD.bazel
```

## Specgen-supported subset (strict)

- VARIABLES / CONSTANTS / EXTENDS Naturals|Integers|FiniteSets
- bool / int / string only (no records, no functions of model values)
- `= /= /\ \/ ~ \in` arithmetic comparisons IF/THEN/ELSE UNCHANGED
- ranges `a..b`, sets of scalars
- Prefer **no records** as VARIABLES (keep bool/int/string)
- Actions must appear as named operators in `Next == A \/ B \/ C`

## Workflow per core

```bash
tlc --parse specs/X/decision/Decision.tla
tlc -c specs/X/decision/MC.cfg specs/X/decision/Decision.tla
tlc -c specs/X/decision/MCBait.cfg specs/X/decision/Decision.tla   # expect fail
# regenerate all (or one):
scripts/regenerate-decision-cores.sh            # all
scripts/regenerate-decision-cores.sh timing-clamp
# optional conform:
#   emit NDJSON via State.Trace("Action") then:
#   conform -spec specs/X/decision/Decision.tla -config MC.cfg run.ndjson
```

## Cores and production wiring

| Core | Models | Gen package | Production |
|------|--------|-------------|------------|
| tui-reload | reload gen + stale fetch discard | `tuireloadspec` | **wired** — `logFetchResultFresh` |
| rate-limit | wait/recheck after sleep | `ratelimitspec` | **wired** — `rateLimitWaitNeeded` |
| timing-clamp | clampSpanToParent | `timingclampspec` | **wired** — `DoClamp` |
| sync-bounds | stale attempt | `syncboundsspec` | **wired** — `acceptJobsAttempt` (SQL twin) |
| gha-lifecycle | pending/fail/queue gates | `ghalifecyclespec` | **wired** — `countsFailed` / `countsQueue` |
| log-groups | stack depth | `loggroupsspec` | gen tests only |
| span-tree | runner-wins keep | `spantreespec` | **wired** — `dropAPIForRunnerTwin` |

Full TLC specs remain authoritative for multi-object interleavings.
Decision cores pin the **pure decisions** that must not drift.

## Production call pattern

```go
// Thin mechanics wrapper — no duplicated formula.
func clampSpanToParent(start, end, pS, pE int64) (int64, int64) {
    s := timingclampspec.State{
        Phase: "init", Start: start, End: end,
        ParentStart: pS, ParentEnd: pE,
    }
    s = s.DoClamp() // generated decision transition
    return s.OutStart, s.OutEnd
}
```
