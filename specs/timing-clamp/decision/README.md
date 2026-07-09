# TimingClampDecision — clampSpanToParent decision core

Scalar state machine for **one** application of `clampSpanToParent`
(`pkg/analyzer/analyzer.go:826`). Specgen-compatible subset only.

- File: `Decision.tla` (SANY module name = filename)
- Logical name: TimingClampDecision
- Full pipeline design model: `../TimingClamp.tla`

## Purpose

Pin the pure clamp formula so production code cannot drift:

1. TLC checks faithful containment + order (MC.cfg green)
2. Bait fails (MCBait) — proves exploration
3. Bug passthrough fails containment (MCMutation) — invariant teeth
4. `specgen` → `pkg/analyzer/timingclampspec` (never hand-edit)
5. Dual test: table of (s,e,pS,pE) compares `clampSpanToParent` vs DoClamp

## Action → code map

| Action | Code |
|--------|------|
| Init | interesting case: child end past parent |
| SetHostile | rewrite open inputs (start before parent, end = Tmax) |
| DoClamp | `clampSpanToParent` / ClampSpan in TimingClamp.tla |
| BugPassthrough | pre-fix raw passthrough (Bug=TRUE, no clamp) |
| Finish | terminal phase for bait witness |

## Clamp formula (DoClamp)

```
pe  = IF parentEnd <= parentStart THEN parentStart + 1 ELSE parentEnd
cs  = IF start < parentStart THEN parentStart
      ELSE IF start > pe - 1 THEN pe - 1
      ELSE start
ce0 = IF end > pe THEN pe ELSE end
ce  = IF ce0 < cs + 1 THEN cs + 1 ELSE ce0
out = (cs, ce)
```

## Invariants

| Invariant | Faithful | Bait | Mutation (Bug) |
|-----------|----------|------|----------------|
| TypeOK | green | — | — |
| ClampedOrdered | green | — | — |
| ClampedContained | green | — | **RED** |
| BaitNeverDone | — | **RED** | — |

## Reproduce

```sh
cd specs/timing-clamp/decision
tlc --parse Decision.tla
tlc -c MC.cfg --no-deadlock Decision.tla          # green
tlc -c MCBait.cfg --no-deadlock Decision.tla      # MUST FAIL
tlc -c MCMutation.cfg --no-deadlock Decision.tla  # MUST FAIL

# from repo root:
specgen -const Tmax=4 -const Bug=FALSE \
  -o pkg/analyzer/timingclampspec -p timingclampspec \
  specs/timing-clamp/decision/Decision.tla
```

## Generated module

`pkg/analyzer/timingclampspec` — regenerate, never hand-edit.
Dual: `pkg/analyzer/clamp_decision_dual_test.go`.
