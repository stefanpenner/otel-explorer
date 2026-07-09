# Decision cores (specgen bridge)

The full TLC specs under `specs/*/` use records, quantifiers, CHOOSE, and
sequences — outside `specgen`'s supported subset. They stay the **design
model-checkers**.

**Decision cores** are scalar state machines that capture the load-bearing
guards/transitions production code should obey. They:

1. Parse under SANY and pass TLC (bait MUST fail, main MUST pass)
2. Generate with `specgen` into `pkg/.../<name>spec/`
3. Provide `Trace()` NDJSON for `conform`
4. Are dual-checked by Go tests (BFS from gen + optional dual vs handwritten)

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
- **Not supported:** records `[a |-> ...]`, quantifiers `\A \E`, CHOOSE,
  sequences, EXCEPT on functions, temporal `[]` in operators used as actions

Actions must appear as named operators in `Next == A \/ B \/ C`.

## Workflow per core

```bash
tlc --parse specs/X/decision/Decision.tla
tlc -c specs/X/decision/MC.cfg specs/X/decision/Decision.tla
tlc -c specs/X/decision/MCBait.cfg specs/X/decision/Decision.tla   # expect fail
specgen -o pkg/.../xspec -p xspec specs/X/decision/Decision.tla
# bind CONSTANTS if needed: specgen -const Bug=FALSE ...
cd pkg/.../xspec && go test .
# optional conform:
#   emit NDJSON via State.Trace("Action") then:
#   conform -spec specs/X/decision/Decision.tla -config MC.cfg run.ndjson
```

## Priority cores

| Core | Models | Wire target |
|------|--------|-------------|
| tui-reload | reload gen + stale log-fetch discard | `pkg/tui/results` |
| rate-limit | wait/recheck after sleep | `pkg/githubapi` rateLimiter |
| timing-clamp | clampSpanToParent contract | `pkg/analyzer` clamp helper |
| sync-bounds | watermark / stale attempt | `pkg/store` |
| gha-lifecycle | isPending / fail / queue gates | `pkg/analyzer` job classify |
| log-groups | open/close stack depth | `pkg/logparse` |
| span-tree | runner-wins keep decision | `pkg/analyzer` dedupe |

Full TLC specs remain authoritative for multi-object interleavings.
Decision cores pin the **pure decisions** that must not drift.
