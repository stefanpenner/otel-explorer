# Decision cores (specgen bridge)

**Agent entrypoint for code changes:** [GATES.md](GATES.md)  
(per-core: `specs/<name>/GATES.md` — not the full TLC model).

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
| log-groups | stack depth | `loggroupsspec` | **wired** — `canOpenGroup` / `canCloseGroup` |
| span-tree | runner-wins keep | `spantreespec` | **wired** — `dropAPIForRunnerTwin` |

Wire health: `scripts/verify-decision-wires.sh` (also from `check-specs.sh`).

Checks:
- Decision.tla present for each core
- Generated `pkg/.../*spec/spec.go` with `PurePredicates()` registry
- Production pure-gate symbols in non-test sources
- Dual test files present (semantic duals + registry smoke)
- Optional: `SPECGEN_CHECK_REGEN=1` → regenerate + no-diff

Generated pure API (per `*spec` package):

```go
func (s State) WaitNeeded() bool          // example pure gate
func PurePredicates() []PurePredicate     // enumerate all pure gates
// TestPurePredicates — generated smoke on Init
```

```bash
# Hermetic Bazel pipeline (preferred — used by CI via bazel test //...)
bazel test  //tools/decision:up_to_date     # fail if *spec stale vs Decision.tla
bazel run   //tools/decision:update         # rewrite committed *spec from .tla
bazel build //tools/specgen                 # hermetic codegen binary

# Inventory / TLC / duals
scripts/decision-stack-status.sh
scripts/verify-decision-wires.sh
scripts/check-specs.sh                      # TLC full + decision + duals + wires
scripts/regenerate-decision-cores.sh        # → bazel run //tools/decision:update
```

### Complete Bazel graph (production path)

```
specs/<core>/decision/Decision.tla
        │
        ▼
//tools/specgen          (hermetic binary)
        │
        ▼
//tools/decision:<core>_gen/spec.go   (genrule outs)
        │
        ▼
//pkg/.../<name>spec     (go_library srcs = genrule outs)
        │
        ▼
//pkg/analyzer (etc.)
        │
        ▼
//:ote   ←  bazel run //:ote
```

Committed `spec.go` copies are for IDE + `up_to_date` checks only.
**Library and binary use genrule outputs** — changing `.tla` rebuilds `ote`.

`bazel test //...` includes `//tools/decision:up_to_date`.
The `tla-specs` job still runs TLC + duals; the unit `test` job runs Bazel.

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
