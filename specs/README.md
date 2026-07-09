# TLA+ Specs

Model-checked specs for otel-explorer's state machines.
Findings + fix list: [FINDINGS.md](FINDINGS.md).

| Spec | Models | Key result |
|------|--------|-----------|
| `sync-bounds` | store watermark, seeded runs, attempt invalidation | 2 bugs; seeded-run discipline verified safe |
| `tui-reload` | TUI reload / log-fetch / progress channels | 1 bug; isLoading guard proven load-bearing |
| `rate-limit` | shared limiter + retry/backoff | 2 low-severity design findings |
| `gha-lifecycle` | GHA run/job lifecycle → metrics + retry links | 4 bugs |
| `timing-clamp` | run/job/step time clamping pipeline | 5 findings (containment escapes) |
| `span-tree` | dedup → filter → parent-link pipeline | 3 bugs (dedup non-idempotent, cycles vanish) |
| `log-groups` | log group nesting + gap parsing | 4 findings (non-monotonic timestamps) |

## Layout (per spec dir)

- `<Spec>.tla` — full design model (records/quantifiers OK); has `Bug` knobs
- `MC*.cfg` — configs; expected result is encoded in the name:
  - `MCBait*` MUST FAIL — proves TLC explores (witness in README)
  - `MCMutation*` MUST FAIL — proves invariants have teeth
  - `MCFinding*` MUST FAIL — documented real finding, kept on purpose
  - everything else MUST PASS
- `README.md` — purpose, action→code map, invariant table
  (code-documented vs proposed), bounds, results, traces
- `decision/` — **specgen bridge** (scalar SM; see [DECISION_CORES.md](DECISION_CORES.md))
  - `Decision.tla` + `MC.cfg` / `MCBait.cfg` / `MCMutation.cfg`
  - generated Go under `pkg/.../<name>spec/` (never hand-edit)

## Decision cores (specgen + conform)

Full TLC specs are **not** directly codegen'd (unsupported TLA+ subset).
Each subsystem has a scalar **decision core** that:

1. Passes TLC (bait/mutation teeth)
2. Generates via `specgen` → `pkg/.../*spec`
3. Dual/conform tests pin load-bearing guards

| Core | Generated package |
|------|-------------------|
| tui-reload | `pkg/tui/results/tuireloadspec` |
| rate-limit | `pkg/githubapi/ratelimitspec` |
| timing-clamp | `pkg/analyzer/timingclampspec` |
| sync-bounds | `pkg/store/syncboundsspec` |
| gha-lifecycle | `pkg/analyzer/ghalifecyclespec` |
| log-groups | `pkg/logparse/loggroupsspec` |
| span-tree | `pkg/analyzer/spantreespec` |

Workflow: `tlc` → `specgen` → dual tests → optional `conform` on Trace NDJSON.

## Running

```
scripts/check-specs.sh                 # full + decision cores + package check
scripts/check-specs.sh span-tree       # one subsystem (full + decision if present)
```

Prefers `tlc` on PATH (dotai CLI); falls back to java + jar.

## Rules

- Never weaken a spec or property to silence a violation — report it.
- Properties labeled `proposed` are suggestions; the user owns correctness.
- Specs drift; when the modeled code changes, update the spec or delete it.
  A stale spec is just documentation.
- Green TLC at recorded bounds = strong bug hunt, not proof.
- Never hand-edit generated `*spec` packages — change Decision.tla, re-run specgen.
