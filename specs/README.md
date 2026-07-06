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

- `<Spec>.tla` — the spec; has a `Bug` constant for the mutation check
- `MC*.cfg` — configs; expected result is encoded in the name:
  - `MCBait*` MUST FAIL — proves TLC explores (witness in README)
  - `MCMutation*` MUST FAIL — proves invariants have teeth
  - `MCFinding*` MUST FAIL — documented real finding, kept on purpose
  - everything else MUST PASS
- `README.md` — purpose, action→code map, invariant table
  (code-documented vs proposed), bounds, results, traces

## Running

```
scripts/check-specs.sh                 # everything (skips without a JVM)
scripts/check-specs.sh span-tree       # one spec
```

## Rules

- Never weaken a spec or property to silence a violation — report it.
- Properties labeled `proposed` are suggestions; the user owns correctness.
- Specs drift; when the modeled code changes, update the spec or delete it.
  A stale spec is just documentation.
- Green TLC at recorded bounds = strong bug hunt, not proof.
