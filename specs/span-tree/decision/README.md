# SpanTreeDecision — runner-wins keep decision core

Scalar state machine for **one** API/runner collision group.

Full pipeline model (records, filter, cycles):
`specs/span-tree/SpanTree.tla`.

This core is **specgen-clean**: bool + string only.

## State

| Var | Domain | Meaning |
|-----|--------|---------|
| `haveAPI` | BOOLEAN | saw API reconstruction span |
| `haveRunner` | BOOLEAN | saw runner-native span |
| `kept` | `"none"`/`"api"`/`"runner"` | keep decision after dedup |
| `done` | BOOLEAN | keep decision taken |

## Actions → code

| Action | Guard | Effect | Code |
|--------|-------|--------|------|
| `SeeAPI` | `~done /\ ~haveAPI` | `haveAPI=TRUE` | observe API span in group |
| `SeeRunner` | `~done /\ ~haveRunner` | `haveRunner=TRUE` | observe runner span in group |
| `DedupChoose` | `~done /\ (haveAPI \/ haveRunner)` | both → `kept="runner"` | `dropAPIForRunnerTwin` |
| `DedupBug` | `Bug /\ both` | `kept="api"` | mutation: wrong keep |
| `Terminating` | `done` | stutter | post-dedup |

## Invariants

| Name | Expected |
|------|----------|
| `TypeOK` | always |
| `Inv_RunnerWins` | green when `Bug=FALSE`; fails under `MCMutation` |
| `BaitNeverRunner` | fails under `MCBait` |

## Configs

| Config | Bug | Expected |
|--------|-----|----------|
| `MC` | FALSE | pass |
| `MCBait` | FALSE | **fail** `BaitNeverRunner` |
| `MCMutation` | TRUE | **fail** `Inv_RunnerWins` |

## Generate + test

```bash
tlc --parse specs/span-tree/decision/Decision.tla
tlc --no-deadlock -c specs/span-tree/decision/MC.cfg \
  specs/span-tree/decision/Decision.tla
specgen -o pkg/analyzer/spantreespec -p spantreespec \
  -const Bug=FALSE \
  specs/span-tree/decision/Decision.tla
go test ./pkg/analyzer/spantreespec/
```

Generated package: `pkg/analyzer/spantreespec` (never hand-edit).
