# LogGroupsDecision — stack-depth decision core

Scalar state machine for `##[group]` / `##[endgroup]` balance.

Full multi-line model (records, sequences, gap parse):
`specs/log-groups/LogGroups.tla`.

This core is **specgen-clean**: int + bool only.

## State

| Var | Domain | Meaning |
|-----|--------|---------|
| `depth` | `(-MaxDepth-1)..MaxDepth` | open-group stack depth |

## Actions → code

| Action | Guard | Effect | Code |
|--------|-------|--------|------|
| `Open` | `depth >= 0 /\ depth < MaxDepth` | `depth+1` | `canOpenGroup` |
| `Close` | `depth > 0` | `depth-1` | `canCloseGroup` |
| `CloseBug` | `Bug /\ depth = 0` | `depth-1` (underflow) | mutation: endgroup at empty stack |
| `Terminating` | — | stutter | terminal/idle |

Production ignores stray endgroup when no open group
(`canCloseGroup` false). That is `~CloseBug` / `Inv_DepthNonNeg`.

## Invariants

| Name | Expected |
|------|----------|
| `TypeOK` | always |
| `Inv_DepthNonNeg` | green when `Bug=FALSE`; fails under `MCMutation` |
| `BaitNeverOpens` | fails under `MCBait` (Open is reachable) |

## Configs

| Config | Bug | Expected |
|--------|-----|----------|
| `MC` | FALSE | pass |
| `MCBait` | FALSE | **fail** `BaitNeverOpens` |
| `MCMutation` | TRUE | **fail** `Inv_DepthNonNeg` |

## Generate + test

```bash
tlc --parse specs/log-groups/decision/Decision.tla
tlc --no-deadlock -c specs/log-groups/decision/MC.cfg \
  specs/log-groups/decision/Decision.tla
specgen -o pkg/logparse/loggroupsspec -p loggroupsspec \
  -const MaxDepth=3 -const Bug=FALSE \
  specs/log-groups/decision/Decision.tla
go test ./pkg/logparse/loggroupsspec/
```

Generated package: `pkg/logparse/loggroupsspec` (never hand-edit).
