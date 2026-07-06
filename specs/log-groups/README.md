# LogGroups — TLA+ spec for the GHA log-group state machine

Models `pkg/logparse/timestamp.go` (post-fix, 2026-07-06):

- `splitGroups` (44-90) — OutsideGroup / InsideGroup scan;
  nested `##[group]` implicit-closes the outer group (51-63)
- `parseWithGroups` (111-186) — sort by start (114-116),
  group-span end override (123-130) + clamp (134-139),
  child clamp (157, `clampSpans` 205-222)
- `parseGapBased` (226-310) — gap splitting, forward-only
  baseline (263-268), minSpan filter (288-291)

**Purpose:** timestamps in real GHA logs are not guaranteed monotonic
(buffered stream interleave), and the caller-supplied step window can
disagree with log times (the fuzz harness already drives an inverted
window on purpose, `fuzz_test.go:43-44`). The code used to assume
monotonic order — that was FINDINGS.md #12 (MCFinding1-4, fixed
2026-07-06). This spec checks the ordering / nesting properties under
that fault. Byte-level robustness is fuzzing's job, not this spec's.

## One correct behavior (fixes the atomicity grain)

1. `##[group]A` @1 → current = {start 1}
2. plain @1 → collected
3. `##[endgroup]` @2 → groups = [A: 1..2]
4. `##[group]B` @3, `##[endgroup]` @4 → groups = [A, B]
5. EOF, stepEnd 4
6. Produce: span A = [1,3] (end → B's start), span B = [3,4] (end → stepEnd)

Grain: one log line per action (the scan loop body).
EOF + stepEnd choice = one action.
`parseWithGroups` + `parseGapBased` are deterministic, nothing
interleaves → one atomic `Produce` action (pure operators).

## Discovery-evidence corrections (verified against code + git history)

- Nested `##[group]` used to overwrite `current` and silently **drop**
  the outer group and its lines (original commit cac23b9 → 2026-07-05).
  Fixed 2026-07-06: implicit-close at the open line's time (51-57).
  The old behavior stays reachable via `BugNest`.
- No group ever reaches `parseWithGroups` with a zero end
  (nested open sets it at line 55, endgroup at 67, EOF at 81-85).
  So the 123-130 override is an unconditional extension,
  not a zero-end repair. `Inv_GroupEndsSet` is green.
  Answer to "zero-end group is LAST": unreachable.
- `parseGapBased`'s `regionStart` parameter was unused — deleted
  2026-07-06 (signature is now `(lines, regionEnd)`).

## Action → code map

| Action        | Code                                                      |
|---------------|-----------------------------------------------------------|
| `Open(t)`     | `timestamp.go:51-63` (implicit-close outer group; `BugNest` = pre-fix drop) |
| `Close(t)`    | `timestamp.go:65-72` (stray endgroup ignored)             |
| `Plain(t)`    | `timestamp.go:73-77`                                      |
| `EndInput(se)`| `timestamp.go:79-87` EOF close; caller's stepEnd at :92   |
| `Produce`     | `timestamp.go:111-186` + `parseGapBased` 226-310 as pure fold (`GapFoldR`, forward-only `current.end` at :263-268, `BugGap` = pre-fix unconditional; minSpan filter :288-291; sort :114-116 via `SortByStart`, `BugSort` skips; end clamp :134-139, `BugClamp` skips; child clamp :157/:205-222 via `ClampKid`, `BugChild` skips) |
| `Terminating` | done-state stutter                                        |

Abstractions dropped (cannot affect ordering properties):
names, `collapseSpans`, `groupByPrefix`, `filterBySignificance`,
top-level gap spans. Time = 1..MaxTime, 0 = Go zero value.
`clampSpans` runs recursively after collapse/prefix merges, so the
merge abstraction stays sound post-fix.

## Invariants

| Invariant                    | Source          | Status | Citation                                   |
|------------------------------|-----------------|--------|--------------------------------------------|
| `TypeInvariant`              | —               | green  | typing                                     |
| `Inv_GroupEndsSet`           | proposed        | green  | structural (see corrections above)         |
| `Inv_GroupsSortedByStart`    | proposed        | **fixed 2026-07-06** | was MCFinding2; sort at `timestamp.go:114-116`; checked at pc="done" (the point of use — mid-scan the append order is close order by design) |
| `Inv_SpanStartLeEnd`         | proposed        | **fixed 2026-07-06** | was MCFinding1; clamp at `timestamp.go:134-139` |
| `Inv_InteriorSpanStartLeEnd` | proposed        | **fixed 2026-07-06** | was MCFinding4; sort + clamp               |
| `Inv_ChildInParent`          | proposed        | **fixed 2026-07-06** | was MCFinding3; `clampSpans` at `timestamp.go:157` |
| `Inv_GroupSpansTile`         | code-documented | green  | comment `timestamp.go:124,127-128`; test `logparse_test.go:67-68`; clamp only raises ends, so tiling survives it |
| `Inv_NoNestedDrop`           | proposed        | **fixed 2026-07-06** | nested-open data loss (bait witness); implicit-close `timestamp.go:51-57` |
| `BaitNoTwoGroupSpans`        | bait            | must fail | —                                       |

Go tests mirroring the witness traces:
`pkg/logparse/timestamp_spec_test.go`
(`TestLogGroupsSpec_NonMonotonicWitnesses`,
`TestLogGroupsSpec_NestedGroupImplicitClose`,
`TestLogGroupsSpec_GapBaselineOnlyMovesForward`,
adversarial table `TestParseGroupsNeverPanics`).
Each failed against the pre-fix code.

## Configs, bounds, results (2026-07-06, tla2tools, 4 workers)

Bounds everywhere: MaxTime=4, MaxLines=5, MinGap=2, MinSpan=1.

| Config         | Monotonic | Mutation           | Expected | Result                                | states gen/distinct | time |
|----------------|-----------|--------------------|----------|----------------------------------------|---------------------|------|
| MC             | TRUE      | —                  | green    | **green**                              | 27,591 / 17,609     | ~1s  |
| MCNonMonotonic | FALSE     | —                  | green    | **green**                              | 785,392 / 512,383   | ~3s  |
| MCBait         | TRUE      | —                  | FAIL     | FAIL `BaitNoTwoGroupSpans`             | 661 / 542           | <1s  |
| MCMutation     | TRUE      | Bug (no override)  | FAIL     | FAIL `Inv_GroupSpansTile`              | 630 / 511           | <1s  |
| MCMutation1    | FALSE     | BugClamp           | FAIL     | FAIL `Inv_SpanStartLeEnd`              | 1,503 / 1,235       | <1s  |
| MCMutation2    | FALSE     | BugSort            | FAIL     | FAIL `Inv_GroupsSortedByStart`         | 8,096 / 6,716       | <1s  |
| MCMutation3    | FALSE     | BugChild           | FAIL     | FAIL `Inv_ChildInParent`               | 24,649 / 20,285     | <1s  |
| MCMutation4    | FALSE     | BugSort + BugClamp | FAIL     | FAIL `Inv_InteriorSpanStartLeEnd`      | 5,351 / 4,451       | <1s  |
| MCMutation5    | TRUE      | BugNest            | FAIL     | FAIL `Inv_NoNestedDrop`                | 68 / 47             | <1s  |

MCMutation1-4 are the former MCFinding1-4: the same fault model
(Monotonic=FALSE) with one fix surgically disabled — proving each
invariant keeps its teeth against the pre-fix behavior.

In MC (Monotonic), the environment also constrains stepEnd to be 0 or
>= the last line time — that IS the documented happy-path assumption.
MCNonMonotonic drops both constraints and is still green post-fix.

`BugGap` (pre-fix backward gap baseline) has no invariant-level teeth at
these bounds — its spurious splits are a quality bug, not a safety bug
(the inverted split it can create is always eaten by the MinSpan
filter). Its regression teeth live in the Go test
`TestLogGroupsSpec_GapBaselineOnlyMovesForward`.

**Green TLC at these bounds = strong bug hunt, not proof.**

## Witness traces (essence, pre-fix — now the MCMutation1-5 witnesses)

- **Bait**: open@1 → open@1 → close@1 → open@1 → EOF → Produce ⇒
  2 group spans. (Pre-fix this also demonstrated the nested-open data
  loss; post-fix the outer group survives as its own span.)
- **Mutation** (Bug): open@1 → EOF(stepEnd=2) → Produce ⇒
  last span ends at 1 < stepEnd 2. Override skipped ⇒ tiling broken.
- **Mutation1** (BugClamp, was Finding 1): open@2 → EOF(stepEnd=1) →
  Produce ⇒ span [start 2, end 1]. Fixed code clamps to [2,2].
- **Mutation2** (BugSort, was Finding 2): groups appended in close
  order; open@2/close, open@1 ⇒ `groups` unsorted by start at Produce.
  Fixed code sorts at parseWithGroups entry.
- **Mutation3** (BugChild, was Finding 3): open@1, plain@1, plain@3,
  EOF(stepEnd=1) ⇒ parent span [1,1], child span [1,3]. Fixed code
  clamps the child to [1,1].
- **Mutation4** (BugSort+BugClamp, was Finding 4): open@2 → close@1 →
  open@1 → EOF ⇒ interior span [start 2, end 1] purely via the
  next-group override — no stepEnd involved.
- **Mutation5** (BugNest): open@1 → open@1 ⇒ outer group dropped
  (`dropped=1`). Fixed code implicit-closes and appends it.

## Fix log (2026-07-06, FINDINGS.md #12)

All four recommendations from the 2026-07-05 campaign landed in
`pkg/logparse/timestamp.go`:

1. sort `groups` by start at parseWithGroups entry + clamp the end
   override (kills Findings 1, 2, 4)
2. clamp gap-parsed children into `[g.start, endTime]` (kills Finding 3)
3. `current.end` only advances forward in parseGapBased
4. nested `##[group]` implicit-closes the outer group instead of
   dropping it and its lines

Simplifications also landed: unused `regionStart` parameter of
parseGapBased deleted; the zero-end-group story stays dead (ends are
always set at append; 123-139 is an extension rule, now clamped).
