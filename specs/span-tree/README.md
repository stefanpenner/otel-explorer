# span-tree — TLA+ model of the span pipeline

## Purpose

Model `BuildTreeFromSpans` (pkg/analyzer/tree.go:87-325) and
`DedupeRunnerSpans` (pkg/analyzer/dedup.go:23-53):

dedup runner spans → enrich-filter → time-filter → hint-dedup
→ nodes map (first occurrence wins) → parent linking
→ parent-cycle containment.

Questions:

1. Is dedup idempotent? (`Dedup(Dedup(x))` really runs:
   main.go:1073 then tree.go:97)
2. Is the output a forest — no cycles, no silent span loss?
3. If a span is filtered while a same-ID duplicate survives,
   do children orphan?
4. Does dedup output depend on input order?

**Status (2026-07-06)**: Findings 1–3 (FINDINGS.md #10, #11) are FIXED
in code; their invariants are now green in MC.cfg / MCCycle.cfg, and the
old buggy behaviors stay reachable behind `Bug2`/`Bug3` so the mutation
configs still fail.

## Behavior sketch (one correct run, post-fix)

Input arrives: `[apiA(id2), apiA'(id2), runner(id2), apiJob(id3→2), selfP(id4→4), zero(id0), runJob(id3→2)]`

1. Dedup resolves per (traceID, spanID) group: id2 = {2 api, 1 runner}
   is ambiguous → keep all three; id3 = {1 api, 1 runner} → the runner
   survives (sub-second timing), the API twin is dropped.
2. Filter: nondeterministic drop set (abstracts enrich-skip + time
   bounds); hint-dedup drops later spans sharing a DedupKey.
3. Nodes map: first surviving span per spanID.
4. Link: par=0 → root; parent in map and ≠ self → child; else root.
   Containment: any emitted node unreachable from the roots (parent
   cycle) is promoted to root — smallest span ID first — until all
   emitted spans are reachable.

**Grain**: one atomic action per pipeline stage. The code is a single
sequential function — no concurrency. All nondeterminism is
environmental and lives in Init: arrival order + drop set.

## Universe (7 spans; 9 with IncludeCycle)

| ix | id | par | runner | hk | why |
|----|----|-----|--------|----|-----|
| 1 | 2 | 1 | no  | - | api step (one of two identically-named) |
| 2 | 2 | 1 | no  | - | api step twin — distinct span, same ID |
| 3 | 2 | 1 | yes | - | runner-native duplicate of 1 |
| 4 | 3 | 2 | no  | 1 | api child of the colliding ID |
| 5 | 4 | 4 | no  | - | self-parent (hostile) |
| 6 | 0 | 2 | no  | 1 | zero-ID span; shares DedupKey with 4 |
| 7 | 3 | 2 | yes | 1 | runner twin of 4: the one {1 api, 1 runner} pair |
| 8 | 5 | 6 | no  | - | mutual-parent pair (IncludeCycle only) |
| 9 | 6 | 5 | no  | - | mutual-parent pair (IncludeCycle only) |

## Action → code map

| Action | Code |
|--------|------|
| Init (order) | main.go:1073 append order; log-fetch appends main.go:1193 |
| Init (dropFilter) | enrich-skip tree.go:117; time bounds tree.go:122-127 |
| DoDedup | dedup.go:23-53 (group-based), called from tree.go:97 |
| DoFilter | tree.go:108-138 (incl. hint-dedup tree.go:130-135) |
| DoLink | nodes map tree.go:253-256; linking tree.go:259-275; cycle containment tree.go:277-316 |

## Invariants

| Invariant | Source | Result |
|-----------|--------|--------|
| TypeOK | proposed | green |
| PipelineMonotone | proposed | green |
| Accounting (emitted ∪ dedup-dropped ∪ filter-dropped = all) | proposed | green |
| NoSilentLossForest (emitted ⇒ reachable from roots) | proposed | green — **was Finding 2 / FINDINGS.md #11, fixed 2026-07-06**; MCMutation2 (Bug3) fails |
| DedupIdempotent | code-documented: tree.go:92-96 comment, TestDedupSpec_Idempotent | green — **was Finding 1 / FINDINGS.md #10, fixed 2026-07-06**; MCMutation1 (Bug2) fails |
| DedupOrderInsensitive | proposed; TestDedupSpec_OrderInsensitive | green — **was Finding 3 / FINDINGS.md #10, fixed 2026-07-06**; MCMutation3 (Bug2) fails |
| DedupCollapsesRunnerPair (spans 4/7 → runner survives) | code-documented: dedup_test.go TestRunnerSpanDedup, TestBuildTreeDedupsDuplicateJob | green; fails under Bug (MCMutation) |
| DedupKeepsAmbiguousGroup (id2 triple kept whole) | proposed; pins the keep-all choice of the group rewrite | green |
| ChildrenLinkWhenParentIdSurvives | proposed | green — answers question 3: no orphaning. Omitted in cycle configs: a cycle-promoted node is its documented exception (edge cut on purpose) |
| SelfParentIsRoot | proposed (guard tree.go:268) | green |
| ZeroIdNeverParent | proposed (tree.go:266) | green |
| BaitNoRootPromotion | bait | fails, as required |

## Configs and results (2026-07-06, tla2tools, 4 workers)

| Config | Constants | Expected | Actual | States gen / distinct | Time |
|--------|-----------|----------|--------|----------------------|------|
| MC | all bugs F, Cycle=F, FixOrder=F | green | green | 1,612,800 / 1,290,240 | 11s |
| MCCycle | all bugs F, Cycle=T, FixOrder=T | green | green | 2,560 / 2,048 | <1s |
| MCBait | FixOrder=T | FAIL | FAIL (BaitNoRootPromotion) | 388 / 388 | <1s |
| MCMutation | Bug=T | FAIL | FAIL (DedupCollapsesRunnerPair) | 132 / 132 | <1s |
| MCMutation1 | Bug2=T, FixOrder=T | FAIL | FAIL (DedupIdempotent) | 132 / 132 | <1s |
| MCMutation2 | Bug3=T, Cycle=T, FixOrder=T | FAIL | FAIL (NoSilentLossForest) | 1,540 / 1,540 | <1s |
| MCMutation3 | Bug2=T, FixOrder=F | FAIL | FAIL (DedupOrderInsensitive) | 337,924 / 337,924 | 2s |

Bounds: 1 trace, 7 spans (9 with cycle pair), full permutation of
arrival order (quotiented by the identical-span symmetry 1↔2), all
2^N filter subsets. Cycle configs pin FixOrder=T (9! orders is
intractable; dedup order coverage lives in MC). **Green TLC at these
bounds = strong bug hunt, not proof.**

Mutation checks:
- `Bug=TRUE` disables the runner-wins pair collapse → the spans-4/7
  pair survives whole → DedupCollapsesRunnerPair trips.
- `Bug2=TRUE` restores the pre-fix arrival-order dedup (`seen` pins the
  first out-index; later same-ID spans compare against whatever now
  sits there) → DedupIdempotent and DedupOrderInsensitive trip.
- `Bug3=TRUE` removes the parent-cycle containment pass →
  NoSilentLossForest trips on the mutual-parent pair.

## Bait witness (MCBait — root promotion is reachable)

order identity, dropFilter={1}: spans 2 and 3 (id2, par=1, and no
id-1 span exists in the batch) end in `roots`; `parentOf[2]=0` with
`ParOf(2)=1≠0`. The promotion path tree.go:272 fires. Bait correctly
fails.

## Finding 1 — dedup was NOT idempotent — FIXED 2026-07-06

Old witness (identity order over the 6-span universe): input
`[1,2,3,4,5,6]` → first dedup `<<3,2,4,5,6>>` (runner 3 replaced api 1,
api twin 2 kept) → second dedup `<<3,4,5,6>>` — span 2 dropped: it
collided with runner 3 at the pinned first index and lost the
runner-vs-api comparison.

This was live: main.go:1073 dedups the combined slice, tree.go:97
dedups it again inside BuildTreeFromSpans.

**Fix (dedup.go:23-53)**: resolve per (traceID, spanID) group —
collapse only when a group is exactly {1 api, 1 runner} (runner
survives); any other shape is kept whole. Idempotent by construction:
a collapsed group re-enters as a runner singleton. Pinned in Go by
`TestDedupSpec_Idempotent` (property test over the corpus of these
traces + 500 generated cases). Old behavior reachable via `Bug2`
(MCMutation1 still fails).

## Finding 2 — parent cycles silently vanished — FIXED 2026-07-06

Old witness: dropFilter={3}: the mutual-parent spans (ids 5,6) both
survive → `parentOf = (8 :> 9 @@ 9 :> 8)`; neither is a root and
neither is reachable; FlattenTree walks from roots, so both spans
disappeared from every renderer with no signal.

**Fix (tree.go:277-316)**: after linking, a reachability pass from the
roots; while any emitted node is unreachable, the one with the
smallest span ID is detached from its parent (so it renders exactly
once), annotated with `otel-explorer.parent_cycle`, and promoted to
root; the rest of the cycle hangs under it. Pinned in Go by
`TestTreeSpec_ParentCycleSpansReachable` (mutual pair, 3-cycle, cycle
with a tail). Old behavior reachable via `Bug3` (MCMutation2 still
fails). MCCycle proves NoSilentLossForest green with hostile input.

## Finding 3 — dedup survivors depended on arrival order — FIXED 2026-07-06

Old witness: `[1,2,3]` (api, api', runner) kept {3,2} but `[1,3,2]`
kept only {3}: after the runner replaced index 1, the later api twin
compared against the runner and was dropped. Same root cause as
Finding 1. Combined-slice order is append-dependent (main.go:1073,
main.go:1193), so which spans rendered could differ between loads.

**Fix**: same group-based rewrite as Finding 1 — the survivor set is a
function of the input multiset only. Pinned in Go by
`TestDedupSpec_OrderInsensitive` (all permutations of the collision
corpus). Old behavior reachable via `Bug2` (MCMutation3 still fails).

## Notes

- The discovery's "re-validate parents after filtering" hint is NOT
  needed: the nodes map is built AFTER filtering, so
  ChildrenLinkWhenParentIdSurvives holds over all MC states — children
  always link to a surviving same-ID span if one exists.
- Behavior choice pinned by DedupKeepsAmbiguousGroup: a colliding
  group that is not exactly {1 api, 1 runner} (e.g. 2 api + 1 runner)
  is kept WHOLE — the old code collapsed one api into the runner.
  Chosen per FINDINGS.md #10 ("else keep all").

## Running

```sh
cd specs/span-tree
/opt/homebrew/opt/openjdk/bin/java -cp ~/.cache/tla2tools.jar tla2sany.SANY SpanTree.tla
/opt/homebrew/opt/openjdk/bin/java -XX:+UseParallelGC -cp ~/.cache/tla2tools.jar \
  tlc2.TLC -workers 4 -cleanup -deadlock -config MC.cfg SpanTree.tla
```

Or: `scripts/check-specs.sh span-tree`
