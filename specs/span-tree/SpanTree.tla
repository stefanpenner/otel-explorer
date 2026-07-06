------------------------------ MODULE SpanTree ------------------------------
(***************************************************************************)
(* WHAT THIS SPEC IS FOR                                                   *)
(*                                                                         *)
(* Models the span-tree assembly pipeline of otel-explorer:                *)
(*   DedupeRunnerSpans (dedup.go:23-53, group-based since 2026-07-06)      *)
(*   -> enrich-skip + time-filter + hint-dedup (tree.go:108-138)           *)
(*   -> nodes map, first occurrence wins (tree.go:253-256)                 *)
(*   -> parent linking / root promotion (tree.go:259-275)                  *)
(*   -> parent-cycle containment (tree.go:277-316, added 2026-07-06)       *)
(*                                                                         *)
(* Questions it answers:                                                   *)
(*   1. Is DedupeRunnerSpans idempotent? (It must be: main.go:1073 +       *)
(*      tree.go:97 really do run Dedup(Dedup(x)).)                         *)
(*   2. Is the output a forest -- no cycles, no silent loss?               *)
(*   3. If a span is dropped by filtering while a same-ID duplicate        *)
(*      survives, do that ID's children orphan?                            *)
(*   4. Does dedup output depend on input order?                           *)
(*                                                                         *)
(* ONE CORRECT BEHAVIOR (informal, post-fix):                              *)
(*   input arrives as [apiStep(id2), apiStep'(id2), runnerStep(id2),       *)
(*                     apiJob(id3), selfP(id4), zeroID(id0), runJob(id3)]  *)
(*   1. Dedup resolves per ID group: id2 = {2 api, 1 runner} is ambiguous  *)
(*      -> keep all three; id3 = {1 api, 1 runner} -> runner wins.         *)
(*   2. Filter: nondeterministic drop set (abstracts enrich-skip + time    *)
(*      bounds); hint-dedup drops later spans sharing a DedupKey.          *)
(*   3. Nodes map: first surviving span per spanID.                        *)
(*   4. Link: par=0 -> root; parent in map and != self -> child;           *)
(*      else root.  Then containment: any node unreachable from the        *)
(*      roots (parent cycle) is promoted -- smallest span ID first --      *)
(*      until every emitted span is reachable.  Result: forest, no loss.   *)
(*                                                                         *)
(* ATOMICITY GRAIN: each pipeline stage is one atomic action.  The code    *)
(* is a single sequential function -- no concurrency -- so sub-steps of a  *)
(* stage commute with everything.  All nondeterminism is environmental     *)
(* and lives in Init: the arrival ORDER of the combined span slice (API    *)
(* fetch vs runner artifact ordering, log-fetch appends) and WHICH spans   *)
(* the enricher/time-bounds drop (dropFilter set).                         *)
(*                                                                         *)
(* DATA ABSTRACTION: spanIDs are small ints, 0 = the zero span ID.         *)
(* One trace only (parent lookup is trace-scoped in code; cross-trace      *)
(* is out of scope here).  'filtered' is a nondeterministic predicate.     *)
(* hk models enrichment DedupKey (0 = no key).                             *)
(*                                                                         *)
(* MUTATIONS (old buggy behaviors kept reachable):                         *)
(*   Bug  = TRUE disables the runner-wins pair collapse entirely.          *)
(*   Bug2 = TRUE restores the pre-2026-07-06 arrival-order dedup           *)
(*          (FINDINGS.md #10: not idempotent, order-sensitive).            *)
(*   Bug3 = TRUE removes parent-cycle containment                          *)
(*          (FINDINGS.md #11: cycle spans vanish silently).                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Bug,          \* BOOLEAN: disable runner-wins dedup (mutation)
          Bug2,         \* BOOLEAN: old arrival-order dedup algorithm (mutation)
          Bug3,         \* BOOLEAN: no parent-cycle containment (mutation)
          IncludeCycle, \* BOOLEAN: add a mutual-parent span pair (hostile input)
          FixOrder      \* BOOLEAN: pin arrival order to identity (cheap cfgs)

ASSUME /\ Bug \in BOOLEAN
       /\ Bug2 \in BOOLEAN
       /\ Bug3 \in BOOLEAN
       /\ IncludeCycle \in BOOLEAN
       /\ FixOrder \in BOOLEAN

Sp(id, par, runner, hk) == [id |-> id, par |-> par, runner |-> runner, hk |-> hk]

(* Universe: abstract spans, indexed 1..N.  id 0 = zero span ID.           *)
(*  1: api step        id2 par1        (one of two identically-named steps)*)
(*  2: api step twin   id2 par1        (legitimately distinct, same ID)    *)
(*  3: runner step     id2 par1        (runner-native duplicate of 1)      *)
(*  4: api child       id3 par2 hk1    (child of the colliding ID)         *)
(*  5: self-parent     id4 par4        (hostile: parent = self)            *)
(*  6: zero-ID span    id0 par2 hk1    (shares DedupKey with 4)            *)
(*  7: runner child    id3 par2 hk1    (runner twin of 4: the one          *)
(*                                      unambiguous {1 api, 1 runner} pair)*)
(*  8,9 (IncludeCycle): id5 par6 / id6 par5 (hostile: mutual parents)      *)
BaseSpans == << Sp(2, 1, FALSE, 0),
                Sp(2, 1, FALSE, 0),
                Sp(2, 1, TRUE,  0),
                Sp(3, 2, FALSE, 1),
                Sp(4, 4, FALSE, 0),
                Sp(0, 2, FALSE, 1),
                Sp(3, 2, TRUE,  1) >>

CyclePair == << Sp(5, 6, FALSE, 0), Sp(6, 5, FALSE, 0) >>

Spans == IF IncludeCycle THEN BaseSpans \o CyclePair ELSE BaseSpans
N     == Len(Spans)
Ix    == 1..N

IdOf(s)     == Spans[s].id
ParOf(s)    == Spans[s].par
IsRunner(s) == Spans[s].runner
HkOf(s)     == Spans[s].hk

IdentityOrder == [i \in Ix |-> i]

VARIABLES
  order,      \* arrival permutation of span indices (env choice)
  dropFilter, \* set of spans the enricher/time-bounds drop (env choice)
  stage,      \* "start" -> "deduped" -> "filtered" -> "done"
  dedupOut,   \* sequence after DedupeRunnerSpans
  pipeOut,    \* sequence after enrich/time/hint filtering (the node list)
  parentOf,   \* emitted span -> parent span index, or 0 = root
  roots       \* emitted spans promoted to / placed at root

vars == <<order, dropFilter, stage, dedupOut, pipeOut, parentOf, roots>>

(***************************************************************************)
(* DedupeRunnerSpans, faithful to dedup.go:23-53 (group-based rewrite,     *)
(* 2026-07-06): count api/runner spans per ID over the WHOLE input; a      *)
(* group collapses only when it is exactly {1 api, 1 runner}, and then     *)
(* the api side is dropped (the runner keeps its own position).  Zero-ID   *)
(* spans always pass through.  Deterministic, order-insensitive in its     *)
(* survivor set, and idempotent (a collapsed group re-enters as a runner   *)
(* singleton).                                                             *)
(*                                                                         *)
(* Bug  = TRUE disables the pair collapse (dedup degenerates to identity). *)
(* Bug2 = TRUE restores the OLD per-arrival algorithm (DedupOldRec below): *)
(* seen pins the first out-index, every later same-ID span is compared     *)
(* against whatever now sits there -- not idempotent, order-sensitive.     *)
(***************************************************************************)
CountIn(seq, id, runner) ==
  Cardinality({i \in DOMAIN seq : IdOf(seq[i]) = id /\ IsRunner(seq[i]) = runner})

DedupFixed(seq) ==
  LET Keep(s) == \/ IdOf(s) = 0
                 \/ IsRunner(s)
                 \/ ~(CountIn(seq, IdOf(s), FALSE) = 1 /\ CountIn(seq, IdOf(s), TRUE) = 1)
  IN SelectSeq(seq, Keep)

RECURSIVE DedupOldRec(_, _, _)
DedupOldRec(rem, out, seen) ==
  IF rem = <<>> THEN out
  ELSE LET s == Head(rem) IN
    IF IdOf(s) = 0
      THEN DedupOldRec(Tail(rem), Append(out, s), seen)
    ELSE IF IdOf(s) \in DOMAIN seen
      THEN LET prev == out[seen[IdOf(s)]] IN
        IF IsRunner(s) # IsRunner(prev)
          THEN IF IsRunner(s)
                 THEN DedupOldRec(Tail(rem), [out EXCEPT ![seen[IdOf(s)]] = s], seen)
                 ELSE DedupOldRec(Tail(rem), out, seen)   \* api dropped
          ELSE DedupOldRec(Tail(rem), Append(out, s), seen)
    ELSE DedupOldRec(Tail(rem), Append(out, s),
                     seen @@ (IdOf(s) :> Len(out) + 1))

DedupSeq(seq) ==
  IF Bug2 THEN DedupOldRec(seq, <<>>, <<>>)
  ELSE IF Bug THEN seq
  ELSE DedupFixed(seq)

(***************************************************************************)
(* Filter pass, faithful to tree.go:108-138 loop order:                    *)
(* enrich-skip/time-filter (abstracted as membership in drop) happen       *)
(* BEFORE the DedupKey is registered, so a dropped span does not consume   *)
(* its hint key (tree.go:117-135).                                         *)
(***************************************************************************)
RECURSIVE FilterRec(_, _, _, _)
FilterRec(rem, out, seenHk, drop) ==
  IF rem = <<>> THEN out
  ELSE LET s == Head(rem) IN
    IF s \in drop
      THEN FilterRec(Tail(rem), out, seenHk, drop)
    ELSE IF HkOf(s) # 0 /\ HkOf(s) \in seenHk
      THEN FilterRec(Tail(rem), out, seenHk, drop)
    ELSE FilterRec(Tail(rem), Append(out, s),
                   seenHk \cup (IF HkOf(s) = 0 THEN {} ELSE {HkOf(s)}),
                   drop)

FilterSeq(seq, drop) == FilterRec(seq, <<>>, {}, drop)

RangeSeq(seq) == {seq[i] : i \in DOMAIN seq}
DedupedSet    == RangeSeq(dedupOut)
Emitted       == RangeSeq(pipeOut)

(* nodes map, first occurrence wins (tree.go:253-256): the node serving    *)
(* parent lookups for an id is the FIRST span in pipeOut with that id.     *)
HasNode(id) == \E i \in DOMAIN pipeOut : IdOf(pipeOut[i]) = id
NodeFor(id) ==
  pipeOut[CHOOSE i \in DOMAIN pipeOut :
            /\ IdOf(pipeOut[i]) = id
            /\ \A j \in DOMAIN pipeOut : IdOf(pipeOut[j]) = id => i <= j]

(***************************************************************************)
(* Parent-cycle containment, faithful to tree.go:277-316 (2026-07-06):     *)
(* while some emitted node is unreachable from the roots, promote the      *)
(* unreachable node with the smallest span ID (deterministic; the code     *)
(* tie-breaks by node-list position, modeled as smallest index -- no tie   *)
(* is reachable in this universe) to root, then recheck.  Bug3 = TRUE      *)
(* skips containment (the pre-fix code).                                   *)
(***************************************************************************)
RECURSIVE ReachIn(_, _)
ReachIn(S, pf) ==
  LET nxt == S \cup {s \in DOMAIN pf : pf[s] \in S}
  IN IF nxt = S THEN S ELSE ReachIn(nxt, pf)

RECURSIVE Contain(_)
Contain(pf) ==
  LET em   == DOMAIN pf
      unre == em \ ReachIn({s \in em : pf[s] = 0}, pf)
  IN IF unre = {} THEN pf
     ELSE LET m == CHOOSE s \in unre :
                     \A t \in unre : \/ IdOf(s) < IdOf(t)
                                     \/ (IdOf(s) = IdOf(t) /\ s <= t)
          IN Contain([pf EXCEPT ![m] = 0])

-----------------------------------------------------------------------------

(* Spans 1 and 2 are identical records, so permutations differing only in *)
(* their order are isomorphic; keep the representative with 1 first.      *)
PosOf(f, s) == CHOOSE i \in Ix : f[i] = s

Init ==
  /\ order \in (IF FixOrder
                  THEN {IdentityOrder}
                  ELSE {f \in [Ix -> Ix] :
                          /\ \A i, j \in Ix : (i # j) => f[i] # f[j]
                          /\ PosOf(f, 1) < PosOf(f, 2)})
  /\ dropFilter \in SUBSET Ix
  /\ stage = "start"
  /\ dedupOut = <<>>
  /\ pipeOut  = <<>>
  /\ parentOf = <<>>
  /\ roots    = {}

DoDedup ==                                   \* tree.go:97 / dedup.go:23-53
  /\ stage = "start"
  /\ stage' = "deduped"
  /\ dedupOut' = DedupSeq(order)
  /\ UNCHANGED <<order, dropFilter, pipeOut, parentOf, roots>>

DoFilter ==                                  \* tree.go:108-138
  /\ stage = "deduped"
  /\ stage' = "filtered"
  /\ pipeOut' = FilterSeq(dedupOut, dropFilter)
  /\ UNCHANGED <<order, dropFilter, dedupOut, parentOf, roots>>

DoLink ==                                    \* tree.go:259-316
  /\ stage = "filtered"
  /\ stage' = "done"
  /\ LET pf0 == [s \in Emitted |->
                   IF ParOf(s) = 0 THEN 0                      \* tree.go:266
                   ELSE IF HasNode(ParOf(s)) /\ NodeFor(ParOf(s)) # s
                          THEN NodeFor(ParOf(s))               \* tree.go:268
                          ELSE 0]                              \* tree.go:272
         pf  == IF Bug3 THEN pf0 ELSE Contain(pf0)             \* tree.go:277-316
     IN /\ parentOf' = pf
        /\ roots' = {s \in Emitted : pf[s] = 0}
  /\ UNCHANGED <<order, dropFilter, dedupOut, pipeOut>>

Terminating ==
  /\ stage = "done"
  /\ UNCHANGED vars

Next == DoDedup \/ DoFilter \/ DoLink \/ Terminating

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Reachability from roots along child edges (inverse of parentOf).       *)
ReachableFromRoots == ReachIn(roots, parentOf)

-----------------------------------------------------------------------------
(* INVARIANTS                                                              *)

TypeOK ==
  /\ order \in [Ix -> Ix]
  /\ \A i, j \in Ix : (i # j) => order[i] # order[j]
  /\ dropFilter \subseteq Ix
  /\ stage \in {"start", "deduped", "filtered", "done"}
  /\ Len(dedupOut) <= N /\ \A i \in DOMAIN dedupOut : dedupOut[i] \in Ix
  /\ Len(pipeOut)  <= N /\ \A i \in DOMAIN pipeOut  : pipeOut[i]  \in Ix
  /\ DOMAIN parentOf \subseteq Ix
  /\ \A s \in DOMAIN parentOf : parentOf[s] \in ({0} \cup Ix)
  /\ roots \subseteq Ix

(* Every stage only narrows the span set.  [proposed]                      *)
PipelineMonotone == Emitted \subseteq DedupedSet /\ DedupedSet \subseteq Ix

(* Full accounting: every input span is emitted, dropped-by-dedup, or      *)
(* dropped-by-filter.  [proposed]                                          *)
Accounting ==
  stage = "done" =>
    Ix = (Ix \ DedupedSet) \cup (DedupedSet \ Emitted) \cup Emitted

(* No silent loss / forest shape: every emitted node is reachable from a   *)
(* root.  Was FINDINGS.md #11 (parent cycles vanished); fixed 2026-07-06   *)
(* by the containment pass.  MCMutation2 (Bug3) proves the teeth.          *)
(* [proposed]                                                              *)
NoSilentLossForest ==
  stage = "done" => Emitted \subseteq ReachableFromRoots

(* Dedup is idempotent.  Was FINDINGS.md #10 (the old arrival-order        *)
(* algorithm refuted the tree.go comment); fixed 2026-07-06 by the         *)
(* group-based rewrite.  Dedup(Dedup(x)) really runs: main.go:1073 then    *)
(* tree.go:97.  MCMutation1 (Bug2) proves the teeth.  [code-documented:    *)
(* tree.go:92-96 comment; TestDedupSpec_Idempotent]                        *)
DedupIdempotent == DedupSeq(dedupOut) = dedupOut

(* The unambiguous API/runner twin pair -- spans 4 and 7, the only id      *)
(* group that is exactly {1 api, 1 runner} -- collapses to the runner.     *)
(* [code-documented: dedup_test.go TestRunnerSpanDedup expects the pair    *)
(* to collapse to the runner span; TestBuildTreeDedupsDuplicateJob         *)
(* expects 1 node]                                                         *)
DedupCollapsesRunnerPair ==
  stage # "start" => (7 \in DedupedSet /\ 4 \notin DedupedSet)

(* An ambiguous colliding group -- id2 has {2 api, 1 runner} -- is kept    *)
(* whole: no span of it is silently dropped.  Pins the keep-all choice of  *)
(* the 2026-07-06 rewrite.  [proposed]                                     *)
DedupKeepsAmbiguousGroup ==
  stage # "start" => {1, 2, 3} \subseteq DedupedSet

(* Which spans survive dedup does not depend on arrival order.  Was        *)
(* FINDINGS.md #10 (Finding 3); fixed 2026-07-06.  MCMutation3 (Bug2)      *)
(* proves the teeth.  [proposed]                                           *)
DedupOrderInsensitive ==
  stage = "start" \/ DedupedSet = RangeSeq(DedupSeq(IdentityOrder))

(* The discovery's orphan question: if ANY same-ID span survives the       *)
(* filter, children of that ID link to it (the nodes map is built AFTER    *)
(* filtering, so the stored occurrence is always a surviving one).         *)
(* Holds without hostile cycles; a cycle-promoted node is the documented   *)
(* exception (its parent survives but the edge is cut), so cycle configs   *)
(* omit this invariant.  [proposed]                                        *)
ChildrenLinkWhenParentIdSurvives ==
  stage = "done" =>
    \A s \in Emitted :
      (ParOf(s) # 0 /\ (\E t \in Emitted : t # s /\ IdOf(t) = ParOf(s)))
        => (parentOf[s] # 0 /\ IdOf(parentOf[s]) = ParOf(s))

(* Self-parent guard holds (tree.go:268 "parent # node").  [proposed]      *)
SelfParentIsRoot ==
  (stage = "done" /\ 5 \in Emitted) => parentOf[5] = 0

(* Zero-ID spans never serve as parents (tree.go:266 short-circuits        *)
(* par=0 to root before any map lookup).  [proposed]                       *)
ZeroIdNeverParent ==
  stage = "done" =>
    \A s \in Emitted : parentOf[s] # 0 => IdOf(parentOf[s]) # 0

(* BAIT: claims no span with a nonzero parent ref is ever promoted to      *)
(* root.  Clearly false -- a filtered/missing parent promotes children.    *)
(* MCBait.cfg MUST fail; the witness is the promotion path tree.go:272.    *)
BaitNoRootPromotion ==
  stage = "done" =>
    \A s \in Emitted : ParOf(s) # 0 => parentOf[s] # 0

=============================================================================
