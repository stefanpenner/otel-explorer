----------------------- MODULE Decision -----------------------
(***************************************************************************)
(* Logical name: LogGroupsDecision                                         *)
(* File is Decision.tla so SANY module name matches the path.              *)
(***************************************************************************)
(***************************************************************************)
(* Decision core: stack-depth model for GHA ##[group]/##[endgroup].       *)
(*                                                                         *)
(* Full design model: specs/log-groups/LogGroups.tla (records, sequences). *)
(* This core pins the pure depth decision production code must obey:       *)
(*   open  -> depth+1 when under MaxDepth                                  *)
(*   close -> depth-1 when depth > 0                                       *)
(*   under Bug, a close at depth 0 is allowed and depth goes negative      *)
(*                                                                         *)
(* Code: pkg/logparse/timestamp.go splitGroups (open/close of current).    *)
(* Full nesting is one-current + implicit-close; depth abstracts balance.  *)
(*                                                                         *)
(* Specgen-supported subset only (no records/quantifiers/sequences).       *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    MaxDepth,   \* maximum nesting depth the environment explores
    Bug         \* TRUE: allow Close at depth 0 (underflow)

ASSUME MaxDepth \in Nat /\ MaxDepth >= 1
ASSUME Bug \in BOOLEAN

VARIABLES depth

vars == <<depth>>

\* Allow negative under Bug so CloseBug is type-legal.
TypeOK == depth \in ((-MaxDepth) - 1)..MaxDepth

Init == depth = 0

\* ##[group] — push when room remains and stack is non-negative.
Open ==
    /\ depth >= 0
    /\ depth < MaxDepth
    /\ depth' = depth + 1

\* ##[endgroup] — pop when a group is open.
Close ==
    /\ depth > 0
    /\ depth' = depth - 1

\* Mutation: stray endgroup at depth 0 underflows (pre-fix / Bug path).
CloseBug ==
    /\ Bug
    /\ depth = 0
    /\ depth' = depth - 1

Terminating == UNCHANGED depth

Next == Open \/ Close \/ CloseBug \/ Terminating

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* [code-documented] Group stack never underflows when Bug is off.
\* Maps to splitGroups ignoring stray ##[endgroup] when current is nil
\* (timestamp.go:65-72). CloseBug only enabled under Bug.
Inv_DepthNonNeg == depth >= 0

\* BAIT: claims depth never leaves 0. Open reaches depth 1 — MUST FAIL.
BaitNeverOpens == depth = 0

=============================================================================
