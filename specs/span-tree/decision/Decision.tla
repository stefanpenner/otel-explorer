----------------------- MODULE Decision -----------------------
(***************************************************************************)
(* Logical name: SpanTreeDecision                                          *)
(* File is Decision.tla so SANY module name matches the path.              *)
(***************************************************************************)
(***************************************************************************)
(* Decision core: runner-wins keep decision for ONE collision group.       *)
(*                                                                         *)
(* Full design model: specs/span-tree/SpanTree.tla (records, sequences).   *)
(* This core pins the pure keep decision for the unambiguous pair          *)
(* {1 api, 1 runner} sharing a span ID:                                    *)
(*   both seen + DedupChoose  => kept = "runner"                           *)
(*   under Bug, DedupBug keeps "api" instead                               *)
(*                                                                         *)
(* Code: pkg/analyzer/dedup.go DedupeRunnerSpans (group collapse).         *)
(*                                                                         *)
(* Specgen-supported subset only (no records/quantifiers/sequences).       *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS Bug   \* TRUE: wrong keep prefers api when both present

ASSUME Bug \in BOOLEAN

VARIABLES
    haveAPI,     \* TRUE after seeing the API reconstruction span
    haveRunner,  \* TRUE after seeing the runner-native span
    kept,        \* "none" | "api" | "runner" after dedup choice
    done         \* TRUE once a keep decision has been taken

vars == <<haveAPI, haveRunner, kept, done>>

TypeOK ==
    /\ haveAPI \in BOOLEAN
    /\ haveRunner \in BOOLEAN
    /\ kept \in {"none", "api", "runner"}
    /\ done \in BOOLEAN

Init ==
    /\ haveAPI = FALSE
    /\ haveRunner = FALSE
    /\ kept = "none"
    /\ done = FALSE

\* Observe the API-side span of the colliding ID.
SeeAPI ==
    /\ ~done
    /\ ~haveAPI
    /\ haveAPI' = TRUE
    /\ UNCHANGED <<haveRunner, kept, done>>

\* Observe the runner-side span of the colliding ID.
SeeRunner ==
    /\ ~done
    /\ ~haveRunner
    /\ haveRunner' = TRUE
    /\ UNCHANGED <<haveAPI, kept, done>>

\* Correct keep: if both present, runner wins; else keep the one seen.
\* Guard uses IF (not nested \/) so specgen preserves precedence in Go.
DedupChoose ==
    /\ ~done
    /\ IF haveAPI THEN TRUE ELSE haveRunner
    /\ done' = TRUE
    /\ kept' = IF haveAPI /\ haveRunner THEN "runner"
               ELSE IF haveRunner THEN "runner"
               ELSE "api"
    /\ UNCHANGED <<haveAPI, haveRunner>>

\* Mutation: when both present, keep api (wrong — first/API wins).
DedupBug ==
    /\ Bug
    /\ ~done
    /\ haveAPI
    /\ haveRunner
    /\ done' = TRUE
    /\ kept' = "api"
    /\ UNCHANGED <<haveAPI, haveRunner>>

Terminating ==
    /\ done
    /\ UNCHANGED vars

Next == SeeAPI \/ SeeRunner \/ DedupChoose \/ DedupBug \/ Terminating

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* [code-documented] Unambiguous {1 api, 1 runner} group → runner wins.
\* dedup.go:46-48; TestRunnerSpanDedup.
Inv_RunnerWins ==
    (done /\ haveAPI /\ haveRunner) => kept = "runner"

\* BAIT: claims runner is never kept. SeeRunner+DedupChoose keeps it —
\* MUST FAIL.
BaitNeverRunner == kept /= "runner"

=============================================================================
