----------------------------- MODULE Decision -----------------------------
(***************************************************************************)
(* Logical name: TimingClampDecision                                       *)
(* File is Decision.tla so SANY module name matches the path.              *)
(***************************************************************************)
(* Decision core for ONE application of clampSpanToParent.                 *)
(*                                                                         *)
(* Full design model: specs/timing-clamp/TimingClamp.tla (pipeline).       *)
(* This core pins the pure clamp decision so it cannot drift from the      *)
(* generated Go module (pkg/analyzer/timingclampspec).                     *)
(*                                                                         *)
(* Mirrors clampSpanToParent in pkg/analyzer/analyzer.go:826:              *)
(*   pe  = parentEnd if parentEnd > parentStart else parentStart + 1       *)
(*   cs  = clamp start into [parentStart, pe-1]                            *)
(*   ce0 = min(end, pe)                                                    *)
(*   ce  = max(ce0, cs+1)   — never zero-width                             *)
(*                                                                         *)
(* Specgen subset only: scalars, IF/THEN/ELSE, LET, named actions.         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
  Tmax,   \* max modeled time (times live in 0..Tmax; 0 = missing ok)
  Bug     \* TRUE = passthrough without clamp (pre-fix / mutation)

VARIABLES
  phase,         \* "init" | "clamped" | "done"
  start,         \* raw child start
  end,           \* raw child end
  parentStart,   \* parent window start
  parentEnd,     \* parent window end
  outStart,      \* clamped child start
  outEnd         \* clamped child end

\* Init: one concrete interesting case - child end past parent end.
\* (start=2, end=4 into parent [1,3]; faithful clamp -> [2,3])

Init ==
  /\ phase = "init"
  /\ start = 2
  /\ end = 4
  /\ parentStart = 1
  /\ parentEnd = 3
  /\ outStart = 0
  /\ outEnd = 0

\* Hostile rewrite of open inputs (still in init). Gives TLC a second
\* case: start before parent, end at Tmax. Under Bug this fails
\* ClampedContained; under faithful DoClamp still contains.

SetHostile ==
  /\ phase = "init"
  /\ start' = 0
  /\ end' = Tmax
  /\ parentStart' = 2
  /\ parentEnd' = 3
  /\ UNCHANGED <<phase, outStart, outEnd>>

\* Faithful clamp - mirrors analyzer.go:826 / ClampSpan in TimingClamp.tla.
\* Only enabled when Bug = FALSE.

DoClamp ==
  /\ phase = "init"
  /\ ~Bug
  /\ phase' = "clamped"
  /\ outStart' =
       LET pe == IF parentEnd <= parentStart THEN parentStart + 1 ELSE parentEnd
           cs == IF start < parentStart THEN parentStart
                 ELSE IF start > pe - 1 THEN pe - 1
                 ELSE start
       IN cs
  /\ outEnd' =
       LET pe == IF parentEnd <= parentStart THEN parentStart + 1 ELSE parentEnd
           cs == IF start < parentStart THEN parentStart
                 ELSE IF start > pe - 1 THEN pe - 1
                 ELSE start
           ce0 == IF end > pe THEN pe ELSE end
       IN IF ce0 < cs + 1 THEN cs + 1 ELSE ce0
  /\ UNCHANGED <<start, end, parentStart, parentEnd>>

\* Mutation path: raw passthrough (no parent clamp). Only when Bug = TRUE.
\* Restores pre-fix "job clamp disabled" behavior (MCMutation JobClampedToRun).

BugPassthrough ==
  /\ phase = "init"
  /\ Bug
  /\ phase' = "clamped"
  /\ outStart' = start
  /\ outEnd' = end
  /\ UNCHANGED <<start, end, parentStart, parentEnd>>

\* Terminal step so bait can see a full correct behavior.

Finish ==
  /\ phase = "clamped"
  /\ phase' = "done"
  /\ UNCHANGED <<start, end, parentStart, parentEnd, outStart, outEnd>>

Next == SetHostile \/ DoClamp \/ BugPassthrough \/ Finish

\* TypeOK: outputs may be Tmax+1 when a degenerate parent at Tmax is given
\* room (parentStart=Tmax -> pe=Tmax+1 -> outEnd=Tmax+1).

TypeOK ==
  /\ phase \in {"init", "clamped", "done"}
  /\ start \in 0..Tmax
  /\ end \in 0..Tmax
  /\ parentStart \in 0..Tmax
  /\ parentEnd \in 0..Tmax
  /\ outStart \in 0..(Tmax + 1)
  /\ outEnd \in 0..(Tmax + 1)

\* After clamp: non-zero width (outStart < outEnd; code guarantees +1).
ClampedOrdered ==
  phase \in {"clamped", "done"} => outStart < outEnd

\* After clamp: child stays inside parent when the parent window is valid.
\* Degenerate parent (parentEnd <= parentStart) only requires the lower
\* bound - the helper invents pe = parentStart+1 which can exceed parentEnd.
ClampedContained ==
  phase \in {"clamped", "done"} =>
    /\ outStart >= parentStart
    /\ (parentEnd > parentStart =>
          /\ outStart <= parentEnd - 1
          /\ outEnd <= parentEnd)

\* Bait: "never finishes" - MUST FAIL (proves exploration / non-vacuity).
BaitNeverDone == phase /= "done"

=============================================================================
)
