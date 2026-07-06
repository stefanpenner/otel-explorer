--------------------------- MODULE TimingClamp ---------------------------
(***************************************************************************)
(* WHAT THIS SPEC IS FOR                                                   *)
(*                                                                         *)
(* Model the run/job/step time-clamping pipeline in                        *)
(* pkg/analyzer/analyzer.go (bounds :341-357, extend :374-383,             *)
(* sanity :385-404, shared clamp helper clampSpanToParent :826, job clamp  *)
(* call :884, step clamp call :1120) and the perfetto export clamp         *)
(* (pkg/perfetto/perfetto.go:44-54), and check whether ANY reachable       *)
(* hostile input (missing, inverted, future-dated, child-escaping-parent   *)
(* timestamps; clock skew via a free `now`) makes the pipeline emit a span *)
(* that violates containment:                                              *)
(*   every emitted span has start <= end, and child within parent bounds   *)
(* (code-documented contract: commit 9ad24f9 "Clamp child span times to    *)
(* parent bounds in flamechart" + the clampSpanToParent contract at        *)
(* analyzer.go:820-825, and the perfetto endNs<startNs / 1ms-min clamp).    *)
(*                                                                         *)
(* STATUS 2026-07-06: models the FIXED code. Findings #8 (shared           *)
(* clampSpanToParent applied on BOTH the OTel and ms/flamechart paths) and *)
(* #9 (24h sanity recalc excludes job ends past the threshold, caps at the *)
(* threshold when no valid end remains) are APPLIED. Under the faithful    *)
(* fixed model (all Bug* flags FALSE, MC.cfg) all five finding invariants  *)
(* are GREEN. Each finding keeps a mutation knob (Bug* constant) that       *)
(* restores its specific pre-fix behavior and turns its invariant RED       *)
(* (MCMutation1..5.cfg).                                                    *)
(*                                                                         *)
(* ONE CORRECT BEHAVIOR (informal sketch, fixes the atomicity grain):      *)
(*   1. bounds:  runStart=CreatedAt=1, runEnd=UpdatedAt=3                  *)
(*   2. extend:  completed job CompletedAt=4 > 3, so runEnd:=4             *)
(*   3. sanity:  run completed, 4-1 <= Threshold, no recalc                *)
(*   4. jobs:    clampSpanToParent([2,4],[1,4]) -> [2,4]; both paths [2,4]  *)
(*   5. steps:   clampSpanToParent([2,3],[2,4]) -> [2,3]; both paths [2,3]  *)
(*   6. export:  perfetto passes all spans through unchanged (each already  *)
(*               a >=1 sliver inside its parent).                          *)
(*                                                                         *)
(* ATOMICITY GRAIN: each pipeline phase is ONE atomic action (the real     *)
(* code runs single-threaded over an immutable fetched snapshot, so the    *)
(* per-job / per-step loops commute and are coarsened into their phase).   *)
(* All environment nondeterminism (hostile raw timestamps, run/job status, *)
(* clock skew) is chosen in Init: the pipeline is deterministic in its     *)
(* input, so TLC explores the input space, not exotic interleavings.       *)
(*                                                                         *)
(* DATA MODEL: timestamps ARE the bug target, so times live in a tiny int  *)
(* domain 0..Tmax with 0 = missing/zero-value. `now` is a free variable    *)
(* anywhere in 1..Tmax (clock skew both ways). The 24h sanity threshold    *)
(* is the constant Threshold (real code: analyzer.go:385).                 *)
(*                                                                         *)
(* FAULTS MODELED (as Init nondeterminism = hostile input):                *)
(*   missing times (0), inverted times, future-dated times, child times    *)
(*   escaping parent, run/job status disagreeing with times, `now` before  *)
(*   or after every data time.                                             *)
(* FAULTS DROPPED: concurrent mutation of the snapshot (impossible: code   *)
(*   processes fetched data single-threaded); unparseable-but-present      *)
(*   strings (equivalent to missing: parse failure -> same skip/fallback   *)
(*   paths); the earliestTime/earliestNs normalization shift (subtracting  *)
(*   a common constant preserves ordering and containment).                *)
(*                                                                         *)
(* MUTATION KNOBS (each restores one pre-fix bug; all FALSE = fixed code):  *)
(*   Bug       TRUE: job phase does NO parent clamp at all (raw passthrough *)
(*             on both paths) -> JobClampedToRun RED (MCMutation.cfg).      *)
(*   BugClamp  TRUE: restore the pre-#8 clamp (end-only Min2, RAW ms start, *)
(*             +1 floors) -> Finding 1/2/3 invariants RED (MCMutation1/2/3).*)
(*   BugBounds TRUE: restore pre-fix bounds (only missing UpdatedAt healed, *)
(*             inverted UpdatedAt survives) -> RunBoundsOrdered RED (MCMut4)*)
(*   BugSanity TRUE: restore pre-#9 sanity recalc (no threshold filter, no  *)
(*             threshold cap) -> SanityHeals24h RED (MCMutation5).          *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    Tmax,       \* top of the time domain; times are 0..Tmax, 0 = missing
    Threshold,  \* models the 24h sanity bound (analyzer.go:385)
    Bug,        \* TRUE: skip the job-level parent clamp entirely
    BugClamp,   \* TRUE: restore the pre-#8 end-only clamp + raw ms start
    BugBounds,  \* TRUE: restore pre-fix bounds (inverted UpdatedAt survives)
    BugSanity   \* TRUE: restore pre-#9 sanity recalc (keeps poisoned ends)

ASSUME Tmax \in Nat /\ Tmax >= 3
ASSUME Threshold \in Nat /\ Threshold >= 1 /\ Threshold < Tmax
ASSUME Bug       \in BOOLEAN
ASSUME BugClamp  \in BOOLEAN
ASSUME BugBounds \in BOOLEAN
ASSUME BugSanity \in BOOLEAN

T  == 0..Tmax          \* raw time domain, 0 = missing
Tv == 1..Tmax          \* valid (present) times
TT == 0..(Tmax + 2)    \* computed times: "+1" bumps can overflow Tmax by 2

Min2(a, b) == IF a < b THEN a ELSE b
Max2(a, b) == IF a > b THEN a ELSE b
SetMax(S)  == CHOOSE x \in S : \A y \in S : y <= x

(* ClampSpan mirrors clampSpanToParent in pkg/analyzer/analyzer.go:826.     *)
(* Forces a child [start,end] into its parent's window: start lands in      *)
(* [pStart, pEnd-1] and end in [start+1, pEnd] — a >=1 sliver at the parent *)
(* edge, never escaping the parent, never zero-width. A degenerate parent   *)
(* (pEnd <= pStart) is first given 1 unit of room. Returns                  *)
(* <<clampedStart, clampedEnd>>.                                            *)
ClampSpan(start, end, pStart, pEnd) ==
    LET pe  == IF pEnd <= pStart THEN pStart + 1 ELSE pEnd
        cs  == IF start < pStart      THEN pStart
               ELSE IF start > pe - 1 THEN pe - 1
               ELSE start
        ce0 == IF end > pe THEN pe ELSE end
        ce  == IF ce0 < cs + 1 THEN cs + 1 ELSE ce0
    IN <<cs, ce>>

Phases == {"init", "bounds", "extended", "sane", "jobs", "steps", "done"}

VARIABLES
    \* ---- environment (fixed at Init; hostile input) ----
    now,        \* time.Now() as seen by processJob fallback (analyzer.go:875)
    rCreated,   \* run.CreatedAt   (always present: code returns early if not, :341-344)
    rUpdated,   \* run.UpdatedAt   (missing or <= start -> runStart+1, :352-357)
    rDone,      \* run.Status == "completed"
    jStart,     \* job1.StartedAt  (0 = missing -> job skipped, :867-869)
    jEnd,       \* job1.CompletedAt (0 = missing -> fallback now, :875-880)
    jDone,      \* job1.Status == "completed"
    sStart,     \* step.StartedAt  (0 = missing -> step skipped, :1101-1103,1110-1113)
    sEnd,       \* step.CompletedAt (0 = missing -> step skipped, :1101-1103,1114-1117)
    j2End,      \* job2.CompletedAt (job2 only feeds extend/sanity phases)
    j2Done,     \* job2.Status == "completed"
    \* ---- pipeline state ----
    pc,         \* current phase
    runS, runE, \* run bounds (runStartTs / runEndTs)
    jobEmit,    \* job1 emitted?
    jobS, jobE,         \* job1 OTel span (pre-export), analyzer.go:1092-1093
    jobMsS, jobMsE,     \* job1 trace event [normalizedJobStart, End], :1009-1015
    stepEmit,   \* step emitted?
    stepS, stepE,       \* step OTel span (pre-export), :1138-1139
    stepMsS, stepMsE,   \* step trace event [normalizedStepStart, End], :1173-1179
    \* ---- perfetto-exported spans (perfetto.go:44-54,147-149) ----
    xWfS, xWfE, xJobS, xJobE, xStepS, xStepE

envVars  == <<now, rCreated, rUpdated, rDone, jStart, jEnd, jDone,
              sStart, sEnd, j2End, j2Done>>
pipeVars == <<runS, runE, jobEmit, jobS, jobE, jobMsS, jobMsE,
              stepEmit, stepS, stepE, stepMsS, stepMsE>>
expVars  == <<xWfS, xWfE, xJobS, xJobE, xStepS, xStepE>>
vars     == <<envVars, pipeVars, expVars, pc>>

TypeInvariant ==
    /\ now \in Tv
    /\ rCreated \in Tv
    /\ rUpdated \in T
    /\ rDone \in BOOLEAN
    /\ jStart \in T /\ jEnd \in T /\ jDone \in BOOLEAN
    /\ sStart \in T /\ sEnd \in T
    /\ j2End \in T /\ j2Done \in BOOLEAN
    /\ pc \in Phases
    /\ runS \in TT /\ runE \in TT
    /\ jobEmit \in BOOLEAN /\ stepEmit \in BOOLEAN
    /\ jobS \in TT /\ jobE \in TT /\ jobMsS \in TT /\ jobMsE \in TT
    /\ stepS \in TT /\ stepE \in TT /\ stepMsS \in TT /\ stepMsE \in TT
    /\ xWfS \in TT /\ xWfE \in TT
    /\ xJobS \in TT /\ xJobE \in TT /\ xStepS \in TT /\ xStepE \in TT

Init ==
    /\ now \in Tv
    /\ rCreated \in Tv
    /\ rUpdated \in T
    /\ rDone \in BOOLEAN
    /\ jStart \in T /\ jEnd \in T /\ jDone \in BOOLEAN
    /\ sStart \in T /\ sEnd \in T
    /\ j2End \in T /\ j2Done \in BOOLEAN
    /\ pc = "init"
    /\ runS = 0 /\ runE = 0
    /\ jobEmit = FALSE /\ stepEmit = FALSE
    /\ jobS = 0 /\ jobE = 0 /\ jobMsS = 0 /\ jobMsE = 0
    /\ stepS = 0 /\ stepE = 0 /\ stepMsS = 0 /\ stepMsE = 0
    /\ xWfS = 0 /\ xWfE = 0
    /\ xJobS = 0 /\ xJobE = 0 /\ xStepS = 0 /\ xStepE = 0

-----------------------------------------------------------------------------
(* Phase 1 — init bounds (analyzer.go:341-357).                            *)
(* runStart := CreatedAt; runEnd := UpdatedAt, or runStart+1ms if UpdatedAt *)
(* is missing OR not after runStart (:353 `!ok || !runEnd.After(runStart)`).*)
(* FIX (Finding 4): the inverted-UpdatedAt case is now healed too, so run   *)
(* bounds are always ordered. BugBounds=TRUE restores the pre-fix version   *)
(* that only healed the missing case, letting an inverted UpdatedAt survive.*)
(* (RunAttempt>1 RunStartedAt override not modeled: single-attempt runs.)   *)
DoBounds ==
    /\ pc = "init"
    /\ pc' = "bounds"
    /\ runS' = rCreated
    /\ runE' = IF rUpdated = 0 THEN rCreated + 1
               ELSE IF ~BugBounds /\ rUpdated <= rCreated THEN rCreated + 1
               ELSE rUpdated
    /\ UNCHANGED <<envVars, expVars, jobEmit, jobS, jobE, jobMsS, jobMsE,
                   stepEmit, stepS, stepE, stepMsS, stepMsE>>

(* Phase 2 — extend runEnd from completed jobs (analyzer.go:374-383).      *)
(* Only jobs with Status=="completed" AND a parseable CompletedAt extend.  *)
DoExtend ==
    /\ pc = "bounds"
    /\ pc' = "extended"
    /\ LET e1 == IF jDone  /\ jEnd  /= 0 THEN {jEnd}  ELSE {}
           e2 == IF j2Done /\ j2End /= 0 THEN {j2End} ELSE {}
       IN runE' = SetMax({runE} \cup e1 \cup e2)
    /\ UNCHANGED <<envVars, expVars, runS, jobEmit, jobS, jobE, jobMsS,
                   jobMsE, stepEmit, stepS, stepE, stepMsS, stepMsE>>

(* Phase 3 — 24h sanity recalc (analyzer.go:385-404).                      *)
(* Only if run completed AND runEnd - runStart > Threshold.                *)
(* The recalc scans ALL jobs' CompletedAt (no Status filter, :391-397).    *)
(* FIX (Finding 9): the recalc now EXCLUDES job ends past the threshold     *)
(* (:393 `ts <= threshold`), so the very poisoned CompletedAt that tripped  *)
(* the anomaly can't re-poison runEnd; if no usable job end remains, runEnd *)
(* is capped at the threshold (:401-403). BugSanity=TRUE restores the       *)
(* pre-fix recalc (all ends counted, no cap) that reused poisoned data.     *)
DoSanity ==
    /\ pc = "extended"
    /\ pc' = "sane"
    /\ IF rDone /\ runE - runS > Threshold
       THEN LET thr      == runS + Threshold
                InWin(e) == IF BugSanity THEN e /= 0 ELSE (e /= 0 /\ e <= thr)
                e1 == IF InWin(jEnd)  THEN {jEnd}  ELSE {}
                e2 == IF InWin(j2End) THEN {j2End} ELSE {}
                maxJobEnd == SetMax({runS} \cup e1 \cup e2)
            IN runE' = IF maxJobEnd > runS THEN maxJobEnd
                       ELSE IF BugSanity THEN runE ELSE thr
       ELSE runE' = runE
    /\ UNCHANGED <<envVars, expVars, runS, jobEmit, jobS, jobE, jobMsS,
                   jobMsE, stepEmit, stepS, stepE, stepMsS, stepMsE>>

(* Phase 4 — clamp job (analyzer.go:867-885, spans :1009-1015,1092-1093).  *)
(* Missing StartedAt -> job skipped (:867-869).                            *)
(* Missing CompletedAt -> fallback time.Now() (:875-880).                  *)
(* FIX (Finding 8): clampSpanToParent([rawStart,rawEnd],[runS,runE]) (:884) *)
(* now drives BOTH the OTel span (jobStart/jobEnd, :1092-1093) AND the ms   *)
(* trace event (normalizedJobStart/End, :1009-1015) — identical clamped     *)
(* values, no more raw ms start, no +1 floor. The clamp leaves a >=1 sliver *)
(* inside the parent, so the perfetto 1ms minimum never re-escapes.         *)
(* BugClamp=TRUE restores the pre-fix clamp: end-only Min2 to runE on the   *)
(* OTel path, RAW start on the ms path, +1 floor (jobEndTs can exceed runE  *)
(* and jobMsS can precede runS). Bug=TRUE disables the parent clamp fully.  *)
DoJobs ==
    /\ pc = "sane"
    /\ pc' = "jobs"
    /\ IF jStart = 0
       THEN /\ jobEmit' = FALSE
            /\ UNCHANGED <<jobS, jobE, jobMsS, jobMsE>>
       ELSE LET rawEnd == IF jEnd = 0 THEN now ELSE jEnd
            IN IF Bug
               THEN /\ jobEmit' = TRUE                     \* clamp fully disabled
                    /\ jobS' = jStart /\ jobE' = rawEnd
                    /\ jobMsS' = jStart /\ jobMsE' = Max2(jStart + 1, rawEnd)
               ELSE IF BugClamp
                    THEN LET ce == Min2(rawEnd, runE)       \* pre-fix end-only clamp
                             cs == Min2(jStart, runE)
                         IN /\ jobEmit' = TRUE
                            /\ jobS' = cs /\ jobE' = ce
                            /\ jobMsS' = jStart              \* RAW ms start (the escape)
                            /\ jobMsE' = Max2(jStart + 1, ce)
                    ELSE LET c == ClampSpan(jStart, rawEnd, runS, runE)
                         IN /\ jobEmit' = TRUE              \* FIXED: both paths clamped
                            /\ jobS' = c[1] /\ jobE' = c[2]
                            /\ jobMsS' = c[1] /\ jobMsE' = c[2]
    /\ UNCHANGED <<envVars, expVars, runS, runE,
                   stepEmit, stepS, stepE, stepMsS, stepMsE>>

(* Phase 5 — clamp step (analyzer.go:1101-1123, spans :1138-1139,1173-1179)*)
(* Missing StartedAt or CompletedAt -> step skipped (:1101-1103).          *)
(* FIX (Finding 8): the SAME clampSpanToParent (:1120) clamps the step into *)
(* the parent job's clamped window [jobS,jobE] (== [jobMsS,jobMsE] now) and *)
(* drives BOTH the OTel span (:1138-1139) and the ms event (:1173-1179).    *)
(* BugClamp=TRUE restores the pre-fix step clamp: end-only Min2 to the ms   *)
(* bound jobMsE, +1 floor re-applied after clamping (can exceed the bound). *)
DoSteps ==
    /\ pc = "jobs"
    /\ pc' = "steps"
    /\ IF ~jobEmit \/ sStart = 0 \/ sEnd = 0
       THEN /\ stepEmit' = FALSE
            /\ UNCHANGED <<stepS, stepE, stepMsS, stepMsE>>
       ELSE IF BugClamp
            THEN LET bound == jobMsE                        \* pre-fix: clamp to +1'd bound
                     ce  == Min2(sEnd, bound)
                     cs  == Min2(sStart, bound)
                     se1 == Max2(cs + 1, ce)
                     se2 == IF se1 > bound THEN Max2(cs + 1, bound) ELSE se1
                 IN /\ stepEmit' = TRUE
                    /\ stepS' = cs /\ stepE' = ce
                    /\ stepMsS' = cs /\ stepMsE' = se2
            ELSE LET c == ClampSpan(sStart, sEnd, jobS, jobE)
                 IN /\ stepEmit' = TRUE                     \* FIXED: both paths clamped
                    /\ stepS' = c[1] /\ stepE' = c[2]
                    /\ stepMsS' = c[1] /\ stepMsE' = c[2]
    /\ UNCHANGED <<envVars, expVars, runS, runE,
                   jobEmit, jobS, jobE, jobMsS, jobMsE>>

(* Phase 6 — perfetto export clamp (perfetto.go:44-54).                    *)
(* Per span: endNs < startNs -> endNs := startNs (:48-49); non-marker       *)
(* zero-duration -> endNs := startNs + 1ms (:51-52). All modeled spans are  *)
(* non-markers. There is NO parent-bound recheck after the bump.            *)
(* (earliestNs shift dropped: subtracting a common constant preserves       *)
(* containment; the startNs<0 clamp cannot fire when earliest is the min.)  *)
ExpS(s, e) == s
ExpE(s, e) == LET e1 == Max2(s, e) IN IF e1 <= s THEN s + 1 ELSE e1

DoExport ==
    /\ pc = "steps"
    /\ pc' = "done"
    /\ xWfS' = ExpS(runS, runE) /\ xWfE' = ExpE(runS, runE)
    /\ IF jobEmit
       THEN /\ xJobS' = ExpS(jobS, jobE) /\ xJobE' = ExpE(jobS, jobE)
       ELSE UNCHANGED <<xJobS, xJobE>>
    /\ IF stepEmit
       THEN /\ xStepS' = ExpS(stepS, stepE) /\ xStepE' = ExpE(stepS, stepE)
       ELSE UNCHANGED <<xStepS, xStepE>>
    /\ UNCHANGED <<envVars, pipeVars>>

Next == DoBounds \/ DoExtend \/ DoSanity \/ DoJobs \/ DoSteps \/ DoExport

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* PROPERTIES                                                              *)

Done == pc = "done"

(* ---- code-documented invariants (GREEN in MC) ---- *)

(* perfetto.go:48-52: exported spans are never inverted; non-markers have   *)
(* strictly positive duration. Ms-path events likewise ordered by           *)
(* construction (ClampSpan guarantees end >= start + 1).                    *)
ExportedSpansOrdered ==
    Done =>
        /\ xWfS < xWfE
        /\ jobEmit => (xJobS < xJobE /\ jobMsS < jobMsE)
        /\ stepEmit => (xStepS < xStepE /\ stepMsS < stepMsE)

(* analyzer.go:820-825 clampSpanToParent contract: the OTel job span        *)
(* (pre-export) stays at or below runEnd. Bug=TRUE (job clamp disabled)     *)
(* breaks this (MCMutation.cfg).                                            *)
JobClampedToRun ==
    (Done /\ jobEmit) => (jobS <= runE /\ jobE <= runE)

(* Finding 1 — commit 9ad24f9 contract, upper bound, as actually EMITTED    *)
(* (perfetto): child span end never exceeds parent span end.                *)
(* GREEN under the fix: ClampSpan leaves a >=1 sliver, so the child end is  *)
(* strictly inside the parent and the perfetto 1ms minimum never bumps it   *)
(* past. RED under BugClamp (MCMutation1): the pre-fix clamp put the child  *)
(* AT the parent end, and the perfetto 1ms bump then pushed it one past.    *)
ExportedChildEndInParent ==
    Done =>
        /\ jobEmit => xJobE <= xWfE
        /\ stepEmit => xStepE <= xJobE

(* Finding 2 — PROPOSED, not code-documented: inferred from commit 9ad24f9  *)
(* title/intent; the commit body only claims end-time clamping. Lower       *)
(* bound: child span never starts before its parent. GREEN under the fix:   *)
(* ClampSpan clamps start up to parentStart. RED under BugClamp             *)
(* (MCMutation2): the pre-fix clamp only bounded the end side.              *)
ExportedChildStartInParent ==
    Done =>
        /\ jobEmit => xJobS >= xWfS
        /\ stepEmit => xStepS >= xJobS

(* Finding 3 — ms/flamechart-JSON path containment. GREEN under the fix:    *)
(* the ms path now uses the SAME clamped values as the OTel path            *)
(* (jobMsS=jobS, jobMsE=jobE, stepMs=step). RED under BugClamp              *)
(* (MCMutation3): the pre-fix ms path used the RAW job start and a +1 floor *)
(* end that escaped runEnd unboundedly.                                     *)
MsChildWithinParent ==
    Done =>
        /\ jobEmit => (jobMsE <= runE /\ jobMsS >= runS)
        /\ stepEmit => (stepMsE <= jobMsE /\ stepMsS >= jobMsS)

(* ---- proposed invariants (spec-mined facts, not code-documented) ---- *)

(* Answers "can the sanity recalc pull runEnd below a completed job end     *)
(* that is ITSELF within the 24h threshold?" — NO. The recalc (:391-397)    *)
(* scans all jobs and keeps the max end <= threshold, a superset of any     *)
(* threshold-valid completed-job end.                                       *)
(* NOTE: the pre-#9 form (no `<= threshold` qualifier) is INTENTIONALLY     *)
(* false under the fix — the #9 fix deliberately drops a completed job's    *)
(* own poisoned (>threshold) CompletedAt. That dropping IS Finding 9's fix, *)
(* so the invariant is qualified to threshold-valid ends, not weakened away.*)
SanityNeverBelowValidCompletedJobEnd ==
    pc \in {"sane", "jobs", "steps", "done"} =>
        /\ (jDone  /\ jEnd  /= 0 /\ jEnd  <= runS + Threshold) => runE >= jEnd
        /\ (j2Done /\ j2End /= 0 /\ j2End <= runS + Threshold) => runE >= j2End

(* analyzer.go step-skip guard is dead code: jobMsE >= jobMsS + 1 always    *)
(* (ClampSpan / the +1 floor guarantee it).                                 *)
JobSkipIsDeadCode ==
    (Done /\ jobEmit) => jobMsE >= jobMsS + 1

(* Likewise for steps: stepMsE >= stepMsS + 1 always.                       *)
StepSkipIsDeadCode ==
    (Done /\ stepEmit) => stepMsE >= stepMsS + 1

(* Finding 4 — run bounds ordered after all phases. GREEN under the fix:    *)
(* bounds heals both missing AND inverted UpdatedAt (:352-357). RED under   *)
(* BugBounds (MCMutation4): pre-fix bounds left an inverted UpdatedAt as a   *)
(* runEnd below runStart when no completed job extended it.                  *)
RunBoundsOrdered == Done => runS <= runE

(* Finding 5 — intent of the 24h sanity check (comment :386-389). GREEN     *)
(* under the fix: the recalc excludes >threshold ends and caps at the       *)
(* threshold, so a completed run never keeps a >24h span. RED under         *)
(* BugSanity (MCMutation5): the pre-fix recalc recomputed the SAME poisoned *)
(* value (max over ALL jobs' CompletedAt).                                  *)
SanityHeals24h == Done => (rDone => runE - runS <= Threshold)

(* ---- bait invariant (MCBait.cfg): "no step is ever emitted".            *)
(* Clearly false — MUST FAIL, and the witness trace is one full correct    *)
(* behavior of the pipeline (sanity check that the model isn't vacuous).   *)
BaitNoStepEmitted == Done => ~stepEmit

=============================================================================
