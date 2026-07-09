--------------------------- MODULE GhaLifecycle ---------------------------
(***************************************************************************)
(* WHAT THIS SPEC IS FOR                                                   *)
(*                                                                         *)
(* Models the GitHub Actions run/job lifecycle as an ENVIRONMENT and the   *)
(* otel-explorer analyzer (pkg/analyzer/analyzer.go, processWorkflowRun /  *)
(* processPreviousAttempt / processJob) as a PROCESSOR, to bug-hunt:       *)
(*   - metric derivation (Pending / Failed / Successful, queue time)      *)
(*   - retry-attempt sequencing and the emitted attempt-span chain        *)
(* under GitHub's real generosity: empty conclusion on completed (fault), *)
(* jobs still advancing after run completion (cancellation stragglers),   *)
(* re-runs bumping RunAttempt, and previous-attempt fetches failing       *)
(* silently (analyzer.go:333 `continue`).                                  *)
(*                                                                         *)
(* Steps are abstracted away: a step has the same status/conclusion pair  *)
(* and the same processing shape as a job; the job level carries all the  *)
(* modeled logic.  "neutral" conclusion is dropped: it lands in the same  *)
(* equivalence class as "success" in every modeled branch.  Timestamps    *)
(* are abstracted to booleans (started / hasCompletedAt); ordering of     *)
(* time values is not a bug target here.                                  *)
(*                                                                         *)
(* ONE CORRECT BEHAVIOR (fixes the atomicity grain):                       *)
(*  1. GH: run queued -> in_progress; attempt-1 job starts, completes     *)
(*     (failure, CompletedAt set); run completes (failure).               *)
(*  2. GH: re-run -> RunAttempt=2, run queued -> in_progress; attempt-2   *)
(*     job starts, completes (success); run completes (success).          *)
(*  3. Analyzer: fetch run snapshot (attempt 2, completed/success) and    *)
(*     count the run successful  [one atomic action - counting is a pure  *)
(*     function of the snapshot, analyzer.go:295-305].                    *)
(*  4. Analyzer: fetch latest jobs (analyzer.go:316).                     *)
(*  5. Analyzer: fetch attempt-1 jobs AND process them: emit attempt-1    *)
(*     span (conclusion derived from jobs), count job metrics             *)
(*     [one atomic action per attempt = the loop body 329-341].           *)
(*  6. Analyzer: process latest attempt: count jobs, emit attempt-2 span  *)
(*     with retry link -> attempt-1 (analyzer.go:511-514, 628-659). Done. *)
(*                                                                         *)
(* GRAIN: each HTTP fetch is one action; the pure in-memory processing    *)
(* that consumes a fetch is folded into that fetch action (it reads only  *)
(* the snapshot, so it commutes with every environment action).           *)
(*                                                                         *)
(* 2026-07-06: findings 1-4 of the campaign are FIXED in code; the model   *)
(* now reflects the fixed code. Each old buggy behavior stays reachable    *)
(* behind its own Bug* flag so the MCMutation* configs keep proving the   *)
(* invariants have teeth:                                                  *)
(*   Bug                  - drops the `!isPending` guard on failed-job     *)
(*                          counting (analyzer.go:903)                     *)
(*   BugPendingRunFailed  - pending runs counted failed (old :296-300)     *)
(*   BugDanglingLink      - retry links added unconditionally (old         *)
(*                          :609-623 / :732-746)                           *)
(*   BugQueueUngated      - queue time counted for non-completed jobs      *)
(*                          (old :852/:890)                                *)
(*   BugTimedOutSuccess   - DerivedConcl ignores timed_out (old :697-701)  *)
(*   BugUnstartedInvisible- never-started jobs invisible to PendingJobs    *)
(*                          (old early return before classification)       *)
(*   BugNoConclFallback   - completed run w/ empty conclusion emitted      *)
(*                          verbatim (old :638, no jobs-derived fallback)  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Bug,                  \* TRUE disables the isPending guard at analyzer.go:903
  BugPendingRunFailed,  \* TRUE reverts run classification to two-way
  BugDanglingLink,      \* TRUE adds retry links unconditionally
  BugQueueUngated,      \* TRUE drops the !isPending gate on queue time
  BugTimedOutSuccess,   \* TRUE makes DerivedConcl ignore timed_out
  BugUnstartedInvisible,\* TRUE hides never-started jobs from PendingJobs
  BugNoConclFallback,   \* TRUE emits empty run conclusion verbatim
  MaxAttempt,   \* max RunAttempt (re-runs bounded)
  JobIds        \* model values: the (fixed) set of jobs per attempt

ASSUME Bug \in BOOLEAN
ASSUME BugPendingRunFailed \in BOOLEAN
ASSUME BugDanglingLink \in BOOLEAN
ASSUME BugQueueUngated \in BOOLEAN
ASSUME BugTimedOutSuccess \in BOOLEAN
ASSUME BugUnstartedInvisible \in BOOLEAN
ASSUME BugNoConclFallback \in BOOLEAN
ASSUME MaxAttempt \in Nat \ {0}

Statuses    == {"queued", "in_progress", "completed"}
Conclusions == {"success", "failure", "timed_out", "cancelled", "skipped"}
\* "" = empty conclusion (the completed-with-no-conclusion fault)
ConclOrEmpty == Conclusions \cup {""}

Attempts == 1..MaxAttempt

JobRec == [status: Statuses, concl: ConclOrEmpty,
           started: BOOLEAN, hasCompletedAt: BOOLEAN]

InitJob == [status |-> "queued", concl |-> "",
            started |-> FALSE, hasCompletedAt |-> FALSE]

NoRun == [status |-> "none", concl |-> "", attempt |-> 0]

VARIABLES
  \* ---- environment (GitHub) ----
  ghRunStatus,   \* run status
  ghRunConcl,    \* run conclusion ("" until completed; may stay "" = fault)
  ghRunAttempt,  \* current RunAttempt
  ghJobs,        \* [Attempts -> [JobIds -> JobRec]]
  \* ---- analyzer ----
  aPhase,        \* "idle" -> "fetched" -> "prev" -> "done"
  aRun,          \* run snapshot (analyzer works from this, not live state)
  aJobs,         \* [Attempts -> [st: notFetched|failed|ok, jobs: snapshot]]
  aPrevIdx,      \* next previous attempt to fetch (loop var, analyzer.go:320)
  emitted,       \* set of attempts whose workflow span was emitted
  links,         \* set of <<from, to>> retry links between attempt spans
  attemptConcl,  \* [Attempts -> conclusion put on the emitted attempt span]
  countedPending,\* set of <<attempt, job>> counted into PendingJobs
  countedFailed, \* set of <<attempt, job>> counted into FailedJobs
  countedQueue,  \* set of <<attempt, job>> whose queue time was counted
  runCounted     \* "none" | "success" | "failed"  (analyzer.go:296-300)

ghVars == <<ghRunStatus, ghRunConcl, ghRunAttempt, ghJobs>>
aVars  == <<aPhase, aRun, aJobs, aPrevIdx, emitted, links, attemptConcl,
            countedPending, countedFailed, countedQueue, runCounted>>
vars   == <<ghVars, aVars>>

Symm == Permutations(JobIds)

-----------------------------------------------------------------------------
TypeInvariant ==
  /\ ghRunStatus \in Statuses
  /\ ghRunConcl \in ConclOrEmpty
  /\ ghRunAttempt \in Attempts
  /\ ghJobs \in [Attempts -> [JobIds -> JobRec]]
  /\ aPhase \in {"idle", "fetched", "prev", "done"}
  /\ aRun \in [status: Statuses \cup {"none"}, concl: ConclOrEmpty,
               attempt: Attempts \cup {0}]
  /\ aJobs \in [Attempts -> [st: {"notFetched", "failed", "ok"},
                             jobs: [JobIds -> JobRec]]]
  /\ aPrevIdx \in Attempts
  /\ emitted \subseteq Attempts
  /\ links \subseteq Attempts \X Attempts
  /\ attemptConcl \in [Attempts -> ConclOrEmpty \cup {"none"}]
  /\ countedPending \subseteq Attempts \X JobIds
  /\ countedFailed \subseteq Attempts \X JobIds
  /\ countedQueue \subseteq Attempts \X JobIds
  /\ runCounted \in {"none", "pending", "success", "failed"}

Init ==
  /\ ghRunStatus = "queued"
  /\ ghRunConcl = ""
  /\ ghRunAttempt = 1
  /\ ghJobs = [a \in Attempts |-> [j \in JobIds |-> InitJob]]
  /\ aPhase = "idle"
  /\ aRun = NoRun
  /\ aJobs = [a \in Attempts |->
                [st |-> "notFetched", jobs |-> [j \in JobIds |-> InitJob]]]
  /\ aPrevIdx = 1
  /\ emitted = {}
  /\ links = {}
  /\ attemptConcl = [a \in Attempts |-> "none"]
  /\ countedPending = {}
  /\ countedFailed = {}
  /\ countedQueue = {}
  /\ runCounted = "none"

-----------------------------------------------------------------------------
(* ------------------------- ENVIRONMENT: GitHub ------------------------- *)
(* All environment actions carry `aPhase /= "done"`: once the analyzer has *)
(* finished, no property reads live GitHub state, so further environment   *)
(* steps only multiply property-irrelevant states.  Sound reduction.       *)

EnvLive == aPhase /= "done"

GhRunStart ==
  /\ EnvLive
  /\ ghRunStatus = "queued"
  /\ ghRunStatus' = "in_progress"
  /\ UNCHANGED <<ghRunConcl, ghRunAttempt, ghJobs>>
  /\ UNCHANGED aVars

\* Run may complete straight from queued (cancelled while queued).
\* Conclusion is nondeterministic; "" on completed is the modeled fault.
GhRunComplete ==
  /\ EnvLive
  /\ ghRunStatus \in {"queued", "in_progress"}
  /\ ghRunStatus' = "completed"
  /\ \E c \in ConclOrEmpty : ghRunConcl' = c
  /\ UNCHANGED <<ghRunAttempt, ghJobs>>
  /\ UNCHANGED aVars

\* Jobs of ANY attempt <= current may advance, even after the run completed
\* (cancellation stragglers: run completed while a job is still in_progress).
GhJobStart(a, j) ==
  /\ EnvLive
  /\ a <= ghRunAttempt
  /\ ghJobs[a][j].status = "queued"
  /\ ghJobs' = [ghJobs EXCEPT ![a][j] =
                  [@ EXCEPT !.status = "in_progress", !.started = TRUE]]
  /\ UNCHANGED <<ghRunStatus, ghRunConcl, ghRunAttempt>>
  /\ UNCHANGED aVars

\* A job may complete from queued (skipped-style, never started) or from
\* in_progress.  Faults: c = "" (empty conclusion on completed) and
\* hc = FALSE (completed_at missing on a completed job).
GhJobComplete(a, j) ==
  /\ EnvLive
  /\ a <= ghRunAttempt
  /\ ghJobs[a][j].status /= "completed"
  /\ \E c \in ConclOrEmpty, hc \in BOOLEAN :
       ghJobs' = [ghJobs EXCEPT ![a][j] =
                    [@ EXCEPT !.status = "completed",
                              !.concl = c,
                              !.hasCompletedAt = hc]]
  /\ UNCHANGED <<ghRunStatus, ghRunConcl, ghRunAttempt>>
  /\ UNCHANGED aVars

\* Re-run bumps RunAttempt; run restarts; old attempts' jobs are left as-is
\* (they may even still advance - see GhJobStart/GhJobComplete guards).
GhReRun ==
  /\ EnvLive
  /\ ghRunStatus = "completed"
  /\ ghRunAttempt < MaxAttempt
  /\ ghRunAttempt' = ghRunAttempt + 1
  /\ ghRunStatus' = "queued"
  /\ ghRunConcl' = ""
  /\ UNCHANGED ghJobs
  /\ UNCHANGED aVars

-----------------------------------------------------------------------------
(* --------------------------- ANALYZER helpers -------------------------- *)

\* analyzer.go:816-818  isJobPending: status != completed || no completed_at
IsPending(jr) == jr.status /= "completed" \/ ~jr.hasCompletedAt

\* analyzer.go:903  if !isPending && (concl == failure || concl == timed_out)
\* Bug=TRUE drops the isPending guard (the mutation).
CountsFailed(jr) ==
  /\ (Bug \/ ~IsPending(jr))
  /\ jr.concl \in {"failure", "timed_out"}

\* analyzer.go:924-926  countsQueueTime: !isPending && conclusion gate.
\* BugQueueUngated=TRUE restores the old conclusion-only gate (Finding 3).
CountsQueue(jr) ==
  /\ (BugQueueUngated \/ ~IsPending(jr))
  /\ jr.concl \notin {"skipped", "cancelled"}

\* analyzer.go:867  jobs with StartedAt == "" have no timing to process
Started(jr) == jr.started

\* analyzer.go:796-811 conclusionFromJobs: failure > timed_out > cancelled
\* > success.  BugTimedOutSuccess=TRUE restores the old derivation that
\* ignored timed_out (Finding 4).
DerivedConcl(S) ==
  IF \E j \in JobIds : S[j].concl = "failure" THEN "failure"
  ELSE IF /\ ~BugTimedOutSuccess
          /\ \E j \in JobIds : S[j].concl = "timed_out" THEN "timed_out"
  ELSE IF \E j \in JobIds : S[j].concl = "cancelled" THEN "cancelled"
  ELSE "success"

\* analyzer.go:853-864  pending classification runs BEFORE the never-started
\* early return, so unstarted (still queued) jobs are visible as pending.
\* BugUnstartedInvisible=TRUE restores the old order (discovery note).
PendingOf(a, S) ==
  {<<a, j>> : j \in {j \in JobIds :
      IsPending(S[j]) /\ (Started(S[j]) \/ ~BugUnstartedInvisible)}}
FailedOf(a, S) ==
  {<<a, j>> : j \in {j \in JobIds : Started(S[j]) /\ CountsFailed(S[j])}}
QueueOf(a, S) ==
  {<<a, j>> : j \in {j \in JobIds : Started(S[j]) /\ CountsQueue(S[j])}}

(* --------------------------- ANALYZER actions -------------------------- *)

\* Fetch the run and immediately classify it (analyzer.go:295-305).
\* Three-way: pending (not completed) / success / failed.  Completed runs
\* with empty conclusion still count failed.  BugPendingRunFailed=TRUE
\* restores the old two-way classification (Finding 2).
AFetchRun ==
  /\ aPhase = "idle"
  /\ aRun' = [status |-> ghRunStatus, concl |-> ghRunConcl,
              attempt |-> ghRunAttempt]
  /\ runCounted' =
       IF ghRunStatus /= "completed"
         THEN IF BugPendingRunFailed THEN "failed" ELSE "pending"
         ELSE IF ghRunConcl = "success" THEN "success" ELSE "failed"
  /\ aPhase' = "fetched"
  /\ UNCHANGED ghVars
  /\ UNCHANGED <<aJobs, aPrevIdx, emitted, links, attemptConcl,
                 countedPending, countedFailed, countedQueue>>

\* Fetch /jobs (analyzer.go:316).  The endpoint returns the LATEST attempt's
\* jobs at fetch time; if a re-run raced in between, that can be a newer
\* attempt than the run snapshot says - the analyzer still files them under
\* its snapshot attempt.  Modeled faithfully; no property asserted about it.
AFetchJobsLatest ==
  /\ aPhase = "fetched"
  /\ aJobs' = [aJobs EXCEPT ![aRun.attempt] =
                 [st |-> "ok", jobs |-> ghJobs[ghRunAttempt]]]
  /\ aPhase' = "prev"
  /\ UNCHANGED ghVars
  /\ UNCHANGED <<aRun, aPrevIdx, emitted, links, attemptConcl,
                 countedPending, countedFailed, countedQueue, runCounted>>

\* One iteration of the previous-attempts loop, fetch succeeded
\* (analyzer.go:327-341 + processPreviousAttempt 697-780).
\* Emits the attempt span, links it to attempt-1 ONLY when attempt-1's span
\* was emitted (linkPrev, :750), and counts job metrics via processJob.
\* BugDanglingLink=TRUE restores the unconditional link (Finding 1).
AFetchPrevOk ==
  /\ aPhase = "prev"
  /\ aPrevIdx < aRun.attempt
  /\ LET a == aPrevIdx
         S == ghJobs[a]
     IN /\ aJobs' = [aJobs EXCEPT ![a] = [st |-> "ok", jobs |-> S]]
        /\ emitted' = emitted \cup {a}
        /\ attemptConcl' = [attemptConcl EXCEPT ![a] = DerivedConcl(S)]
        /\ links' = IF a > 1 /\ (BugDanglingLink \/ (a - 1) \in emitted)
                      THEN links \cup {<<a, a - 1>>} ELSE links
        /\ countedPending' = countedPending \cup PendingOf(a, S)
        /\ countedFailed' = countedFailed \cup FailedOf(a, S)
        /\ countedQueue' = countedQueue \cup QueueOf(a, S)
        /\ aPrevIdx' = a + 1
  /\ UNCHANGED ghVars
  /\ UNCHANGED <<aPhase, aRun, runCounted>>

\* One iteration of the previous-attempts loop, fetch FAILED:
\* analyzer.go:333 `continue` - the attempt is skipped silently.  No span,
\* no metrics, no error - and (post-fix) no link will target it.  (The
\* len(jobs)==0 early return at 698 is the same skip class, folded in.)
AFetchPrevFail ==
  /\ aPhase = "prev"
  /\ aPrevIdx < aRun.attempt
  /\ aJobs' = [aJobs EXCEPT ![aPrevIdx].st = "failed"]
  /\ aPrevIdx' = aPrevIdx + 1
  /\ UNCHANGED ghVars
  /\ UNCHANGED <<aPhase, aRun, emitted, links, attemptConcl,
                 countedPending, countedFailed, countedQueue, runCounted>>

\* Process the latest attempt from the snapshot taken at AFetchJobsLatest
\* (analyzer.go:511-514 job processing, 628-643 retry link, 650-659 span).
\* The latest span carries run.Conclusion, falling back to the jobs-derived
\* conclusion when a COMPLETED run reports an empty one (:542-548).
\* BugNoConclFallback=TRUE emits the empty conclusion verbatim (old code).
\* The retry link is gated on the previous attempt's span existing (:629);
\* BugDanglingLink=TRUE restores the unconditional link (Finding 1).
AProcessLatest ==
  /\ aPhase = "prev"
  /\ aPrevIdx = aRun.attempt
  /\ LET a == aRun.attempt
         S == aJobs[a].jobs
     IN /\ emitted' = emitted \cup {a}
        /\ attemptConcl' = [attemptConcl EXCEPT ![a] =
             IF /\ ~BugNoConclFallback
                /\ aRun.concl = ""
                /\ aRun.status = "completed"
               THEN DerivedConcl(S) ELSE aRun.concl]
        /\ links' = IF a > 1 /\ (BugDanglingLink \/ (a - 1) \in emitted)
                      THEN links \cup {<<a, a - 1>>} ELSE links
        /\ countedPending' = countedPending \cup PendingOf(a, S)
        /\ countedFailed' = countedFailed \cup FailedOf(a, S)
        /\ countedQueue' = countedQueue \cup QueueOf(a, S)
  /\ aPhase' = "done"
  /\ UNCHANGED ghVars
  /\ UNCHANGED <<aRun, aJobs, aPrevIdx, runCounted>>

-----------------------------------------------------------------------------
Next ==
  \/ GhRunStart
  \/ GhRunComplete
  \/ \E a \in Attempts, j \in JobIds : GhJobStart(a, j)
  \/ \E a \in Attempts, j \in JobIds : GhJobComplete(a, j)
  \/ GhReRun
  \/ AFetchRun
  \/ AFetchJobsLatest
  \/ AFetchPrevOk
  \/ AFetchPrevFail
  \/ AProcessLatest

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* ------------------------------ PROPERTIES ----------------------------- *)
(* Source labels:                                                          *)
(*   [code-documented] cited to a code comment/assertion/test              *)
(*   [proposed]        author-proposed; the user owns correctness props    *)

\* [code-documented] analyzer.go:903 "Only count genuine failures" - a job
\* counted pending must never also be counted failed.
InvPendingNeverFailed == countedPending \cap countedFailed = {}

\* [proposed] sanity: every emitted attempt span carries some conclusion.
InvEmittedHaveConcl == \A a \in emitted : attemptConcl[a] /= "none"

\* [proposed - discovery: "pending runs must NOT be counted as failed"]
\* A run counted failed should at least be completed.
\* Finding 2 - FIXED 2026-07-06 (analyzer.go:295-305 three-way classify);
\* fails again with BugPendingRunFailed=TRUE (MCMutation2).
InvPendingRunNotFailed == runCounted = "failed" => aRun.status = "completed"

\* [proposed] every retry link emitted must point at an attempt span that
\* was actually emitted - the attempt-span chain is contiguous.
\* Finding 1 - FIXED 2026-07-06 (emittedAttempts tracking analyzer.go:327,
\* link gates :629 / :750); fails with BugDanglingLink=TRUE (MCMutation1).
InvLinkTargetsEmitted ==
  aPhase = "done" => \A l \in links : l[2] \in emitted

\* [proposed - discovery hint] queue time should only be counted for jobs
\* that completed.  Finding 3 - FIXED 2026-07-06 (analyzer.go:924-926
\* countsQueueTime gate); fails with BugQueueUngated=TRUE (MCMutation3).
InvQueueOnlyCompletedJobs ==
  \A p \in countedQueue : aJobs[p[1]].jobs[p[2]].status = "completed"

\* [proposed] a previous attempt whose jobs timed out must not get a
\* "success" attempt span.  Finding 4 - FIXED 2026-07-06
\* (conclusionFromJobs analyzer.go:796-811 handles timed_out, consistent
\* with FailedJobs :877); fails with BugTimedOutSuccess=TRUE (MCMutation4).
InvTimedOutAttemptNotSuccess ==
  \A a \in emitted :
    ( /\ a < aRun.attempt
      /\ aJobs[a].st = "ok"
      /\ \E j \in JobIds : aJobs[a].jobs[j].concl = "timed_out" )
    => attemptConcl[a] /= "success"

\* [proposed - discovery note, FIXED 2026-07-06] every pending job in the
\* latest fetched snapshot is visible in PendingJobs, including jobs that
\* never started (analyzer.go:853-864 classifies before the early return).
\* Fails with BugUnstartedInvisible=TRUE (MCMutation5).
InvPendingJobsVisible ==
  aPhase = "done" =>
    \A j \in JobIds :
      IsPending(aJobs[aRun.attempt].jobs[j])
        => <<aRun.attempt, j>> \in countedPending

\* [proposed, added 2026-07-06] a COMPLETED run's emitted span never
\* carries an empty conclusion: analyzer.go:546-548 falls back to the
\* jobs-derived conclusion on the empty-conclusion fault.
\* Fails with BugNoConclFallback=TRUE (MCMutation6).
InvCompletedRunSpanHasConclusion ==
  (aPhase = "done" /\ aRun.status = "completed")
    => attemptConcl[aRun.attempt] /= ""

\* BAIT (must fail): claims no retry-attempt span is ever emitted.
\* Clearly reachable: re-run, then analyze.  Demands the witness trace.
InvBaitNoRetrySpan == \A a \in emitted : a = 1

=============================================================================
