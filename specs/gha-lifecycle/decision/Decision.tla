----------------------------- MODULE Decision -----------------------------
(***************************************************************************)
(* Logical name: GhaLifecycleDecision                                      *)
(* File is Decision.tla so SANY module name matches the path.              *)
(***************************************************************************)
(* Decision core for job classification gates in processJob.               *)
(*                                                                         *)
(* Full design model: specs/gha-lifecycle/GhaLifecycle.tla.                *)
(* This core pins the pure isPending / FailedJobs / countsQueueTime        *)
(* guards so they cannot drift from the generated Go module.               *)
(*                                                                         *)
(* analyzer.go:                                                            *)
(*   isJobPending     : status != completed OR CompletedAt empty  (:816)   *)
(*   FailedJobs       : not pending AND (failure OR timed_out)    (:904)   *)
(*   countsQueueTime  : not pending AND not skipped/cancelled     (:924)   *)
(*                                                                         *)
(* Abstraction: status is folded into hasCompletedAt for the interesting   *)
(* fault (completed with empty completed_at is still pending). Pending     *)
(* here is ~hasCompletedAt - same as full-spec IsPending when status is    *)
(* already "completed".                                                    *)
(*                                                                         *)
(* Specgen subset: scalars, named actions, no records/quantifiers.         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
  Bug   \* TRUE: drop the not-pending guard on fail + queue (mutation)

ASSUME Bug \in BOOLEAN

VARIABLES
  hasCompletedAt,   \* job.CompletedAt != ""
  conclusion,       \* "" | "failure" | "success" | "timed_out"
  countedPending,   \* metrics.PendingJobs received this job
  countedFailed,    \* metrics.FailedJobs++
  queueCounted      \* metrics.QueueTimes received a sample

vars == <<hasCompletedAt, conclusion, countedPending, countedFailed, queueCounted>>

\* Init: completed-looking failure with NO completed_at - pending under the
\* faithful helper; the mutation witness for PendingNeverFailed.
Init ==
  /\ hasCompletedAt = FALSE
  /\ conclusion = "failure"
  /\ countedPending = FALSE
  /\ countedFailed = FALSE
  /\ queueCounted = FALSE

\* ClassifyPending - analyzer.go:855-864.
\* Only pending jobs land in PendingJobs.
\* Pending = ~hasCompletedAt (isJobPending with status=completed assumed).
ClassifyPending ==
  /\ ~hasCompletedAt
  /\ ~countedPending
  /\ countedPending' = TRUE
  /\ UNCHANGED <<hasCompletedAt, conclusion, countedFailed, queueCounted>>

\* ClassifyFailed - analyzer.go:903-906.
\* Faithful: not pending AND (failure OR timed_out).
\* Bug=TRUE drops the not-pending guard (MCMutation of full GhaLifecycle).
ClassifyFailed ==
  /\ (Bug \/ hasCompletedAt)
  /\ (conclusion = "failure" \/ conclusion = "timed_out")
  /\ ~countedFailed
  /\ countedFailed' = TRUE
  /\ UNCHANGED <<hasCompletedAt, conclusion, countedPending, queueCounted>>

\* ClassifyQueue - analyzer.go:924-926 countsQueueTime.
\* Faithful: not pending (and not skipped/cancelled - those conclusions are
\* outside this core's domain, so the conclusion gate is always open here).
\* Bug=TRUE drops the not-pending guard (same tooth as BugQueueUngated).
ClassifyQueue ==
  /\ (Bug \/ hasCompletedAt)
  /\ ~queueCounted
  /\ queueCounted' = TRUE
  /\ UNCHANGED <<hasCompletedAt, conclusion, countedPending, countedFailed>>

\* Reset - clear counts and advance the job scenario so TLC can reach
\* completed-failure / timed_out / success (faithful fail+queue paths).
\* Cycle: failure -> timed_out -> success -> failure, always completed after
\* the first Reset (hasCompletedAt becomes TRUE and stays TRUE).
Reset ==
  /\ countedPending' = FALSE
  /\ countedFailed' = FALSE
  /\ queueCounted' = FALSE
  /\ hasCompletedAt' = TRUE
  /\ conclusion' =
       IF conclusion = "failure" THEN "timed_out"
       ELSE IF conclusion = "timed_out" THEN "success"
       ELSE "failure"

Next ==
  \/ ClassifyPending
  \/ ClassifyFailed
  \/ ClassifyQueue
  \/ Reset

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ hasCompletedAt \in BOOLEAN
  /\ conclusion \in {"", "failure", "success", "timed_out"}
  /\ countedPending \in BOOLEAN
  /\ countedFailed \in BOOLEAN
  /\ queueCounted \in BOOLEAN

\* Load-bearing (InvPendingNeverFailed): a pending job is never failed.
PendingNeverFailed == ~(countedPending /\ countedFailed)

\* Load-bearing (InvQueueOnlyCompletedJobs / countsQueueTime):
\* queue time only for non-pending jobs.
QueueOnlyNotPending == queueCounted => hasCompletedAt

\* Bait: "never lands in PendingJobs" - MUST FAIL from Init via ClassifyPending.
BaitNeverPending == ~countedPending

=============================================================================
