--------------------------- MODULE TuiReload ---------------------------
(***************************************************************************
PURPOSE

What is this spec FOR?
  The bubbletea results TUI (pkg/tui/results) runs two async operations:
  reload (doReload goroutine + listenForProgress command chain) and
  log fetch (fetchLogsForCurrentItem command). Both deliver results as
  tea.Msgs. The design question: when reload and log fetch race, is the
  documented intent "reload supersedes fetch; in-flight fetch results are
  ignored" (model.go:407-409 comment) actually enforced? And does the
  isLoading key guard (model.go:473-479) fully prevent the stale-channel
  hazard of a second reload overwriting progressCh/resultCh?

  HISTORY: the original code did NOT enforce supersession (campaign
  finding #3, 2026-07-05).  Fixed 2026-07-06: LogFetchResultMsg is
  tagged with (jobID, reload generation) at command creation
  (logfetch.go:59-66) and the handler discards mismatched results
  (model.go:365-372).  The pre-fix behavior is kept reachable behind
  Bug2 = TRUE (MCMutation3.cfg, must FAIL).

ONE CORRECT BEHAVIOR (informal sketch, fixes the atomicity grain):
  1. User presses L: fetch for job X starts (logFetchingJobID = X),
     command captures the CURRENT span set and tags the future msg
     with (X, reloadGen = 0).
  2. User presses r: reload gen 1 starts (isLoading = TRUE, fresh
     progress/result channels, one listen command on gen 1).
  3. Reload goroutine finishes; listen command reads the result.
  4. Update(ReloadResultMsg): spans := gen-1 data, reloadGen := 1,
     logFetchingJobID := 0, logFetchedJobIDs := {}, channels nil'd,
     isLoading := FALSE.
  5. The stale fetch-X result arrives tagged (X, gen 0): handler sees
     gen 0 # reloadGen = 1 and DISCARDS it (model.go:365-372).
     Displayed spans = gen-1 base only.  Correct — even if a NEW fetch
     of X is already in flight (the witness trace of finding #3).

ATOMICITY GRAIN
  Each Update(msg) handler is one atomic action (bubbletea guarantee).
  Concurrency lives only in WHICH in-flight command delivers next.
  Coarsenings (each commutes with all other actions):
  - "channel read + Update handler" is one action: the value a listen
    command reads is fixed at read time and delivery is the only free
    variable in between.
  - "fetch goroutine computes + delivers + Update" is one action: the
    msg content (which job, which base span set, which generation) is
    fixed when the command is CREATED (logfetch.go:59-66 captures
    spans/jobID/gen), so only delivery time is free; we let
    FetchDeliver fire at any time.
  - Reload goroutine completion (GoroutineDone) stays a separate action
    because it changes what a listen command reads (progress vs result).

DATA ABSTRACTION
  Spans are modeled by provenance, not content:
  - displayedBase \in 0..MaxGen        : generation of m.spans base data
  - displayedLogs : set of [job, base] : appended log spans, tagged with
    the job they belong to and the base generation their fetch ran under.
  The bug target is span staleness/loss, so WHICH spans matters, not how
  many.  Abstraction: every successful fetch returns >= 1 span.

CONSTANTS
  Bug = TRUE is the mutation: removes the isLoading key guard
    (model.go:473-479) so a second reload can start while one is in
    flight.  Proves the invariants have teeth and exercises the
    stale-channel supersession hazard (discovery race (b)).
  Bug2 = TRUE is the mutation for finding #3: strips the (job, gen)
    tag check from LogFetchResultMsg handling — the pre-2026-07-06
    code, which attributed any arriving result to the CURRENT
    logFetchingJobID.  Must FAIL NoStaleLogSpans (MCMutation3.cfg).
***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
  Jobs,       \* set of fetchable job IDs (model values)
  MaxGen,     \* max number of reloads in a behavior
  Bug,        \* BOOLEAN: TRUE = disable the isLoading key guard (mutation)
  Bug2        \* BOOLEAN: TRUE = drop the (job, gen) staleness check (mutation)

ASSUME MaxGen \in Nat /\ MaxGen >= 1
ASSUME Bug \in BOOLEAN /\ Bug2 \in BOOLEAN

Gens == 1..MaxGen

NoJob == "NoJob"   \* sentinel for logFetchingJobID == 0; Jobs are model values
ASSUME NoJob \notin Jobs

VARIABLES
  gen,           \* latest started reload generation, 0..MaxGen
  status,        \* [Gens -> {"none","running","done"}] reload goroutines
  resultRead,    \* SUBSET Gens: generations whose ReloadResultMsg was consumed
  succeeded,     \* SUBSET resultRead: generations whose reload succeeded
  isLoading,     \* m.isLoading
  chGen,         \* generation m.progressCh/m.resultCh point at; 0 = nil
  listens,       \* [Gens -> Nat]: in-flight listen commands per captured chan gen
  displayedBase, \* generation of the base data in m.spans (0 = initial load)
  displayedLogs, \* SUBSET [job: Jobs, base: 0..MaxGen]: appended log spans
  fetchingJob,   \* m.logFetchingJobID: NoJob or a job
  fetchedJobs,   \* m.logFetchedJobIDs
  fetches        \* SUBSET [job: Jobs, base: 0..MaxGen]: in-flight fetch cmds

vars == <<gen, status, resultRead, succeeded, isLoading, chGen, listens,
          displayedBase, displayedLogs, fetchingJob, fetchedJobs, fetches>>

LogRecords == [job: Jobs, base: 0..MaxGen]

TypeInvariant ==
  /\ gen \in 0..MaxGen
  /\ status \in [Gens -> {"none", "running", "done"}]
  /\ resultRead \subseteq Gens
  /\ succeeded \subseteq resultRead
  /\ isLoading \in BOOLEAN
  /\ chGen \in 0..MaxGen
  /\ listens \in [Gens -> 0..MaxGen]
  /\ displayedBase \in 0..MaxGen
  /\ displayedLogs \subseteq LogRecords
  /\ fetchingJob \in Jobs \cup {NoJob}
  /\ fetchedJobs \subseteq Jobs
  /\ fetches \subseteq LogRecords

Init ==
  /\ gen = 0
  /\ status = [g \in Gens |-> "none"]
  /\ resultRead = {}
  /\ succeeded = {}
  /\ isLoading = FALSE
  /\ chGen = 0
  /\ listens = [g \in Gens |-> 0]
  /\ displayedBase = 0
  /\ displayedLogs = {}
  /\ fetchingJob = NoJob
  /\ fetchedJobs = {}
  /\ fetches = {}

-----------------------------------------------------------------------------
(* ACTIONS — each is one atomic Update(msg) handler or goroutine event.   *)

(* User presses 'r' (model.go:844-849 / 705-711) + doReload
   (model.go:1878-1901): fresh channels overwrite m.progressCh/m.resultCh,
   goroutine spawned, ONE listen command returned.
   Faithful code: the KeyMsg guard model.go:473-479 drops keys while
   isLoading, so a second reload cannot start.  Bug=TRUE removes that. *)
PressReload ==
  /\ gen < MaxGen
  /\ Bug \/ ~isLoading
  /\ gen' = gen + 1
  /\ status' = [status EXCEPT ![gen + 1] = "running"]
  /\ isLoading' = TRUE
  /\ chGen' = gen + 1
  /\ listens' = [listens EXCEPT ![gen + 1] = @ + 1]
  /\ UNCHANGED <<resultRead, succeeded, displayedBase, displayedLogs,
                 fetchingJob, fetchedJobs, fetches>>

(* User presses 'L' (model.go:851-855) + fetchLogsForCurrentItem
   (logfetch.go:18-68): guarded by logFetchingJobID == 0 (logfetch.go:19)
   and not-already-fetched (logfetch.go:40); the command captures the
   CURRENT span set and stamps the msg tag (logfetch.go:59-66), i.e.
   base = displayedBase.
   Keys are dropped while isLoading (model.go:473-479), so no fetch can
   START during a reload — but one started earlier can still FINISH. *)
PressFetch(j) ==
  /\ ~isLoading
  /\ fetchingJob = NoJob
  /\ j \notin fetchedJobs
  /\ fetchingJob' = j
  /\ fetches' = fetches \cup {[job |-> j, base |-> displayedBase]}
  /\ UNCHANGED <<gen, status, resultRead, succeeded, isLoading, chGen,
                 listens, displayedBase, displayedLogs, fetchedJobs>>

(* Reload goroutine finishes (model.go:1888-1897): closes progressCh,
   puts the result in the buffered resultCh.  Fault model: this happens
   at ANY time relative to everything else. *)
GoroutineDone(g) ==
  /\ status[g] = "running"
  /\ status' = [status EXCEPT ![g] = "done"]
  /\ UNCHANGED <<gen, resultRead, succeeded, isLoading, chGen, listens,
                 displayedBase, displayedLogs, fetchingJob, fetchedJobs,
                 fetches>>

(* A listen command captured gen g's channels (model.go:1904-1924) and
   reads a progress update (goroutine g still running).  Update handler
   (model.go:455-466) re-listens on the CURRENT m.progressCh/m.resultCh,
   i.e. on chGen — which may differ from g, or be nil (chGen = 0). *)
ListenProgress(g) ==
  /\ listens[g] > 0
  /\ status[g] = "running"
  /\ listens' = IF chGen = 0
                  THEN [listens EXCEPT ![g] = @ - 1]
                ELSE IF chGen = g
                  THEN listens
                ELSE [listens EXCEPT ![g] = @ - 1, ![chGen] = @ + 1]
  /\ UNCHANGED <<gen, status, resultRead, succeeded, isLoading, chGen,
                 displayedBase, displayedLogs, fetchingJob, fetchedJobs,
                 fetches>>

(* A listen command on gen g reads the (unconsumed) result; reload
   SUCCEEDED.  Update(ReloadResultMsg) success path (model.go:395-445):
   replaces m.spans entirely, nils the channels, bumps m.reloadGen and
   resets ALL log-fetch state (model.go:407-414: "any in-flight fetch
   result is ignored").  Bumping the generation is what makes earlier
   fetch tags stale — modeled here as displayedBase' = g. *)
ListenResultOK(g) ==
  /\ listens[g] > 0
  /\ status[g] = "done"
  /\ g \notin resultRead
  /\ resultRead' = resultRead \cup {g}
  /\ succeeded' = succeeded \cup {g}
  /\ listens' = [listens EXCEPT ![g] = @ - 1]
  /\ isLoading' = FALSE
  /\ chGen' = 0
  /\ displayedBase' = g
  /\ displayedLogs' = {}
  /\ fetchingJob' = NoJob
  /\ fetchedJobs' = {}
  /\ UNCHANGED <<gen, status, fetches>>

(* Same, but the reload FAILED.  Update(ReloadResultMsg) error path
   (model.go:402-405) returns EARLY: spans kept, generation NOT bumped,
   and the log-fetch reset at model.go:410-413 is SKIPPED, so an
   in-flight fetch stays attributable. *)
ListenResultErr(g) ==
  /\ listens[g] > 0
  /\ status[g] = "done"
  /\ g \notin resultRead
  /\ resultRead' = resultRead \cup {g}
  /\ listens' = [listens EXCEPT ![g] = @ - 1]
  /\ isLoading' = FALSE
  /\ chGen' = 0
  /\ UNCHANGED <<gen, status, succeeded, displayedBase, displayedLogs,
                 fetchingJob, fetchedJobs, fetches>>

(* Whether the handler discards this fetch result.
   Faithful (fixed) code: the msg is tagged with (jobID, gen) at
   command creation (model.go:178-184, logfetch.go:63-66); the handler
   discards when the tag mismatches the current logFetchingJobID or
   reloadGen (model.go:365-372).  fetchingJob = NoJob also discards
   (a tagged jobID never matches 0).
   Bug2 = TRUE (pre-fix code, finding #3): the msg carried NO tag, so
   only logFetchingJobID == 0 discarded; any surviving result was
   attributed to the CURRENT fetchingJob. *)
FetchDiscarded(f) ==
  \/ fetchingJob = NoJob
  \/ ~Bug2 /\ (f.job # fetchingJob \/ f.base # displayedBase)

(* In-flight fetch command delivers LogFetchResultMsg; fetch SUCCEEDED.
   Update handler success path (model.go:364-393): clears
   logFetchingJobID, marks the CURRENT fetchingJob as fetched
   (model.go:384), appends the MSG's spans (model.go:387) — which carry
   f's provenance; with the tag check those always match fetchingJob. *)
FetchDeliverOK(f) ==
  /\ f \in fetches
  /\ fetches' = fetches \ {f}
  /\ IF FetchDiscarded(f)
       THEN UNCHANGED <<fetchingJob, fetchedJobs, displayedLogs>>
       ELSE /\ fetchingJob' = NoJob
            /\ fetchedJobs' = fetchedJobs \cup {fetchingJob}
            /\ displayedLogs' = displayedLogs \cup {f}
  /\ UNCHANGED <<gen, status, resultRead, succeeded, isLoading, chGen,
                 listens, displayedBase>>

(* Fetch FAILED.  Handler error path (model.go:374-380): clears
   logFetchingJobID (line 374 runs BEFORE the err check but AFTER the
   staleness discard), does NOT mark fetched, appends nothing. *)
FetchDeliverErr(f) ==
  /\ f \in fetches
  /\ fetches' = fetches \ {f}
  /\ IF FetchDiscarded(f)
       THEN UNCHANGED fetchingJob
       ELSE fetchingJob' = NoJob
  /\ UNCHANGED <<gen, status, resultRead, succeeded, isLoading, chGen,
                 listens, displayedBase, displayedLogs, fetchedJobs>>

Next ==
  \/ PressReload
  \/ \E j \in Jobs : PressFetch(j)
  \/ \E g \in Gens : GoroutineDone(g)
  \/ \E g \in Gens : ListenProgress(g)
  \/ \E g \in Gens : ListenResultOK(g)
  \/ \E g \in Gens : ListenResultErr(g)
  \/ \E f \in LogRecords : FetchDeliverOK(f)
  \/ \E f \in LogRecords : FetchDeliverErr(f)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* PROPERTIES *)

(* CODE-DOCUMENTED: the KeyMsg guard model.go:473-479 ("Ignore keys while
   loading") plus the explicit !m.isLoading check at model.go:707 enforce
   at most one reload in flight. *)
AtMostOneReloadRunning ==
  Cardinality({g \in Gens : status[g] = "running"}) <= 1

(* CODE-DOCUMENTED: model.go:407-409 "Reset log fetch state so
   previously fetched jobs can be re-fetched against the fresh data, and
   any in-flight fetch result is ignored" and model.go:366-370 "Stale
   result ... discard so they never attach to the current data".
   Enforced by the (jobID, gen) tag since 2026-07-06 (finding #3 fixed).
   Stated as: every displayed log span was fetched against the currently
   displayed base generation. *)
NoStaleLogSpans ==
  \A l \in displayedLogs : l.base = displayedBase

(* PROPOSED: a job marked as fetched actually has its log spans in the
   display (under the abstraction that a successful fetch returns >= 1
   span).  Guards against cross-attribution: marking job Y fetched while
   appending job X's spans. *)
FetchedJobsHaveTheirSpans ==
  \A j \in fetchedJobs : \E l \in displayedLogs : l.job = j

(* PROPOSED: the displayed base data is always the LATEST successfully
   completed reload generation (0 = initial data before any success). *)
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
DisplayedIsLatestSuccess ==
  displayedBase = MaxOf({0} \cup succeeded)

(* PROPOSED: no listen command is permanently stuck.  A listen command
   whose captured generation is done AND result-consumed blocks forever
   in listenForProgress's select (model.go:1912-1923): progressCh is
   closed/drained and resultCh (never closed, model.go:1881) is empty. *)
NoStuckListen ==
  \A g \in Gens :
    listens[g] > 0 => ~(status[g] = "done" /\ g \in resultRead)

(* BAIT (MCBait.cfg only): claims log spans can never be displayed.
   Clearly reachable — MUST fail, proving TLC explores fetch delivery. *)
BaitNoLogsDisplayed ==
  displayedLogs = {}

=============================================================================
