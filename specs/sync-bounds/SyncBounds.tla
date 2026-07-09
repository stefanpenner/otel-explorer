--------------------------- MODULE SyncBounds ---------------------------
(***************************************************************************)
(* PURPOSE                                                                 *)
(*                                                                         *)
(* Model the otel-explorer store's incremental-sync bounds and             *)
(* attempt-invalidation protocol (pkg/store/store.go, pkg/store/sync.go).  *)
(* Design questions this spec is FOR:                                      *)
(*   - Can seeded (synced=0) runs clamp future listing windows and create  *)
(*     permanent listing gaps?                                             *)
(*   - Can two concurrent syncs leave stale attempt-N job detail marked    *)
(*     jobs_fetched=1 forever after a re-attempt?                          *)
(*   - Does a crash between listing and job-fetch (partial progress        *)
(*     persists; every SQL tx is separate) corrupt the bounds?             *)
(*                                                                         *)
(* ONE CORRECT BEHAVIOR (informal, fixes the atomicity grain):             *)
(*   1. Run A appears upstream at day 1.                                   *)
(*   2. Sync s: reads watermark (none) + oldest (none)  [2 SQL queries]    *)
(*   3. s lists upstream window [1..now]                [1 HTTP snapshot]  *)
(*   4. s upserts listed runs with synced=1             [1 SQL tx]         *)
(*   5. s reads RunsNeedingJobs -> {A}                  [1 SQL query]      *)
(*   6. s fetches A's jobs, upserts them, jobs_fetched=1 [HTTP + 1 SQL tx] *)
(*   7. Run A re-attempted upstream (attempt 2).                           *)
(*   8. Next sync re-lists A at attempt 2 inside watermark-overlap window; *)
(*      the upsert CASE resets jobs_fetched=0; jobs refetched at attempt 2.*)
(*                                                                         *)
(* ATOMICITY GRAIN: each SQLite transaction is one atomic action (SQLite   *)
(* serializes transactions).  The HTTP listing is one atomic snapshot of   *)
(* upstream (pagination tearing abstracted).  Job fetch (HTTP) + UpsertJobs*)
(* (tx) are coarsened into ONE action: the only externally visible effect  *)
(* is the written row, and the jobs-attempt value written differs between  *)
(* the split and coarse forms only among values that are all "not the      *)
(* current stored attempt", which the properties treat identically.        *)
(* Watermark and OldestRun are two separate queries -> two actions.        *)
(*                                                                         *)
(* DATA ABSTRACTION: created_at is a concrete tiny int (1..MaxTime)        *)
(* because the watermark/overlap window arithmetic IS the bug target.      *)
(* Everything else (status, ids, durations, jobs content) is abstracted.   *)
(* All upstream runs are completed at appearance (status dropped).         *)
(*                                                                         *)
(* MUTATION KNOBS (each restores a pre-fix behavior; faithful = all off):  *)
(*   Bug  = TRUE disables the attempt-bump jobs_fetched reset              *)
(*          (store.go:164-166 CASE).                                       *)
(*   Bug2 = TRUE removes the UpsertJobs attempt guard                      *)
(*          (store.go:390-400, fixed 2026-07-06 for FINDING 1).            *)
(*   FreshNowAtListing = TRUE recomputes the listing `since` from a fresh  *)
(*          time.Now() (pre-fix client behavior, fixed 2026-07-06 for      *)
(*          FINDING 2: sync.go:74-77 now passes the captured now down).    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Runs,        \* set of run ids (model values)
    Syncers,     \* set of sync processes (model values)
    MaxTime,     \* created_at / clock domain 1..MaxTime
    MaxAttempt,  \* run_attempt domain 1..MaxAttempt
    Days,        \* requested sync depth (sync.go: days argument)
    Overlap,     \* watermarkOverlap in days (sync.go:28, 24h -> 1)
    Seeding,     \* TRUE: URL-analysis seeder is active (fault-model knob)
    FreshNowAtListing,
                 \* FALSE = faithful to fixed code: `since` is computed once
                 \*         from the now captured at sync.go:39 and passed
                 \*         down (sync.go:74-77, client.go:724-737).
                 \* TRUE  = pre-fix mutation: the listing recomputed `since`
                 \*         from a fresh time.Now() -> window slides if the
                 \*         clock advanced (FINDING 2, fixed 2026-07-06).
    Bug,         \* TRUE: disable attempt-bump jobs_fetched reset
    Bug2         \* TRUE: remove the UpsertJobs attempt guard (pre-fix)

ASSUME MaxTime >= 1 /\ MaxAttempt >= 1 /\ Days >= 1 /\ Overlap >= 0
ASSUME Bug \in BOOLEAN /\ Bug2 \in BOOLEAN /\ Seeding \in BOOLEAN
ASSUME FreshNowAtListing \in BOOLEAN

VARIABLES
    clock,  \* wall clock in days, 1..MaxTime
    up,     \* upstream GitHub truth: [Runs -> [ex, created, att]]
    db,     \* store rows:   [Runs -> [present, created, synced, att, jf, ja]]
            \*   jf = jobs_fetched; ja = attempt the stored job detail
            \*   belongs to (0 = none / filtered-empty payload)
    pc,     \* per-syncer program counter
    wmS,    \* per-syncer: watermark read at sync start   (0 = none)
    oldS,   \* per-syncer: oldest    read at sync start   (0 = none)
    nowS,   \* per-syncer: now captured at sync start (sync.go:36)
    snap,   \* per-syncer listing snapshot: [Runs -> 0..MaxAttempt]
            \*   0 = not listed; >0 = listed at that attempt (byID)
    pend    \* per-syncer: runs still needing a job fetch this sync

vars == <<clock, up, db, pc, wmS, oldS, nowS, snap, pend>>

NoUp  == [ex |-> FALSE, created |-> 0, att |-> 0]
NoRow == [present |-> FALSE, created |-> 0, synced |-> 0,
          att |-> 0, jf |-> 0, ja |-> 0]
ZeroSnap == [r \in Runs |-> 0]

Max2(a, b) == IF a >= b THEN a ELSE b

SetMaxZ(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : m >= x
SetMinZ(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : m <= x

\* SQL: SELECT MAX(created_at) FROM runs WHERE synced=1   (store.go:299)
SyncedSet == {r \in Runs : db[r].present /\ db[r].synced = 1}
WM  == SetMaxZ({db[r].created : r \in SyncedSet})
\* SQL: SELECT MIN(created_at) ... created_at>0 AND synced=1 (store.go:433)
OLD == SetMinZ({db[r].created : r \in SyncedSet})

\* requestStart = now - days (sync.go:55), clamped into the model domain
ReqStart(t) == Max2(1, t - Days + 1)

\* Window floor per sync.go:56-67: clamp to watermark-overlap only when the
\* store already covers the requested depth (oldest <= requestStart, day
\* granularity); otherwise backfill the full requested window.
FloorOf(wm, old, t) ==
    IF wm = 0 \/ old = 0 \/ old > ReqStart(t)
    THEN ReqStart(t)
    ELSE Max2(ReqStart(t), wm - Overlap)

(***************************************************************************)
(* UpsertRow: one row of the upsert in store.go:154-168.                   *)
(*   sNew  = excluded.synced (1 sync path, 0 seed path)                    *)
(*   jfNew = excluded.jobs_fetched (payload carries jobs?)                 *)
(*   CASE WHEN excluded.run_attempt > runs.run_attempt                     *)
(*        THEN excluded.jobs_fetched ELSE max(...)          (164-166)      *)
(*   run_attempt = excluded.run_attempt unconditionally     (167)          *)
(*   synced      = max(runs.synced, excluded.synced)        (168)          *)
(*   hasJobs: payload replaced the jobs table (store.go:208-222)           *)
(*   Bug=TRUE removes the attempt-bump reset (always max).                 *)
(***************************************************************************)
UpsertRow(old, c, a, sNew, jfNew, jaNew, hasJobs) ==
    IF ~old.present
    THEN [present |-> TRUE, created |-> c, synced |-> sNew, att |-> a,
          jf |-> jfNew, ja |-> IF hasJobs THEN jaNew ELSE 0]
    ELSE [present |-> TRUE, created |-> c,
          synced  |-> Max2(old.synced, sNew),
          att     |-> a,
          jf      |-> IF (~Bug) /\ a > old.att
                      THEN jfNew
                      ELSE Max2(old.jf, jfNew),
          ja      |-> IF hasJobs THEN jaNew ELSE old.ja]

---------------------------------------------------------------------------

TypeInvariant ==
    /\ clock \in 1..MaxTime
    /\ up \in [Runs -> [ex : BOOLEAN, created : 0..MaxTime,
                        att : 0..MaxAttempt]]
    /\ db \in [Runs -> [present : BOOLEAN, created : 0..MaxTime,
                        synced : 0..1, att : 0..MaxAttempt,
                        jf : 0..1, ja : 0..MaxAttempt]]
    /\ pc \in [Syncers -> {"idle", "wm", "bounds", "listed",
                           "upserted", "fetch"}]
    /\ wmS  \in [Syncers -> 0..MaxTime]
    /\ oldS \in [Syncers -> 0..MaxTime]
    /\ nowS \in [Syncers -> 0..MaxTime]
    /\ snap \in [Syncers -> [Runs -> 0..MaxAttempt]]
    /\ pend \in [Syncers -> SUBSET Runs]

Init ==
    /\ clock = 1
    /\ up = [r \in Runs |-> NoUp]
    /\ db = [r \in Runs |-> NoRow]
    /\ pc = [s \in Syncers |-> "idle"]
    /\ wmS  = [s \in Syncers |-> 0]
    /\ oldS = [s \in Syncers |-> 0]
    /\ nowS = [s \in Syncers |-> 0]
    /\ snap = [s \in Syncers |-> ZeroSnap]
    /\ pend = [s \in Syncers |-> {}]

---------------------------------------------------------------------------
(* Environment: wall clock and the GitHub API's "true" runs. *)

Tick ==
    /\ clock < MaxTime
    /\ clock' = clock + 1
    /\ UNCHANGED <<up, db, pc, wmS, oldS, nowS, snap, pend>>

\* A new (completed) run appears upstream at the current day.
Appear(r) ==
    /\ ~up[r].ex
    /\ up' = [up EXCEPT ![r] = [ex |-> TRUE, created |-> clock, att |-> 1]]
    /\ UNCHANGED <<clock, db, pc, wmS, oldS, nowS, snap, pend>>

\* The run is re-run: attempt bumps (created_at unchanged).
ReAttempt(r) ==
    /\ up[r].ex
    /\ up[r].att < MaxAttempt
    /\ up' = [up EXCEPT ![r].att = @ + 1]
    /\ UNCHANGED <<clock, db, pc, wmS, oldS, nowS, snap, pend>>

---------------------------------------------------------------------------
(* URL-analysis seeder: SeedRuns (store.go:137-139, cmd/ote/main.go:1955). *)
(* One atomic tx; payload is a completed run WITH job detail               *)
(* (CollectCompletedRunData), synced=0.                                    *)

Seed(r) ==
    /\ Seeding
    /\ up[r].ex
    /\ db' = [db EXCEPT ![r] =
                UpsertRow(db[r], up[r].created, up[r].att,
                          0, 1, up[r].att, TRUE)]
    /\ UNCHANGED <<clock, up, pc, wmS, oldS, nowS, snap, pend>>

---------------------------------------------------------------------------
(* Sync pipeline (sync.go:34-150), per syncer s.                           *)

\* sync.go:36 (now := time.Now()) + sync.go:47 Watermark query.
ReadWM(s) ==
    /\ pc[s] = "idle"
    /\ nowS' = [nowS EXCEPT ![s] = clock]
    /\ wmS'  = [wmS  EXCEPT ![s] = WM]
    /\ pc'   = [pc   EXCEPT ![s] = "wm"]
    /\ UNCHANGED <<clock, up, db, oldS, snap, pend>>

\* sync.go:51 OldestRun query (separate tx from Watermark).
ReadOldest(s) ==
    /\ pc[s] = "wm"
    /\ oldS' = [oldS EXCEPT ![s] = OLD]
    /\ pc'   = [pc   EXCEPT ![s] = "bounds"]
    /\ UNCHANGED <<clock, up, db, wmS, nowS, snap, pend>>

\* Window computation (sync.go:57-69) + HTTP listing (sync.go:74-77).
\* Fixed code computes `since` ONCE from the now captured at sync start and
\* passes it down (FetchWorkflowRunsSince). FreshNowAtListing=TRUE restores
\* the pre-fix slide: `since` from a fresh time.Now() at listing time.
ListRuns(s) ==
    /\ pc[s] = "bounds"
    /\ LET floor == FloorOf(wmS[s], oldS[s], nowS[s])
           span  == nowS[s] - floor + 1
           lf    == IF FreshNowAtListing THEN clock - span + 1 ELSE floor
       IN snap' = [snap EXCEPT ![s] =
                     [r \in Runs |->
                        IF up[r].ex /\ up[r].created >= lf
                                    /\ up[r].created <= clock
                        THEN up[r].att
                        ELSE 0]]
    /\ wmS'  = [wmS  EXCEPT ![s] = 0]   \* locals dead after this point
    /\ oldS' = [oldS EXCEPT ![s] = 0]
    /\ pc'   = [pc   EXCEPT ![s] = "listed"]
    /\ UNCHANGED <<clock, up, db, nowS, pend>>

\* UpsertRuns: ONE tx for all listed rows (store.go:141-225), synced=1,
\* listing payload carries no jobs (jfNew=0, jobs table untouched).
UpsertListed(s) ==
    /\ pc[s] = "listed"
    /\ db' = [r \in Runs |->
                IF snap[s][r] > 0
                THEN UpsertRow(db[r], up[r].created, snap[s][r],
                               1, 0, 0, FALSE)
                ELSE db[r]]
    /\ pc' = [pc EXCEPT ![s] = "upserted"]
    /\ UNCHANGED <<clock, up, wmS, oldS, nowS, snap, pend>>

\* RunsNeedingJobs (sync.go:85-89, store.go:309-330): completed runs with
\* jobs_fetched=0 in [now-days, now+1h]; note: NO synced filter in the SQL.
ReadNeedJobs(s) ==
    /\ pc[s] = "upserted"
    /\ pend' = [pend EXCEPT ![s] =
                  {r \in Runs :
                     /\ db[r].present
                     /\ db[r].jf = 0
                     /\ db[r].created >= ReqStart(nowS[s])
                     /\ db[r].created <= nowS[s]}]
    /\ pc' = [pc EXCEPT ![s] = "fetch"]
    /\ UNCHANGED <<clock, up, db, wmS, oldS, nowS, snap>>

\* One worker: HTTP jobs fetch + UpsertJobs tx (sync.go:110-147,
\* store.go:379-425), coarsened to one action (see header).
\* capturedAtt = byID attempt from THIS sync's listing; 0 = not listed
\* -> ConvertJobs accepts all attempts (sync.go:116-119).
\* The jobs endpoint returns the LATEST attempt's jobs; ConvertJobs filters
\* to capturedAtt, so a re-attempted run yields an EMPTY payload (ja=0).
\* FIXED (2026-07-06, FINDING 1): UpsertJobs guards the whole write with
\* WHERE run_attempt = capturedAtt (store.go:390-400): a payload fetched
\* for a superseded attempt is discarded (capturedAtt 0 = unknown ->
\* accepted unconditionally). Bug2=TRUE removes the guard (pre-fix stomp).
FetchJob(s, r) ==
    /\ pc[s] = "fetch"
    /\ r \in pend[s]
    /\ db[r].present
    /\ LET cap == snap[s][r]
           jaN == IF cap = 0 THEN up[r].att
                  ELSE IF up[r].att = cap THEN cap ELSE 0
           guardOK == Bug2 \/ cap = 0 \/ db[r].att = cap
       IN db' = IF guardOK
                THEN [db EXCEPT ![r].jf = 1, ![r].ja = jaN]
                ELSE db
    /\ pend' = [pend EXCEPT ![s] = @ \ {r}]
    /\ UNCHANGED <<clock, up, pc, wmS, oldS, nowS, snap>>

\* Fault: a job fetch fails (sync.go:121-127); the run stays jf=0 and a
\* later sync re-enters it via RunsNeedingJobs.
FetchJobFail(s, r) ==
    /\ pc[s] = "fetch"
    /\ r \in pend[s]
    /\ pend' = [pend EXCEPT ![s] = @ \ {r}]
    /\ UNCHANGED <<clock, up, db, pc, wmS, oldS, nowS, snap>>

SyncDone(s) ==
    /\ pc[s] = "fetch"
    /\ pend[s] = {}
    /\ pc'   = [pc   EXCEPT ![s] = "idle"]
    /\ nowS' = [nowS EXCEPT ![s] = 0]
    /\ snap' = [snap EXCEPT ![s] = ZeroSnap]
    /\ UNCHANGED <<clock, up, db, wmS, oldS, pend>>

\* Fault: process crash at any point mid-sync.  Every committed tx persists
\* (partial progress IS the real behavior: each upsert is its own tx);
\* in-flight local state and pending workers die.  Restart = a fresh sync.
Crash(s) ==
    /\ pc[s] # "idle"
    /\ pc'   = [pc   EXCEPT ![s] = "idle"]
    /\ wmS'  = [wmS  EXCEPT ![s] = 0]
    /\ oldS' = [oldS EXCEPT ![s] = 0]
    /\ nowS' = [nowS EXCEPT ![s] = 0]
    /\ snap' = [snap EXCEPT ![s] = ZeroSnap]
    /\ pend' = [pend EXCEPT ![s] = {}]
    /\ UNCHANGED <<clock, up, db>>

---------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E r \in Runs : Appear(r) \/ ReAttempt(r) \/ Seed(r)
    \/ \E s \in Syncers :
         \/ ReadWM(s) \/ ReadOldest(s) \/ ListRuns(s)
         \/ UpsertListed(s) \/ ReadNeedJobs(s) \/ SyncDone(s) \/ Crash(s)
    \/ \E s \in Syncers, r \in Runs : FetchJob(s, r) \/ FetchJobFail(s, r)

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
(* PROPERTIES                                                              *)

(* [code-documented, store.go:150-153 + store_test.go:243-268]             *)
(* "attempt-1 jobs would otherwise be served forever": once the store has  *)
(* caught up to the upstream attempt, jobs_fetched=1 must mean the stored  *)
(* job detail belongs to that attempt.  (While db.att < up.att the store   *)
(* is merely behind; the next listing's reset heals it -- not a violation.)*)
NoStaleFetchedJobs ==
    \A r \in Runs :
        (/\ db[r].present
         /\ db[r].jf = 1
         /\ up[r].ex
         /\ db[r].att = up[r].att)
        => db[r].ja = db[r].att

(* [proposed; emergent consequence of the code-documented rule that seeded *)
(* runs must not move the bounds, store.go:101-105 comment]                *)
(* No permanently lost run: an upstream run below the clamped window floor *)
(* (watermark - overlap) with backfill impossible (oldest already at full  *)
(* requested depth => every future window is clamped, and the watermark    *)
(* only grows) can never be listed again.                                  *)
NoPermanentGap ==
    \A r \in Runs :
        (/\ up[r].ex
         /\ ~(db[r].present /\ db[r].synced = 1)
         /\ WM # 0
         /\ OLD = 1        \* store claims full requested depth -> clamps
         /\ ReqStart(MaxTime) = 1)
        => up[r].created >= WM - Overlap

(* [code-documented, store.go:168 synced=max(old,new); rows never deleted] *)
SyncedMonotone ==
    [][\A r \in Runs :
         db[r].present => (db'[r].present
                           /\ db'[r].synced >= db[r].synced)]_vars

(* [code-documented, store.go:150-153,164-166] On an attempt bump the old  *)
(* jobs_fetched must not leak through: if the new row claims jf=1 it must  *)
(* be because the SAME payload carried that attempt's jobs.                *)
AttemptBumpNoStaleCarry ==
    [][\A r \in Runs :
         (/\ db[r].present
          /\ db'[r].present
          /\ db'[r].att > db[r].att
          /\ db'[r].jf = 1)
         => db'[r].ja = db'[r].att]_vars

(* Bait for MCBait.cfg: claims a sync never finds a run needing job        *)
(* detail.  Reaching a nonempty pend requires the WHOLE pipeline           *)
(* (Appear -> ReadWM -> ReadOldest -> ListRuns -> UpsertListed ->          *)
(* ReadNeedJobs), so this MUST fail with a witness trace, proving TLC      *)
(* actually explores the sync pipeline, not just the seeder.               *)
BaitNoRunEverNeedsJobs ==
    \A s \in Syncers : pend[s] = {}

===========================================================================
