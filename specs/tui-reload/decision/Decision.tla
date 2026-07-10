----------------------------- MODULE Decision -----------------------------
(***************************************************************************
Logical name: TuiReloadDecision
File is Decision.tla so SANY module name matches the path.

PURPOSE — decision core (specgen subset) for TUI reload / log-fetch
supersession.

Full design model: specs/tui-reload/TuiReload.tla (records, quantifiers).
This core pins the pure decisions production code must obey:

  1. PressReload starts a reload (isLoading); optional Bug drops the
     isLoading key guard so a second press can stack.
  2. ReloadDone finishes a reload: isLoading' = FALSE, reloadGen'++.
     In-flight fetch is NOT cancelled — only its gen stamp becomes stale.
  3. PressFetchN starts a fetch for job N, stamping fetchGen = reloadGen.
  4. FetchAccept: result matches current reloadGen → clear fetchJob.
  5. FetchDiscard: result gen # reloadGen → discard (correct path).
  6. FetchStaleBug: Bug path that wrongly accepts a stale result
     (staleAccepted' = TRUE) — mutation target for NoStaleAccepted.

Maps to pkg/tui/results symbols:
  PressReload   → doReload (isLoading key guard)
  ReloadDone    → ReloadResultMsg success (reloadGen++, clear expected job)
  PressFetchN   → fetchLogsForCurrentItem (stamps gen)
  FetchAccept   → logFetchResultFresh true → apply
  FetchDiscard  → logFetchResultFresh false → discard
  FetchStaleBug → pre-fix accept-any (Bug2 era)

Scalar-only: no records, no quantifiers in actions, no sequences.
***************************************************************************)
EXTENDS Naturals

CONSTANTS MaxGen, MaxJobs, Bug

ASSUME MaxGen \in Nat /\ MaxGen >= 1
ASSUME MaxJobs \in Nat /\ MaxJobs >= 1
ASSUME Bug \in BOOLEAN

VARIABLES isLoading, reloadGen, fetchJob, fetchGen, staleAccepted

vars == <<isLoading, reloadGen, fetchJob, fetchGen, staleAccepted>>

Init ==
  /\ isLoading = FALSE
  /\ reloadGen = 0
  /\ fetchJob = 0
  /\ fetchGen = 0
  /\ staleAccepted = FALSE

\* User presses r. Faithful: keys dropped while isLoading.
\* Bug=TRUE removes that guard. Gen bumps on ReloadDone.
PressReload ==
  /\ (Bug \/ ~isLoading)
  /\ reloadGen < MaxGen
  /\ isLoading' = TRUE
  /\ UNCHANGED <<reloadGen, fetchJob, fetchGen, staleAccepted>>

\* ReloadResultMsg success: clear loading, bump reloadGen. Does not cancel
\* the in-flight fetch command — gen stamp makes Accept/Discard choose.
ReloadDone ==
  /\ isLoading
  /\ reloadGen < MaxGen
  /\ isLoading' = FALSE
  /\ reloadGen' = reloadGen + 1
  /\ UNCHANGED <<fetchJob, fetchGen, staleAccepted>>

\* Press L for job 1 / job 2. No quantifier — separate actions.
PressFetch1 ==
  /\ ~isLoading
  /\ fetchJob = 0
  /\ fetchJob' = 1
  /\ fetchGen' = reloadGen
  /\ UNCHANGED <<isLoading, reloadGen, staleAccepted>>

PressFetch2 ==
  /\ ~isLoading
  /\ fetchJob = 0
  /\ fetchJob' = 2
  /\ fetchGen' = reloadGen
  /\ UNCHANGED <<isLoading, reloadGen, staleAccepted>>

\* In-flight result matches current reload generation → accept.
FetchAccept ==
  /\ fetchJob # 0
  /\ fetchGen = reloadGen
  /\ fetchJob' = 0
  /\ UNCHANGED <<isLoading, reloadGen, fetchGen, staleAccepted>>

\* Stale result (gen mismatch) → discard. Correct path after reload.
FetchDiscard ==
  /\ fetchJob # 0
  /\ fetchGen # reloadGen
  /\ fetchJob' = 0
  /\ UNCHANGED <<isLoading, reloadGen, fetchGen, staleAccepted>>

\* Mutation: wrongly accept a stale result. Only when Bug=TRUE.
FetchStaleBug ==
  /\ Bug
  /\ fetchJob # 0
  /\ fetchGen # reloadGen
  /\ staleAccepted' = TRUE
  /\ fetchJob' = 0
  /\ UNCHANGED <<isLoading, reloadGen, fetchGen>>

Next ==
  \/ PressReload
  \/ ReloadDone
  \/ PressFetch1
  \/ PressFetch2
  \/ FetchAccept
  \/ FetchDiscard
  \/ FetchStaleBug

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ isLoading \in BOOLEAN
  /\ reloadGen \in 0..MaxGen
  /\ fetchJob \in 0..MaxJobs
  /\ fetchGen \in 0..MaxGen
  /\ staleAccepted \in BOOLEAN

\* Load-bearing: a stale fetch result must never be accepted.
NoStaleAccepted == ~staleAccepted

\* Bait: fetchJob is always 0. MUST FAIL after PressFetch.
BaitNeverFetch == fetchJob = 0

=============================================================================
