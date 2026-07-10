----------------------------- MODULE Decision -----------------------------
(***************************************************************************)
(* Logical name: SyncBoundsDecision                                        *)
(* File is Decision.tla so SANY module name matches the path.              *)
(***************************************************************************)
(* Decision core for the UpsertJobs attempt guard.                         *)
(*                                                                         *)
(* Full design model: specs/sync-bounds/SyncBounds.tla (multi-run sync).  *)
(* This core pins the pure "stale attempt cannot stomp newer" decision:    *)
(*                                                                         *)
(* Production: acceptJobsAttempt + UpsertJobs                              *)
(*     UPDATE runs SET jobs_fetched=1                                      *)
(*     WHERE attempt is 0 OR run_attempt matches incoming                  *)
(*   accept only when incoming is 0 (unknown) or equals stored.            *)
(*                                                                         *)
(* Specgen subset: scalars, named actions, no records/quantifiers.         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
  Bug   \* TRUE: accept older attempt (pre-fix UpsertJobs, no attempt guard)

ASSUME Bug \in BOOLEAN

VARIABLES
  phase,            \* "empty" | "stored" | "decided"
  storedAttempt,    \* run_attempt currently in the store (0 = none)
  incomingAttempt,  \* attempt the offered jobs payload was fetched for
  accepted          \* whether the write was applied

vars == <<phase, storedAttempt, incomingAttempt, accepted>>

\* Init: empty store, no offer yet.
Init ==
  /\ phase = "empty"
  /\ storedAttempt = 0
  /\ incomingAttempt = 0
  /\ accepted = FALSE

\* Store1/2/3 set (or bump) the stored run_attempt.
\* Mirrors UpsertRuns writing run_attempt. Separate named
\* actions (no quantifier) so specgen stays on the simple dispatcher path.
Store1 ==
  /\ (phase = "empty" \/ phase = "stored")
  /\ phase' = "stored"
  /\ storedAttempt' = 1
  /\ UNCHANGED <<incomingAttempt, accepted>>

Store2 ==
  /\ (phase = "empty" \/ phase = "stored")
  /\ phase' = "stored"
  /\ storedAttempt' = 2
  /\ UNCHANGED <<incomingAttempt, accepted>>

Store3 ==
  /\ (phase = "empty" \/ phase = "stored")
  /\ phase' = "stored"
  /\ storedAttempt' = 3
  /\ UNCHANGED <<incomingAttempt, accepted>>

\* Offer a jobs write for the CURRENT stored attempt.
\* Always accepted (guard: run_attempt = incoming). "Newer" = the attempt
\* currently in the store after a bump (not a stale in-flight capture).
OfferNewer ==
  /\ phase = "stored"
  /\ storedAttempt > 0
  /\ phase' = "decided"
  /\ incomingAttempt' = storedAttempt
  /\ accepted' = TRUE
  /\ UNCHANGED storedAttempt

\* Offer a jobs write for an OLDER attempt (stale in-flight worker).
\* Faithful (Bug=FALSE): rejected — acceptJobsAttempt false / rows=0.
\* Mutation (Bug=TRUE): accepted - pre-fix unconditional jobs_fetched=1.
OfferOlder ==
  /\ phase = "stored"
  /\ storedAttempt > 1
  /\ phase' = "decided"
  /\ incomingAttempt' = storedAttempt - 1
  /\ accepted' = Bug
  /\ UNCHANGED storedAttempt

Next == Store1 \/ Store2 \/ Store3 \/ OfferNewer \/ OfferOlder

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in {"empty", "stored", "decided"}
  /\ storedAttempt \in 0..3
  /\ incomingAttempt \in 0..3
  /\ accepted \in BOOLEAN

\* Load-bearing: an accepted write was not for an older attempt.
\* Matches the fixed UpsertJobs guard (equality; OfferNewer only offers
\* equal, OfferOlder offers stored-1, so >= is the compact form).
NoStaleAccepted ==
  accepted => incomingAttempt >= storedAttempt

\* Bait: "never accepts any write" - MUST FAIL after OfferNewer.
\* Proves TLC explores Store then OfferNewer.
BaitNeverAccepted == ~accepted

=============================================================================
