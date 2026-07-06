--------------------------- MODULE LogGroups ---------------------------
(***************************************************************************)
(* PURPOSE                                                                 *)
(*                                                                         *)
(* Model the GHA log group-nesting state machine and gap-based span        *)
(* production in pkg/logparse/timestamp.go (as FIXED on 2026-07-06 for     *)
(* FINDINGS.md #12):                                                       *)
(*   - splitGroups        (timestamp.go:44-90)  — one pass over lines,     *)
(*     OutsideGroup/InsideGroup, ##[group] opens, ##[endgroup] closes,     *)
(*     nested open IMPLICIT-CLOSES the outer group at the open line's      *)
(*     time (51-63; it used to overwrite and drop it),                     *)
(*     EOF closes an open group at its last content-line time (79-87).    *)
(*   - parseWithGroups    (timestamp.go:111-186) — groups are SORTED by    *)
(*     start (114-116); group i's span end is overridden to                *)
(*     groups[i+1].start (125-126), or to stepEnd for the last group       *)
(*     (127-130), else keeps g.end; the override is CLAMPED so it can      *)
(*     never precede the group start (134-139); gap-parsed children are    *)
(*     CLAMPED into [g.start, endTime] (154-157, clampSpans 205-222).      *)
(*   - parseGapBased      (timestamp.go:226-310) — split a line sequence   *)
(*     on inter-line gaps >= minGap; current.end only advances FORWARD     *)
(*     (263-268; it used to move backward on out-of-order lines);          *)
(*     spans shorter than minSpan are filtered (288-291).                  *)
(*                                                                         *)
(* Bug target: timestamps are NOT necessarily monotonic (GHA logs can      *)
(* interleave buffered streams).  The fixed code no longer assumes         *)
(* monotonicity.  Fuzzing already covers byte-level robustness; this spec  *)
(* checks the STATE-MACHINE / ordering properties.                         *)
(*                                                                         *)
(* ONE CORRECT BEHAVIOR (fixes the atomicity grain: one log line per       *)
(* action; the deterministic post-pass parseWithGroups+parseGapBased is a  *)
(* single Produce action since nothing interleaves with it):               *)
(*   1. Open "A" @1      -> cur = {start:1, lines:<<>>}                    *)
(*   2. Plain @1         -> cur.lines = <<1>>                              *)
(*   3. Close @2         -> groups = << [start 1, end 2, lines <<1>>] >>   *)
(*   4. Open "B" @3, Close @4 -> groups has 2 entries                      *)
(*   5. EndInput, stepEnd=4                                                *)
(*   6. Produce: span1 = [1,3] (end overridden to next group's start),     *)
(*      span2 = [3,4] (end overridden to stepEnd).                         *)
(*   Invariants: starts sorted, start <= end, spans tile, children inside  *)
(*   their parent.                                                         *)
(*                                                                         *)
(* ABSTRACTIONS: line content is reduced to kind (open/close/plain);       *)
(* names, collapseSpans, groupByPrefix, filterBySignificance and top-level *)
(* gap spans are dropped (name/noise cosmetics, cannot affect the group    *)
(* ordering properties; collapse/prefix merges preserve start/end bounds,  *)
(* and clampSpans runs recursively AFTER them).  Time is a tiny int domain *)
(* 1..MaxTime with 0 = Go zero value, because timestamps ARE the bug       *)
(* target.                                                                 *)
(*                                                                         *)
(* MUTATION CONSTANTS — each restores one pre-fix behavior so the          *)
(* MCMutation* configs prove the invariants keep their teeth:              *)
(*   Bug      = TRUE: skip the parseWithGroups end-override entirely       *)
(*              (group spans keep their raw splitGroups end).              *)
(*   BugSort  = TRUE: skip the sort at parseWithGroups entry (114-116).    *)
(*   BugClamp = TRUE: skip the end-override clamp (134-139).               *)
(*   BugChild = TRUE: skip the child clamp (157).                          *)
(*   BugNest  = TRUE: nested ##[group] overwrites/drops the outer group    *)
(*              (pre-fix 50-56 behavior).                                  *)
(*   BugGap   = TRUE: parseGapBased current.end updates unconditionally,   *)
(*              moving backward on out-of-order lines (pre-fix :218).      *)
(*              No invariant at these bounds distinguishes BugGap (the     *)
(*              spurious splits it causes are quality, not safety); its    *)
(*              teeth live in the Go test                                  *)
(*              TestLogGroupsSpec_GapBaselineOnlyMovesForward.             *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS
    MaxTime,     \* time domain is 1..MaxTime; 0 encodes Go's zero time
    MaxLines,    \* max log lines the environment produces
    MinGap,      \* p.MinGapDuration  (timestamp.go:235-238)
    MinSpan,     \* p.MinSpanDuration (timestamp.go:239-242)
    Monotonic,   \* TRUE: environment produces non-decreasing timestamps
    Bug,         \* TRUE: skip the parseWithGroups end-override
    BugSort,     \* TRUE: skip the sort at parseWithGroups entry
    BugClamp,    \* TRUE: skip the end-override clamp
    BugChild,    \* TRUE: skip the child clamp
    BugNest,     \* TRUE: nested open drops the outer group (pre-fix)
    BugGap       \* TRUE: gap baseline moves backward (pre-fix)

ASSUME MaxTime \in Nat /\ MaxTime >= 1
ASSUME MaxLines \in Nat
ASSUME MinGap \in Nat /\ MinGap >= 1
ASSUME MinSpan \in Nat /\ MinSpan >= 1
ASSUME Monotonic \in BOOLEAN
ASSUME Bug \in BOOLEAN
ASSUME BugSort \in BOOLEAN
ASSUME BugClamp \in BOOLEAN
ASSUME BugChild \in BOOLEAN
ASSUME BugNest \in BOOLEAN
ASSUME BugGap \in BOOLEAN

Times == 1..MaxTime

\* cur.start = 0 encodes "no current group" (OutsideGroup).
NoCur == [start |-> 0, lines |-> <<>>]

VARIABLES
    pc,       \* "scan" (splitGroups loop) -> "assign" (post-EOF) -> "done"
    lastT,    \* time of last arrived line (0 = none) — env monotonicity
    nLines,   \* lines consumed so far
    cur,      \* current open group: [start, lines] (timestamp.go:47)
    groups,   \* appended group blocks: [start, end, lines] (timestamp.go:46)
    dropped,  \* count of groups discarded by a nested ##[group] — only
              \* reachable with BugNest (pre-fix 50-56 overwrite)
    stepEnd,  \* step end time passed to Parse; 0 = zero time
    spans,    \* produced group spans: seq of [start, end]
    kids      \* produced child spans: set of [start, end, parent]

vars == <<pc, lastT, nLines, cur, groups, dropped, stepEnd, spans, kids>>

IsGroupRec(g) ==
    /\ g.start \in Times
    /\ g.end \in Times
    /\ g.lines \in Seq(Times)

TypeInvariant ==
    /\ pc \in {"scan", "assign", "done"}
    /\ lastT \in 0..MaxTime
    /\ nLines \in 0..MaxLines
    /\ \/ cur = NoCur
       \/ (cur.start \in Times /\ cur.lines \in Seq(Times))
    /\ \A i \in DOMAIN groups : IsGroupRec(groups[i])
    /\ dropped \in 0..MaxLines
    /\ stepEnd \in 0..MaxTime
    /\ \A i \in DOMAIN spans :
          spans[i].start \in Times /\ spans[i].end \in Times
    /\ \A k \in kids :
          k.start \in Times /\ k.end \in Times /\ k.parent \in DOMAIN spans

Init ==
    /\ pc = "scan"
    /\ lastT = 0
    /\ nLines = 0
    /\ cur = NoCur
    /\ groups = <<>>
    /\ dropped = 0
    /\ stepEnd = 0
    /\ spans = <<>>
    /\ kids = {}

\* Environment: any time in the domain; non-decreasing iff Monotonic.
TimeOK(t) == t \in Times /\ (Monotonic => t >= lastT)

InScan == pc = "scan" /\ nLines < MaxLines

(***************************************************************************)
(* splitGroups loop body — one line per action (timestamp.go:49-78).       *)
(***************************************************************************)

\* "##[group]NAME": start a new group.  If one is already open the fixed
\* code implicit-closes it at this line's time and appends it (51-63).
\* BugNest restores the pre-fix overwrite: outer group + lines dropped.
Open(t) ==
    /\ InScan
    /\ TimeOK(t)
    /\ groups' = IF cur = NoCur \/ BugNest THEN groups
                 ELSE Append(groups,
                        [start |-> cur.start, end |-> t, lines |-> cur.lines])
    /\ dropped' = IF cur # NoCur /\ BugNest THEN dropped + 1 ELSE dropped
    /\ cur' = [start |-> t, lines |-> <<>>]
    /\ lastT' = t
    /\ nLines' = nLines + 1
    /\ UNCHANGED <<pc, stepEnd, spans, kids>>

\* "##[endgroup]": close current group (end = this line's time) and append.
\* A stray endgroup with no open group is ignored (timestamp.go:65-72).
Close(t) ==
    /\ InScan
    /\ TimeOK(t)
    /\ IF cur = NoCur
         THEN UNCHANGED <<cur, groups>>
         ELSE /\ groups' = Append(groups,
                     [start |-> cur.start, end |-> t, lines |-> cur.lines])
              /\ cur' = NoCur
    /\ lastT' = t
    /\ nLines' = nLines + 1
    /\ UNCHANGED <<pc, dropped, stepEnd, spans, kids>>

\* Plain content line: inside a group it is collected; outside it is a
\* top-level line (not modeled beyond advancing time — see ABSTRACTIONS).
Plain(t) ==
    /\ InScan
    /\ TimeOK(t)
    /\ cur' = IF cur = NoCur THEN cur
              ELSE [cur EXCEPT !.lines = Append(@, t)]
    /\ lastT' = t
    /\ nLines' = nLines + 1
    /\ UNCHANGED <<pc, groups, dropped, stepEnd, spans, kids>>

LastOf(s) == s[Len(s)]

(***************************************************************************)
(* EOF / truncation at any point (timestamp.go:79-87): an unclosed group   *)
(* is closed at its last content-line time, or its own start if empty.     *)
(* stepEnd is the step window end handed to Parse by the caller; in the    *)
(* Monotonic (happy-path) config it is 0 or >= every log-line time.        *)
(***************************************************************************)
EndInput(se) ==
    /\ pc = "scan"
    /\ se \in {0} \cup {t \in Times : Monotonic => t >= lastT}
    /\ groups' = IF cur = NoCur THEN groups
                 ELSE Append(groups,
                        [start |-> cur.start,
                         end   |-> IF cur.lines = <<>> THEN cur.start
                                   ELSE LastOf(cur.lines),
                         lines |-> cur.lines])
    /\ cur' = NoCur
    /\ stepEnd' = se
    /\ pc' = "assign"
    /\ UNCHANGED <<lastT, nLines, dropped, spans, kids>>

(***************************************************************************)
(* parseGapBased (timestamp.go:226-310) as a pure fold: split on           *)
(* gap >= MinGap measured against current.end, which only advances         *)
(* FORWARD (263-268) — BugGap restores the pre-fix unconditional update    *)
(* that a backward timestamp drags backward.                               *)
(***************************************************************************)
RECURSIVE GapFoldR(_, _, _)
GapFoldR(rest, c, acc) ==
    IF rest = <<>> THEN Append(acc, c)
    ELSE IF Head(rest) - c.end >= MinGap
         THEN GapFoldR(Tail(rest),
                       [start |-> Head(rest), end |-> Head(rest)],
                       Append(acc, c))
         ELSE GapFoldR(Tail(rest),
                       [c EXCEPT !.end =
                           IF BugGap \/ Head(rest) > @ THEN Head(rest) ELSE @],
                       acc)

GapGroupsOf(times) ==
    IF times = <<>> THEN <<>>
    ELSE GapFoldR(Tail(times),
                  [start |-> times[1], end |-> times[1]],
                  <<>>)

(***************************************************************************)
(* Stable insertion sort by .start — sort.SliceStable at                   *)
(* timestamp.go:114-116 (in place, hence the groups' write-back below).    *)
(***************************************************************************)
RECURSIVE InsertByStart(_, _)
InsertByStart(s, g) ==
    IF s = <<>> THEN <<g>>
    ELSE IF g.start <= Head(s).start
         THEN <<g>> \o s
         ELSE <<Head(s)>> \o InsertByStart(Tail(s), g)

RECURSIVE SortByStart(_)
SortByStart(s) ==
    IF s = <<>> THEN <<>>
    ELSE InsertByStart(SortByStart(Tail(s)), Head(s))

(***************************************************************************)
(* parseWithGroups (timestamp.go:111-186), deterministic, one atomic step. *)
(* Groups are first sorted by start (114-116; BugSort skips).              *)
(* Group i's raw span end:                                                 *)
(*   groups[i+1].start if a next group exists            (125-126)         *)
(*   else stepEnd if stepEnd is not the zero time        (127-130)         *)
(*   else the group's own end from splitGroups.                            *)
(* The raw end is clamped (134-139; BugClamp skips): if it precedes the    *)
(* group start, fall back to g.end, then to g.start.                       *)
(* Bug = TRUE skips the override entirely (span end = raw g.end).          *)
(* Children: parseGapBased(g.lines, endTime) (154); a single gap group     *)
(* yields no children (273-275); child j's end is the next gap group's     *)
(* start, last child ends at the region end (280-286); children shorter    *)
(* than MinSpan are dropped (288-291) — BEFORE the clamp, matching code    *)
(* order; then clampSpans pins them into [g.start, endTime] (157, 205-222; *)
(* BugChild skips).                                                        *)
(***************************************************************************)
Produce ==
    /\ pc = "assign"
    /\ LET sg == IF BugSort THEN groups ELSE SortByStart(groups)
           n  == Len(sg)
           RawET(i) == IF Bug THEN sg[i].end
                       ELSE IF i < n THEN sg[i+1].start
                       ELSE IF stepEnd # 0 THEN stepEnd
                       ELSE sg[i].end
           ET(i) == IF BugClamp \/ RawET(i) >= sg[i].start THEN RawET(i)
                    ELSE IF sg[i].end >= sg[i].start THEN sg[i].end
                    ELSE sg[i].start
           ClampKid(k, lo, hi) ==
               LET s  == IF k.start < lo THEN lo
                         ELSE IF k.start > hi THEN hi
                         ELSE k.start
                   e0 == IF k.end > hi THEN hi ELSE k.end
               IN [start |-> s,
                   end   |-> IF e0 < s THEN s ELSE e0,
                   parent |-> k.parent]
           KidsOf(i) ==
               LET gg == GapGroupsOf(sg[i].lines)
                   m  == Len(gg)
                   KEnd(j) == IF j < m THEN gg[j+1].start ELSE ET(i)
                   raw == IF m <= 1 THEN {}
                          ELSE {[start |-> gg[j].start,
                                 end   |-> KEnd(j),
                                 parent |-> i] :
                                j \in {k \in 1..m :
                                          KEnd(k) - gg[k].start >= MinSpan}}
               IN IF BugChild THEN raw
                  ELSE {ClampKid(k, sg[i].start, ET(i)) : k \in raw}
       IN /\ groups' = sg
          /\ spans' = [i \in 1..n |->
                          [start |-> sg[i].start, end |-> ET(i)]]
          /\ kids' = UNION {KidsOf(i) : i \in 1..n}
    /\ pc' = "done"
    /\ UNCHANGED <<lastT, nLines, cur, dropped, stepEnd>>

Terminating == pc = "done" /\ UNCHANGED vars

Next ==
    \/ \E t \in Times : Open(t) \/ Close(t) \/ Plain(t)
    \/ \E se \in 0..MaxTime : EndInput(se)
    \/ Produce
    \/ Terminating

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* INVARIANTS                                                              *)
(***************************************************************************)

\* [proposed] Every group block reaching parseWithGroups has a real end
\* time.  Expected GREEN: splitGroups always sets end at append time
\* (nested open: 55; endgroup: 67; EOF: 81-85) — no zero-end group is
\* reachable, so the 123-130 override is NOT a zero-end repair, it fires
\* unconditionally.
Inv_GroupEndsSet ==
    \A i \in DOMAIN groups : groups[i].end \in Times

\* [proposed — fixed 2026-07-06] The ordering the end override relies on.
\* splitGroups appends in close order, so mid-scan the sequence may be
\* unsorted under non-monotonic input; the fix sorts at parseWithGroups
\* entry (timestamp.go:114-116, in place), so the property holds at the
\* point of use — checked at "done" on the written-back groups.
\* MCMutation2 (BugSort) proves the check still has teeth.
Inv_GroupsSortedByStart ==
    pc = "done" =>
        \A i \in 1..(Len(groups) - 1) : groups[i].start <= groups[i+1].start

\* [proposed — fixed 2026-07-06] Every produced span is well-formed:
\* start <= end.  Was FINDING 1 (end-override trusted stepEnd blindly);
\* fixed by the clamp at timestamp.go:134-139.
Inv_SpanStartLeEnd ==
    /\ \A i \in DOMAIN spans : spans[i].start <= spans[i].end
    /\ \A k \in kids : k.start <= k.end

\* [proposed — fixed 2026-07-06] Same as Inv_SpanStartLeEnd but restricted
\* to NON-LAST group spans, isolating the groups[i+1].start override path
\* (timestamp.go:125-126) from the stepEnd path (127-130): an inversion
\* here cannot be blamed on a bad caller-supplied step window.  Was
\* FINDING 4; fixed by sort (114-116) + clamp (134-139).
Inv_InteriorSpanStartLeEnd ==
    \A i \in 1..(Len(spans) - 1) : spans[i].start <= spans[i].end

\* [proposed — fixed 2026-07-06] Child spans lie inside their parent group
\* span (required for a well-formed OTel span tree).  Was FINDING 3; fixed
\* by clampSpans at timestamp.go:157 (205-222).
Inv_ChildInParent ==
    \A k \in kids : /\ spans[k.parent].start <= k.start
                    /\ k.end <= spans[k.parent].end

\* [code-documented] Group spans tile: each extends to the next group's
\* start, the last to stepEnd.  Comment timestamp.go:124 & 127-128; test
\* logparse_test.go:67-68 asserts the same next-start/stepEnd rule.
\* This is the property the Bug mutation (skip override) must break.
\* The clamp only RAISES ends, so tiling survives it.
Inv_GroupSpansTile ==
    (pc = "done" /\ Len(spans) > 0) =>
        /\ \A i \in 1..(Len(spans) - 1) : spans[i].end >= spans[i+1].start
        /\ (stepEnd # 0 => spans[Len(spans)].end >= stepEnd)

\* [proposed — fixed 2026-07-06] Nested ##[group] no longer discards the
\* outer group and its lines: splitGroups implicit-closes it
\* (timestamp.go:51-57).  dropped only increments under BugNest, so this
\* is exactly "no group was lost".  MCMutation5 proves the teeth.
Inv_NoNestedDrop == dropped = 0

\* BAIT (MCBait.cfg only): claims two group spans can never be produced.
\* Clearly reachable — TLC must fail this and print the witness.
BaitNoTwoGroupSpans == Len(spans) <= 1

=========================================================================
