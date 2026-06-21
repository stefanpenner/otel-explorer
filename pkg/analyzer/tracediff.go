package analyzer

import (
	"sort"
	"strconv"
	"time"
)

// ChangeKind classifies how a node differs between the "before" and "after"
// trace, the way `git diff` classifies a line or file.
type ChangeKind int

const (
	// Unchanged: present in both traces, within noise on every axis, and no
	// descendant changed.
	Unchanged ChangeKind = iota
	// Added: present only in "after" (a new job/step appeared).
	Added
	// Removed: present only in "before" (a job/step went away).
	Removed
	// Changed: present in both, but its duration or status moved materially,
	// or a descendant changed.
	Changed
)

func (k ChangeKind) String() string {
	switch k {
	case Added:
		return "added"
	case Removed:
		return "removed"
	case Changed:
		return "changed"
	default:
		return "unchanged"
	}
}

// DiffOptions tunes what counts as a material duration change. A change must
// clear BOTH thresholds: enough wall-clock to matter and enough proportionally
// to not be a busy-runner wobble. Status flips ignore these — a pass→fail is
// always material.
type DiffOptions struct {
	MinDeltaPct float64       // proportional floor, e.g. 20 (%)
	MinDeltaAbs time.Duration // absolute floor, e.g. 2s — a change must clear both this AND MinDeltaPct
	BigDeltaAbs time.Duration // ...unless the absolute change is at least this, which is material regardless of percentage (so a +3m regression on a 30m job is not hidden by the 20% gate)
}

// DefaultDiffOptions are tuned for CI traces, where sub-second jitter is
// constant and proportional drift below ~20% is rarely actionable — but a
// multi-minute swing always is, however large the job.
func DefaultDiffOptions() DiffOptions {
	return DiffOptions{MinDeltaPct: 20, MinDeltaAbs: 2 * time.Second, BigDeltaAbs: 2 * time.Minute}
}

// DiffNode is one node in the diff tree, pairing a "before" span with its
// matched "after" span (either may be nil at the boundary of an add/remove).
type DiffNode struct {
	Kind     ChangeKind
	Name     string
	Category string // workflow / job / step / marker ...

	Before *TreeNode // nil when Added
	After  *TreeNode // nil when Removed

	Children []*DiffNode

	DurBefore time.Duration
	DurAfter  time.Duration
	DurDelta  time.Duration // After - Before (0 on the missing side)

	OutcomeBefore string
	OutcomeAfter  string
}

// DeltaPct is the duration change as a percentage of the "before" duration.
// Returns 0 when there is no "before" to compare against (Added nodes).
func (d *DiffNode) DeltaPct() float64 {
	if d.DurBefore <= 0 {
		return 0
	}
	return float64(d.DurDelta) / float64(d.DurBefore) * 100
}

// StatusChanged reports a pass/fail/skip transition. Empty outcomes (markers,
// structural spans) never count as a flip.
func (d *DiffNode) StatusChanged() bool {
	if d.Before == nil || d.After == nil {
		return false
	}
	if d.OutcomeBefore == "" || d.OutcomeAfter == "" {
		return false
	}
	return d.OutcomeBefore != d.OutcomeAfter
}

// SelfDelta is the change in this node's own time — its duration minus the
// time covered by its children's envelope. It isolates a node's own scheduling
// and overhead (queueing, runner provisioning, checkout, teardown) from work
// that happened inside its children. Zero unless the node is matched on both
// sides.
func (d *DiffNode) SelfDelta() time.Duration {
	if d.Before == nil || d.After == nil {
		return 0
	}
	return selfTime(d.After) - selfTime(d.Before)
}

// Regressed reports a matched node that got materially slower.
func (d *DiffNode) Regressed() bool { return d.Kind == Changed && d.DurDelta > 0 }

// Improved reports a matched node that got materially faster.
func (d *DiffNode) Improved() bool { return d.Kind == Changed && d.DurDelta < 0 }

// TraceDiff is the semantic delta between two traces.
type TraceDiff struct {
	Roots []*DiffNode

	WallBefore time.Duration
	WallAfter  time.Duration

	NumAdded   int // count of added subtrees (a new job counts once, not per step)
	NumRemoved int
	NumChanged int // count of nodes whose own duration/status moved

	opts DiffOptions
}

// WallDelta is the end-to-end wall-clock change (after - before).
func (t *TraceDiff) WallDelta() time.Duration { return t.WallAfter - t.WallBefore }

// DiffTraces computes the semantic delta between two TreeNode forests using
// the default thresholds. "before" and "after" are forests of root spans
// (typically one workflow each, but any forest is supported).
func DiffTraces(before, after []*TreeNode) *TraceDiff {
	return DiffTracesWithOptions(before, after, DefaultDiffOptions())
}

// DiffTracesWithOptions is DiffTraces with caller-controlled thresholds.
func DiffTracesWithOptions(before, after []*TreeNode, opts DiffOptions) *TraceDiff {
	b := &diffBuilder{opts: opts}
	roots := b.diffLevel(before, after)
	return &TraceDiff{
		Roots:      roots,
		WallBefore: forestWall(before),
		WallAfter:  forestWall(after),
		NumAdded:   b.numAdded,
		NumRemoved: b.numRemoved,
		NumChanged: b.numChanged,
		opts:       opts,
	}
}

type diffBuilder struct {
	opts       DiffOptions
	numAdded   int
	numRemoved int
	numChanged int
}

// diffLevel matches one level of siblings between two forests and recurses.
// Output order: matched and added nodes in "after" order, then removed nodes
// in "before" order. Renderers that care about impact re-sort anyway; the tree
// view keeps removed clearly grouped at the tail of each level.
func (b *diffBuilder) diffLevel(before, after []*TreeNode) []*DiffNode {
	if len(before) == 0 && len(after) == 0 {
		return nil
	}

	beforeKeys := siblingKeys(before)
	afterKeys := siblingKeys(after)

	beforeByKey := make(map[string]*TreeNode, len(before))
	for i, n := range before {
		beforeByKey[beforeKeys[i]] = n
	}
	matchedBefore := make(map[string]bool, len(before))

	var out []*DiffNode
	for i, an := range after {
		key := afterKeys[i]
		if bn, ok := beforeByKey[key]; ok {
			matchedBefore[key] = true
			out = append(out, b.matched(bn, an))
		} else {
			b.numAdded++
			out = append(out, buildSubtree(an, Added))
		}
	}
	for i, bn := range before {
		if matchedBefore[beforeKeys[i]] {
			continue
		}
		b.numRemoved++
		out = append(out, buildSubtree(bn, Removed))
	}
	return out
}

func (b *diffBuilder) matched(bn, an *TreeNode) *DiffNode {
	d := &DiffNode{
		Name:          an.Name,
		Category:      an.Hints.Category,
		Before:        bn,
		After:         an,
		DurBefore:     bn.Duration(),
		DurAfter:      an.Duration(),
		OutcomeBefore: bn.Hints.Outcome,
		OutcomeAfter:  an.Hints.Outcome,
	}
	d.DurDelta = d.DurAfter - d.DurBefore
	d.Children = b.diffLevel(bn.Children, an.Children)

	ownChanged := d.StatusChanged() || b.significant(d.DurDelta, d.DurBefore)
	childChanged := false
	for _, c := range d.Children {
		if c.Kind != Unchanged {
			childChanged = true
			break
		}
	}
	if ownChanged || childChanged {
		d.Kind = Changed
	} else {
		d.Kind = Unchanged
	}
	if ownChanged {
		b.numChanged++
	}
	return d
}

// significant reports whether a duration delta is material: it must clear the
// absolute floor, and then either be large in absolute terms (BigDeltaAbs) or
// clear the proportional floor.
func (b *diffBuilder) significant(delta, base time.Duration) bool {
	a := absDur(delta)
	if a < b.opts.MinDeltaAbs {
		return false
	}
	if b.opts.BigDeltaAbs > 0 && a >= b.opts.BigDeltaAbs {
		return true // a multi-minute swing matters however large the job
	}
	if base <= 0 {
		return true // appeared/grew from nothing measurable
	}
	return float64(a)/float64(base)*100 >= b.opts.MinDeltaPct
}

// buildSubtree turns an unmatched node (and all descendants) into a diff
// subtree of a single kind (Added or Removed).
func buildSubtree(n *TreeNode, kind ChangeKind) *DiffNode {
	d := &DiffNode{
		Kind:     kind,
		Name:     n.Name,
		Category: n.Hints.Category,
	}
	if kind == Added {
		d.After = n
		d.DurAfter = n.Duration()
		d.OutcomeAfter = n.Hints.Outcome
	} else {
		d.Before = n
		d.DurBefore = n.Duration()
		d.OutcomeBefore = n.Hints.Outcome
	}
	for _, c := range n.Children {
		d.Children = append(d.Children, buildSubtree(c, kind))
	}
	return d
}

// siblingKeys assigns each node a key that is stable across traces: its
// category and name, disambiguated by occurrence index among same-named
// siblings (so two "Run tests" steps match positionally rather than collapsing
// into one).
func siblingKeys(nodes []*TreeNode) []string {
	seen := make(map[string]int, len(nodes))
	keys := make([]string, len(nodes))
	for i, n := range nodes {
		base := n.Hints.Category + "\x00" + n.Name
		occ := seen[base]
		seen[base]++
		keys[i] = base + "\x00#" + strconv.Itoa(occ)
	}
	return keys
}

// FlatDiffNode is a diff node paired with its depth in the tree, for flat
// (row-oriented) rendering.
type FlatDiffNode struct {
	Node  *DiffNode
	Depth int
}

// Flatten walks the diff tree depth-first into rows. When hideUnchanged is
// true, unchanged subtrees are pruned — and because a node is only Unchanged
// when nothing in its subtree changed, this keeps every changed node together
// with its changed ancestors as context, the way `git diff` keeps surrounding
// structure but drops untouched regions.
func (t *TraceDiff) Flatten(hideUnchanged bool) []FlatDiffNode {
	var out []FlatDiffNode
	var walk func(ns []*DiffNode, depth int)
	walk = func(ns []*DiffNode, depth int) {
		for _, n := range ns {
			if hideUnchanged && n.Kind == Unchanged {
				continue
			}
			out = append(out, FlatDiffNode{Node: n, Depth: depth})
			walk(n.Children, depth+1)
		}
	}
	walk(t.Roots, 0)
	return out
}

// HasChanges reports whether the two traces differ at all.
func (t *TraceDiff) HasChanges() bool {
	return t.NumAdded > 0 || t.NumRemoved > 0 || t.NumChanged > 0
}

// StatusFlips returns every node that changed pass/fail/skip status, in
// depth-first order.
func (t *TraceDiff) StatusFlips() []*DiffNode {
	var out []*DiffNode
	var walk func(ns []*DiffNode)
	walk = func(ns []*DiffNode) {
		for _, n := range ns {
			if n.StatusChanged() {
				out = append(out, n)
			}
			walk(n.Children)
		}
	}
	walk(t.Roots)
	return out
}

// Mover is one entry in the attributed wall-clock change. It embeds the diff
// node and carries the signed duration attributed to it (Delta) and whether
// that attribution is the node's own time rather than its whole subtree.
type Mover struct {
	*DiffNode
	SelfOnly bool          // the change is in this node's own scheduling/overhead, not its children
	Delta    time.Duration // signed wall-clock change attributed to this mover
}

// TopMovers attributes the wall-clock change to the smallest set of
// responsible nodes, largest magnitude first, capped at n.
//
// A mover is:
//   - an added subtree (the whole new job/step, counted once), or
//   - a removed subtree, or
//   - a changed leaf (a step, or a job with no recorded steps), or
//   - a changed node whose children are all unchanged — the change is in the
//     node's own time, or
//   - a changed node whose own time (SelfOnly) moved materially even though a
//     child also changed — its scheduling/overhead is reported alongside the
//     responsible child, so a job's own queue/provisioning growth is not lost.
//
// A changed node that merely passes a child's regression upward, with no
// own-time change of its own, is skipped in favour of the responsible child —
// so the list reads like a blame, not an echo.
func (t *TraceDiff) TopMovers(n int) []Mover {
	var movers []Mover
	var walk func(ns []*DiffNode)
	walk = func(ns []*DiffNode) {
		for _, d := range ns {
			switch d.Kind {
			case Added:
				movers = append(movers, Mover{DiffNode: d, Delta: d.DurAfter})
			case Removed:
				movers = append(movers, Mover{DiffNode: d, Delta: -d.DurBefore})
			case Changed:
				if len(d.Children) == 0 || !anyChildChanged(d.Children) {
					// Movers are about wall clock; a node that changed only in
					// status (no material duration delta) is reported in the
					// StatusFlips section, not here.
					if t.isSignificant(d.DurDelta, d.DurBefore) {
						movers = append(movers, Mover{DiffNode: d, Delta: d.DurDelta})
					}
					continue
				}
				// A child changed: blame the responsible children, and also
				// surface this node's own-time change when it is material.
				if sd := d.SelfDelta(); t.isSignificant(sd, selfTime(d.Before)) {
					movers = append(movers, Mover{DiffNode: d, SelfOnly: true, Delta: sd})
				}
				walk(d.Children)
			}
		}
	}
	walk(t.Roots)

	sort.SliceStable(movers, func(i, j int) bool {
		return absDur(movers[i].Delta) > absDur(movers[j].Delta)
	})
	if n >= 0 && len(movers) > n {
		movers = movers[:n]
	}
	return movers
}

func anyChildChanged(ns []*DiffNode) bool {
	for _, c := range ns {
		if c.Kind != Unchanged {
			return true
		}
	}
	return false
}

// isSignificant applies the diff's thresholds outside the build phase (e.g. to
// a node's self-time delta).
func (t *TraceDiff) isSignificant(delta, base time.Duration) bool {
	a := absDur(delta)
	if a < t.opts.MinDeltaAbs {
		return false
	}
	if t.opts.BigDeltaAbs > 0 && a >= t.opts.BigDeltaAbs {
		return true
	}
	if base <= 0 {
		return true
	}
	return float64(a)/float64(base)*100 >= t.opts.MinDeltaPct
}

// coveredSpan is the time envelope of a node's children (latest child end minus
// earliest child start), ignoring zero timestamps. Zero when there are no
// children.
func coveredSpan(n *TreeNode) time.Duration {
	var earliest, latest time.Time
	for _, c := range n.Children {
		if !c.StartTime.IsZero() && (earliest.IsZero() || c.StartTime.Before(earliest)) {
			earliest = c.StartTime
		}
		if !c.EndTime.IsZero() && (latest.IsZero() || c.EndTime.After(latest)) {
			latest = c.EndTime
		}
	}
	if earliest.IsZero() || latest.IsZero() || latest.Before(earliest) {
		return 0
	}
	return latest.Sub(earliest)
}

// selfTime is a node's own time: its duration minus the span its children
// cover. Clamped to >= 0.
func selfTime(n *TreeNode) time.Duration {
	if n == nil {
		return 0
	}
	s := n.Duration() - coveredSpan(n)
	if s < 0 {
		return 0
	}
	return s
}

// RootsForURLIndex returns the roots belonging to one input trace, identified
// by the URLIndex stamped on each span. Combined multi-trace span sets carry
// both sides of a diff; this peels off one side.
func RootsForURLIndex(roots []*TreeNode, idx int) []*TreeNode {
	var out []*TreeNode
	for _, r := range roots {
		if r.URLIndex == idx {
			out = append(out, r)
		}
	}
	return out
}

// forestWall is the end-to-end span of a forest: latest end minus earliest
// start across every node. Zero timestamps (a span whose start/end was never
// recorded — year 1) are ignored so they cannot poison the window into an
// absurd multi-century span.
func forestWall(roots []*TreeNode) time.Duration {
	var earliest, latest time.Time
	var visit func(n *TreeNode)
	visit = func(n *TreeNode) {
		if !n.StartTime.IsZero() && (earliest.IsZero() || n.StartTime.Before(earliest)) {
			earliest = n.StartTime
		}
		if !n.EndTime.IsZero() && (latest.IsZero() || n.EndTime.After(latest)) {
			latest = n.EndTime
		}
		for _, c := range n.Children {
			visit(c)
		}
	}
	for _, r := range roots {
		visit(r)
	}
	if earliest.IsZero() || latest.IsZero() || latest.Before(earliest) {
		return 0
	}
	return latest.Sub(earliest)
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
