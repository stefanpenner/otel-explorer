package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
)

// epoch is a fixed base time so test traces are deterministic.
var epoch = time.Unix(1700000000, 0)

// tn builds a TreeNode starting at epoch+offset, lasting dur, with children.
// Children keep their own absolute times; the parent's window is taken as-is
// (callers size it to cover children when wall time matters).
func tn(name, category, outcome string, dur time.Duration, children ...*TreeNode) *TreeNode {
	return tnAt(name, category, outcome, 0, dur, children...)
}

func tnAt(name, category, outcome string, offset, dur time.Duration, children ...*TreeNode) *TreeNode {
	start := epoch.Add(offset)
	return &TreeNode{
		Name:      name,
		StartTime: start,
		EndTime:   start.Add(dur),
		Hints:     enrichment.SpanHints{Category: category, Outcome: outcome},
		Children:  children,
	}
}

// find walks a diff tree returning the first DiffNode whose Name matches.
func find(nodes []*DiffNode, name string) *DiffNode {
	for _, n := range nodes {
		if n.Name == name {
			return n
		}
		if got := find(n.Children, name); got != nil {
			return got
		}
	}
	return nil
}

func TestDiffIdenticalTracesAllUnchanged(t *testing.T) {
	t.Parallel()
	before := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 5*time.Minute,
				tn("compile", "step", "success", 5*time.Minute)),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 5*time.Minute,
				tn("compile", "step", "success", 5*time.Minute)),
		),
	}

	d := DiffTraces(before, after)

	if d.NumAdded != 0 || d.NumRemoved != 0 || d.NumChanged != 0 {
		t.Fatalf("identical traces: added=%d removed=%d changed=%d, want 0/0/0",
			d.NumAdded, d.NumRemoved, d.NumChanged)
	}
	if d.WallDelta() != 0 {
		t.Errorf("WallDelta = %v, want 0", d.WallDelta())
	}
	ci := find(d.Roots, "CI")
	if ci == nil || ci.Kind != Unchanged {
		t.Fatalf("CI node kind = %v, want Unchanged", ci.Kind)
	}
	if len(d.TopMovers(10)) != 0 {
		t.Errorf("identical traces should have no movers, got %d", len(d.TopMovers(10)))
	}
}

func TestDiffStepGotSlower(t *testing.T) {
	t.Parallel()
	before := []*TreeNode{
		tn("CI", "workflow", "success", 5*time.Minute,
			tn("test", "job", "success", 5*time.Minute,
				tn("run specs", "step", "success", 5*time.Minute)),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			tn("test", "job", "success", 8*time.Minute,
				tn("run specs", "step", "success", 8*time.Minute)),
		),
	}

	d := DiffTraces(before, after)

	step := find(d.Roots, "run specs")
	if step == nil {
		t.Fatal("run specs not found in diff")
	}
	if step.Kind != Changed {
		t.Errorf("step kind = %v, want Changed", step.Kind)
	}
	if step.DurDelta != 3*time.Minute {
		t.Errorf("step DurDelta = %v, want 3m", step.DurDelta)
	}
	if !step.Regressed() {
		t.Error("a slower step should be Regressed()")
	}
	// The parent job and workflow are Changed because a descendant changed.
	if job := find(d.Roots, "test"); job.Kind != Changed {
		t.Errorf("parent job kind = %v, want Changed (descendant changed)", job.Kind)
	}
	if d.WallDelta() != 3*time.Minute {
		t.Errorf("WallDelta = %v, want 3m", d.WallDelta())
	}
	// The step is the single leaf mover.
	movers := d.TopMovers(10)
	if len(movers) != 1 || movers[0].Name != "run specs" {
		t.Fatalf("TopMovers = %+v, want [run specs]", moverNames(movers))
	}
}

func TestDiffJobAddedAndRemoved(t *testing.T) {
	t.Parallel()
	before := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 5*time.Minute),
			tn("lint", "job", "success", 2*time.Minute),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 5*time.Minute),
			tn("typecheck", "job", "success", 3*time.Minute),
		),
	}

	d := DiffTraces(before, after)

	if added := find(d.Roots, "typecheck"); added == nil || added.Kind != Added {
		t.Errorf("typecheck kind = %v, want Added", kindOf(added))
	}
	if removed := find(d.Roots, "lint"); removed == nil || removed.Kind != Removed {
		t.Errorf("lint kind = %v, want Removed", kindOf(removed))
	}
	if d.NumAdded != 1 || d.NumRemoved != 1 {
		t.Errorf("counts added=%d removed=%d, want 1/1", d.NumAdded, d.NumRemoved)
	}
	// build is identical → Unchanged.
	if build := find(d.Roots, "build"); build.Kind != Unchanged {
		t.Errorf("build kind = %v, want Unchanged", build.Kind)
	}
}

func TestDiffStatusFlipSuccessToFailure(t *testing.T) {
	t.Parallel()
	before := []*TreeNode{
		tn("CI", "workflow", "success", 5*time.Minute,
			tn("test", "job", "success", 5*time.Minute)),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "failure", 5*time.Minute,
			tn("test", "job", "failure", 5*time.Minute)),
	}

	d := DiffTraces(before, after)

	job := find(d.Roots, "test")
	if !job.StatusChanged() {
		t.Error("test job should report StatusChanged")
	}
	if job.OutcomeBefore != "success" || job.OutcomeAfter != "failure" {
		t.Errorf("outcomes = %s→%s, want success→failure", job.OutcomeBefore, job.OutcomeAfter)
	}
	// A status flip is significant even with identical durations.
	if job.Kind != Changed {
		t.Errorf("status-flipped job kind = %v, want Changed", job.Kind)
	}
	flips := d.StatusFlips()
	// Both the workflow and the job flipped.
	if len(flips) != 2 {
		t.Fatalf("StatusFlips len = %d, want 2", len(flips))
	}
}

func TestDiffMatrixJobShift(t *testing.T) {
	t.Parallel()
	// A matrix dimension moved: ruby 3.2 dropped, ruby 3.4 added.
	before := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("test (ruby 3.2)", "job", "success", 6*time.Minute),
			tn("test (ruby 3.3)", "job", "success", 6*time.Minute),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("test (ruby 3.3)", "job", "success", 6*time.Minute),
			tn("test (ruby 3.4)", "job", "success", 7*time.Minute),
		),
	}

	d := DiffTraces(before, after)

	if n := find(d.Roots, "test (ruby 3.2)"); kindOf(n) != Removed {
		t.Errorf("ruby 3.2 kind = %v, want Removed", kindOf(n))
	}
	if n := find(d.Roots, "test (ruby 3.4)"); kindOf(n) != Added {
		t.Errorf("ruby 3.4 kind = %v, want Added", kindOf(n))
	}
	if n := find(d.Roots, "test (ruby 3.3)"); kindOf(n) != Unchanged {
		t.Errorf("ruby 3.3 kind = %v, want Unchanged", kindOf(n))
	}
}

func TestDiffDuplicateSiblingNames(t *testing.T) {
	t.Parallel()
	// Two steps share a name; a third copy appears in "after". Positional
	// disambiguation matches the first two and treats the third as Added.
	before := []*TreeNode{
		tn("CI", "workflow", "success", 6*time.Minute,
			tn("job", "job", "success", 6*time.Minute,
				tn("retry", "step", "success", 2*time.Minute),
				tn("retry", "step", "success", 2*time.Minute),
			),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 6*time.Minute,
			tn("job", "job", "success", 6*time.Minute,
				tn("retry", "step", "success", 2*time.Minute),
				tn("retry", "step", "success", 2*time.Minute),
				tn("retry", "step", "success", 2*time.Minute),
			),
		),
	}

	d := DiffTraces(before, after)
	job := find(d.Roots, "job")
	retries := 0
	added := 0
	for _, c := range job.Children {
		if c.Name != "retry" {
			continue
		}
		retries++
		if c.Kind == Added {
			added++
		}
	}
	if retries != 3 {
		t.Fatalf("expected 3 retry nodes in diff, got %d", retries)
	}
	if added != 1 {
		t.Errorf("expected exactly 1 added retry, got %d", added)
	}
}

func TestDiffInsignificantDeltaStaysUnchanged(t *testing.T) {
	t.Parallel()
	// A 1-second wobble on a 5-minute step is noise, not a change.
	before := []*TreeNode{
		tn("CI", "workflow", "success", 5*time.Minute,
			tn("test", "job", "success", 5*time.Minute,
				tn("specs", "step", "success", 5*time.Minute)),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 5*time.Minute+time.Second,
			tn("test", "job", "success", 5*time.Minute+time.Second,
				tn("specs", "step", "success", 5*time.Minute+time.Second)),
		),
	}

	d := DiffTraces(before, after)
	if step := find(d.Roots, "specs"); step.Kind != Unchanged {
		t.Errorf("1s wobble: step kind = %v, want Unchanged", step.Kind)
	}
	if d.NumChanged != 0 {
		t.Errorf("NumChanged = %d, want 0 (noise filtered)", d.NumChanged)
	}
}

func TestTopMoversSurfacesOwnTimeRegression(t *testing.T) {
	t.Parallel()
	// The job's wall time exploded +20m, but its only step actually got 5m
	// FASTER. The regression lives entirely in the job's own time (queue /
	// provisioning / teardown). The mover list must surface that own-time
	// regression, not just the improving child — otherwise the single reported
	// "top mover" points the wrong way.
	before := []*TreeNode{
		tnAt("CI", "workflow", "success", 0, 10*time.Minute,
			tnAt("test", "job", "success", 0, 10*time.Minute,
				tnAt("specs", "step", "success", 0, 8*time.Minute))),
	}
	after := []*TreeNode{
		tnAt("CI", "workflow", "success", 0, 30*time.Minute,
			tnAt("test", "job", "success", 0, 30*time.Minute,
				tnAt("specs", "step", "success", 0, 3*time.Minute))),
	}

	d := DiffTraces(before, after)
	movers := d.TopMovers(10)

	var ownTime *Mover
	var stepImproved bool
	for i := range movers {
		if movers[i].SelfOnly && movers[i].Name == "test" {
			ownTime = &movers[i]
		}
		if movers[i].Name == "specs" && movers[i].Delta < 0 {
			stepImproved = true
		}
	}
	if ownTime == nil {
		t.Fatalf("expected an own-time mover for 'test'; got %v", moverNames(movers))
	}
	// job dur +20m, step -5m → own time grew by 25m.
	if ownTime.Delta != 25*time.Minute {
		t.Errorf("own-time delta = %v, want 25m", ownTime.Delta)
	}
	if !stepImproved {
		t.Error("the improving step should still be reported")
	}
	// The dominant mover must be the +25m regression, not the -5m improvement.
	if movers[0].Name != "test" || !movers[0].SelfOnly {
		t.Errorf("top mover = %q (selfOnly=%v), want test own-time", movers[0].Name, movers[0].SelfOnly)
	}
}

func TestDiffBigAbsoluteRegressionOnLongJobIsSignificant(t *testing.T) {
	t.Parallel()
	// +3m on a 30m job is only 10% — below the proportional gate — but three
	// minutes of wall clock is worth surfacing regardless.
	before := []*TreeNode{
		tn("CI", "workflow", "success", 30*time.Minute,
			tn("test", "job", "success", 30*time.Minute,
				tn("run specs", "step", "success", 30*time.Minute))),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 33*time.Minute,
			tn("test", "job", "success", 33*time.Minute,
				tn("run specs", "step", "success", 33*time.Minute))),
	}

	d := DiffTraces(before, after)
	step := find(d.Roots, "run specs")
	if step.Kind != Changed {
		t.Errorf("+3m on 30m step: kind = %v, want Changed (big absolute swing)", step.Kind)
	}
	if movers := d.TopMovers(5); len(movers) != 1 || movers[0].Name != "run specs" {
		t.Errorf("TopMovers = %v, want [run specs]", moverNames(movers))
	}
}

func TestDiffAttributionPrefersResponsibleLeaf(t *testing.T) {
	t.Parallel()
	// The job got 4m slower, but only one of its steps is responsible. The
	// top mover should be that leaf, not the parent job (which is a conduit).
	before := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("test", "job", "success", 10*time.Minute,
				tn("setup", "step", "success", 2*time.Minute),
				tn("specs", "step", "success", 8*time.Minute)),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 14*time.Minute,
			tn("test", "job", "success", 14*time.Minute,
				tn("setup", "step", "success", 2*time.Minute),
				tn("specs", "step", "success", 12*time.Minute)),
		),
	}

	d := DiffTraces(before, after)
	movers := d.TopMovers(5)
	if len(movers) == 0 {
		t.Fatal("expected at least one mover")
	}
	if movers[0].Name != "specs" {
		t.Errorf("top mover = %q, want \"specs\" (responsible leaf, not parent)", movers[0].Name)
	}
	if movers[0].DurDelta != 4*time.Minute {
		t.Errorf("specs DurDelta = %v, want 4m", movers[0].DurDelta)
	}
}

func TestFlattenHideUnchangedKeepsChangedAncestors(t *testing.T) {
	t.Parallel()
	before := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 4*time.Minute,
				tn("compile", "step", "success", 4*time.Minute)),
			tn("test", "job", "success", 6*time.Minute,
				tn("specs", "step", "success", 6*time.Minute)),
		),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 13*time.Minute,
			tn("build", "job", "success", 4*time.Minute,
				tn("compile", "step", "success", 4*time.Minute)),
			tn("test", "job", "success", 9*time.Minute,
				tn("specs", "step", "success", 9*time.Minute)),
		),
	}

	d := DiffTraces(before, after)

	// Full flatten: every node present (CI, build, compile, test, specs).
	if full := d.Flatten(false); len(full) != 5 {
		t.Fatalf("Flatten(false) len = %d, want 5", len(full))
	}

	// Hiding unchanged drops the build/compile branch entirely but keeps the
	// changed CI→test→specs chain.
	pruned := d.Flatten(true)
	names := map[string]bool{}
	for _, f := range pruned {
		names[f.Node.Name] = true
	}
	if names["build"] || names["compile"] {
		t.Errorf("hideUnchanged kept the unchanged build branch: %v", names)
	}
	for _, want := range []string{"CI", "test", "specs"} {
		if !names[want] {
			t.Errorf("hideUnchanged dropped changed node %q: %v", want, names)
		}
	}
}

func TestDiffEmptyForests(t *testing.T) {
	t.Parallel()
	full := []*TreeNode{
		tn("CI", "workflow", "success", 5*time.Minute,
			tn("test", "job", "success", 5*time.Minute)),
	}

	// before empty → everything added.
	d := DiffTraces(nil, full)
	if d.NumAdded != 1 || d.NumRemoved != 0 {
		t.Errorf("nil→full: added=%d removed=%d, want 1/0", d.NumAdded, d.NumRemoved)
	}
	if d.WallBefore != 0 || d.WallAfter != 5*time.Minute {
		t.Errorf("nil→full wall: %v→%v, want 0→5m", d.WallBefore, d.WallAfter)
	}
	if ci := find(d.Roots, "CI"); kindOf(ci) != Added {
		t.Errorf("CI kind = %v, want Added", kindOf(ci))
	}

	// after empty → everything removed.
	d = DiffTraces(full, nil)
	if d.NumRemoved != 1 || d.NumAdded != 0 {
		t.Errorf("full→nil: added=%d removed=%d, want 0/1", d.NumAdded, d.NumRemoved)
	}

	// both empty → nothing.
	d = DiffTraces(nil, nil)
	if d.HasChanges() {
		t.Errorf("nil→nil should have no changes, got %+v", d)
	}
	if len(d.TopMovers(5)) != 0 || len(d.StatusFlips()) != 0 {
		t.Error("nil→nil should yield no movers or flips")
	}
}

func TestForestWallIgnoresZeroTimes(t *testing.T) {
	t.Parallel()
	// A child whose StartTime was never recorded (time.Time{} = year 1) must
	// not poison the forest's wall clock into a ~2000-year span.
	parent := tnAt("CI", "workflow", "success", 0, 10*time.Minute)
	parent.Children = []*TreeNode{{Name: "queued", StartTime: time.Time{}, EndTime: epoch.Add(time.Minute)}}
	if wall := forestWall([]*TreeNode{parent}); wall != 10*time.Minute {
		t.Errorf("forestWall with zero-start child = %v, want 10m", wall)
	}

	// A node with a zero EndTime must not produce a negative wall.
	roots := []*TreeNode{{Name: "x", StartTime: epoch, EndTime: time.Time{}}}
	if wall := forestWall(roots); wall < 0 {
		t.Errorf("forestWall with zero-end node = %v, want >= 0", wall)
	}
}

// --- helpers for assertions ---

func kindOf(n *DiffNode) ChangeKind {
	if n == nil {
		return -1
	}
	return n.Kind
}

func moverNames(ns []Mover) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.Name
	}
	return out
}
