package analyzer

import (
	"testing"
	"time"
)

// matrixJob builds a job named `name` with the standard three CI steps, so two
// matrix variants share identical child structure (the rename signal).
func matrixJob(name string, dur time.Duration) *TreeNode {
	return tn(name, "job", "success", dur,
		tn("Checkout", "step", "success", 10*time.Second),
		tn("bundle install", "step", "success", 1*time.Minute),
		tn("Run specs", "step", "success", dur-70*time.Second))
}

// TestRenameCollapsesMatrixShift: a matrix dimension bump (ruby 3.2 → 3.4) with
// identical child structure collapses to ONE Renamed node, not add + remove.
func TestRenameCollapsesMatrixShift(t *testing.T) {
	before := []*TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			matrixJob("test (ruby 3.2)", 8*time.Minute)),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			matrixJob("test (ruby 3.4)", 8*time.Minute)),
	}

	d := DiffTraces(before, after)

	if d.NumAdded != 0 || d.NumRemoved != 0 {
		t.Errorf("added=%d removed=%d, want both 0 (collapsed to rename)", d.NumAdded, d.NumRemoved)
	}
	if d.NumRenamed != 1 {
		t.Fatalf("NumRenamed=%d, want 1", d.NumRenamed)
	}

	r := find(d.Roots, "test (ruby 3.4)")
	if r == nil {
		t.Fatal("renamed node not found by after-name")
	}
	if r.Kind != Renamed {
		t.Errorf("Kind=%v, want Renamed", r.Kind)
	}
	if r.OldName != "test (ruby 3.2)" {
		t.Errorf("OldName=%q, want %q", r.OldName, "test (ruby 3.2)")
	}
	// The old name must NOT also appear as a separate Removed node.
	if old := find(d.Roots, "test (ruby 3.2)"); old != nil {
		t.Errorf("old name still present as a separate node (kind=%v)", old.Kind)
	}
}

// TestRenamePreservesInnerDelta: a renamed job still diffs its children, so a
// regression inside the renamed job surfaces (and as a real mover, not +/-).
func TestRenamePreservesInnerDelta(t *testing.T) {
	before := []*TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			matrixJob("test (ruby 3.2)", 8*time.Minute)), // Run specs ≈ 6m50s
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 11*time.Minute,
			matrixJob("test (ruby 3.4)", 11*time.Minute)), // Run specs ≈ 9m50s
	}

	d := DiffTraces(before, after)

	specs := find(d.Roots, "Run specs")
	if specs == nil || specs.Kind != Changed {
		t.Fatalf("Run specs should be Changed inside the rename, got %+v", specs)
	}
	if specs.DurDelta <= 0 {
		t.Errorf("Run specs DurDelta=%v, want positive regression", specs.DurDelta)
	}

	// Top movers must blame "Run specs", and must NOT contain a giant
	// add/remove pair for the relabeled job.
	movers := d.TopMovers(10)
	var sawSpecs bool
	for _, m := range movers {
		if m.Kind == Added || m.Kind == Removed {
			t.Errorf("mover %q is %v — matrix relabel leaked as add/remove", m.Name, m.Kind)
		}
		if m.Name == "Run specs" {
			sawSpecs = true
		}
	}
	if !sawSpecs {
		t.Error("Run specs regression missing from top movers")
	}
}

// TestRenamePureRelabelNotAMover: a relabel with no timing change carries no
// wall-clock delta, so it stays out of the movers list entirely.
func TestRenamePureRelabelNotAMover(t *testing.T) {
	before := []*TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			matrixJob("test (ruby 3.2)", 8*time.Minute)),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			matrixJob("test (ruby 3.4)", 8*time.Minute)),
	}

	d := DiffTraces(before, after)
	for _, m := range d.TopMovers(10) {
		t.Errorf("unexpected mover %q (%v, Δ=%v) for a pure relabel", m.Name, m.Kind, m.Delta)
	}
}

// TestRenameDoesNotMatchDissimilar: genuinely different jobs (different name,
// no shared children) must NOT be collapsed into a rename — stay add + remove.
func TestRenameDoesNotMatchDissimilar(t *testing.T) {
	before := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 5*time.Minute),
			tn("lint", "job", "success", 2*time.Minute)),
	}
	after := []*TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 5*time.Minute),
			tn("typecheck", "job", "success", 3*time.Minute)),
	}

	d := DiffTraces(before, after)
	if d.NumRenamed != 0 {
		t.Errorf("NumRenamed=%d, want 0 (lint vs typecheck are not a rename)", d.NumRenamed)
	}
	if find(d.Roots, "lint") == nil || find(d.Roots, "lint").Kind != Removed {
		t.Error("lint should remain Removed")
	}
	if find(d.Roots, "typecheck") == nil || find(d.Roots, "typecheck").Kind != Added {
		t.Error("typecheck should remain Added")
	}
}

func TestNameSimilarity(t *testing.T) {
	tests := []struct {
		a, b string
		min  float64 // similarity must be at least this
		max  float64 // ...and at most this
	}{
		{"test (ruby 3.2)", "test (ruby 3.4)", 0.8, 1.0}, // matrix bump: very similar
		{"test (ruby 3.2)", "test (ruby 3.2)", 1.0, 1.0}, // identical
		{"lint", "typecheck", 0.0, 0.2},                  // unrelated: dissimilar
	}
	for _, tt := range tests {
		got := nameSimilarity(tt.a, tt.b)
		if got < tt.min || got > tt.max {
			t.Errorf("nameSimilarity(%q,%q)=%.2f, want in [%.2f,%.2f]", tt.a, tt.b, got, tt.min, tt.max)
		}
	}
}
