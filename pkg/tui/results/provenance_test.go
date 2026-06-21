package results

import (
	"regexp"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
)

// colorBefore returns the truecolor SGR escape immediately preceding the given
// substring in s (empty if the substring isn't color-wrapped).
func colorBefore(s, sub string) string {
	re := regexp.MustCompile(`\x1b\[(38;2;[0-9;]+)m` + regexp.QuoteMeta(sub))
	if m := re.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

// TestSourceHintColoredRender verifies the boundary label is color-coded on an
// active row, that distinct sources get distinct accents, and that a row with no
// boundary renders no hint at all.
func TestSourceHintColoredRender(t *testing.T) {
	lipgloss.SetColorProfile(termenv.TrueColor)
	defer lipgloss.SetColorProfile(termenv.Ascii)

	m := Model{treeWidth: 120, expandedState: map[string]bool{}}

	runnerRow := m.renderItem(TreeItem{ID: "x", DisplayName: "build", SourceHint: "runner"}, false, 0)
	toolRow := m.renderItem(TreeItem{ID: "y", DisplayName: "unit tests", SourceHint: "jest"}, false, 0)

	runnerColor := colorBefore(runnerRow, " ← runner")
	toolColor := colorBefore(toolRow, " ← jest")
	if runnerColor == "" {
		t.Errorf("runner label not color-wrapped; row=%q", runnerRow)
	}
	if toolColor == "" {
		t.Errorf("tool label not color-wrapped; row=%q", toolRow)
	}
	if runnerColor == toolColor {
		t.Errorf("runner and tool should use distinct accents, both %q", runnerColor)
	}

	plain := m.renderItem(TreeItem{ID: "z", DisplayName: "Set up job"}, false, 0)
	if strings.Contains(plain, "←") {
		t.Errorf("non-boundary row should have no hint: %q", plain)
	}
}

// TestProvenanceBoundaryHint verifies a node is tagged with a SourceHint only at
// a provenance boundary (where its emitter differs from its parent's), and that
// the canonical Name/DisplayName stay clean (no hint baked in — lookups/focus/
// search depend on that).
func TestProvenanceBoundaryHint(t *testing.T) {
	runner := map[string]string{"source": "runner"}
	tool := &analyzer.TreeNode{Name: "unit tests", Hints: enrichment.SpanHints{ServiceName: "jest", Category: "operation"}}
	step := &analyzer.TreeNode{Name: "Run tests", Attrs: runner, ScopeName: "github.actions.runner", Hints: enrichment.SpanHints{Category: "step"}, Children: []*analyzer.TreeNode{tool}}
	job := &analyzer.TreeNode{Name: "build", Attrs: runner, ScopeName: "github.actions.runner", Hints: enrichment.SpanHints{Category: "job"}, Children: []*analyzer.TreeNode{step}}
	wf := &analyzer.TreeNode{Name: "CI", ScopeName: "github.com/stefanpenner/otel-explorer/pkg/analyzer", Hints: enrichment.SpanHints{Category: "workflow"}, Children: []*analyzer.TreeNode{job}}

	items := BuildTreeItems([]*analyzer.TreeNode{wf}, map[string]bool{}, nil)

	byName := map[string]*TreeItem{}
	var walk func(its []*TreeItem)
	walk = func(its []*TreeItem) {
		for _, it := range its {
			byName[it.DisplayName] = it
			walk(it.Children)
		}
	}
	walk(items)

	if got := byName["build"]; got == nil || got.SourceHint != "runner" {
		t.Errorf("job 'build' should have SourceHint=runner, got %+v", got)
	}
	if got := byName["unit tests"]; got == nil || got.SourceHint != "jest" {
		t.Errorf("tool span should have SourceHint=jest, got %+v", got)
	}
	if got := byName["Run tests"]; got == nil || got.SourceHint != "" {
		t.Errorf("step should have no SourceHint (same source as parent), got %q", got.SourceHint)
	}
	// Canonical names must remain hint-free.
	if got := byName["build"]; got != nil && got.Name != "build" {
		t.Errorf("Name must stay clean, got %q", got.Name)
	}
}

func TestSourceLabel(t *testing.T) {
	cases := []struct {
		node *analyzer.TreeNode
		want string
	}{
		{&analyzer.TreeNode{Hints: enrichment.SpanHints{ServiceName: "github-actions-runner"}}, "runner"},
		{&analyzer.TreeNode{Attrs: map[string]string{"source": "runner"}}, "runner"},
		{&analyzer.TreeNode{Hints: enrichment.SpanHints{ServiceName: "jest"}}, "jest"},
		{&analyzer.TreeNode{ScopeName: "github.com/stefanpenner/otel-explorer/pkg/analyzer"}, "github-api"},
		{&analyzer.TreeNode{}, "github-api"},
	}
	for _, c := range cases {
		if got := c.node.SourceLabel(); got != c.want {
			t.Errorf("SourceLabel() = %q, want %q", got, c.want)
		}
	}
}
