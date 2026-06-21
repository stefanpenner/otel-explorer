package diff

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

func tn(name, category, outcome string, dur time.Duration, children ...*analyzer.TreeNode) *analyzer.TreeNode {
	start := time.Unix(1700000000, 0)
	return &analyzer.TreeNode{
		Name:      name,
		StartTime: start,
		EndTime:   start.Add(dur),
		Hints:     enrichment.SpanHints{Category: category, Outcome: outcome},
		Children:  children,
	}
}

func sampleDiff() *analyzer.TraceDiff {
	before := []*analyzer.TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 4*time.Minute,
				tn("compile", "step", "success", 4*time.Minute)),
			tn("test", "job", "success", 6*time.Minute,
				tn("run specs", "step", "success", 6*time.Minute)),
			tn("lint", "job", "success", 2*time.Minute),
		),
	}
	after := []*analyzer.TreeNode{
		tn("CI", "workflow", "failure", 13*time.Minute,
			tn("build", "job", "success", 4*time.Minute,
				tn("compile", "step", "success", 4*time.Minute)),
			tn("test", "job", "failure", 9*time.Minute,
				tn("run specs", "step", "failure", 9*time.Minute)),
			tn("typecheck", "job", "success", 3*time.Minute),
		),
	}
	return analyzer.DiffTraces(before, after)
}

func sized(m Model) Model {
	nm, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	return nm.(Model)
}

func TestModelStartsOnChangesOnly(t *testing.T) {
	m := NewModel(sampleDiff(), "abc", "def")
	if !m.hideUnchanged {
		t.Error("diff view should start with unchanged subtrees hidden")
	}
	for _, r := range m.rows {
		if r.Node.Kind == analyzer.Unchanged {
			t.Errorf("changes-only view contains unchanged node %q", r.Node.Name)
		}
	}
}

func TestToggleUnchangedRevealsContextAndKeepsAnchor(t *testing.T) {
	m := sized(NewModel(sampleDiff(), "abc", "def"))
	changesOnly := len(m.rows)

	// Anchor on the "test" node, then reveal the full tree.
	for i, r := range m.rows {
		if r.Node.Name == "test" {
			m.cursor = i
		}
	}
	nm, _ := m.Update(keyMsg("u"))
	m = nm.(Model)

	if m.hideUnchanged {
		t.Fatal("pressing u should reveal unchanged nodes")
	}
	if len(m.rows) <= changesOnly {
		t.Errorf("full tree (%d rows) should be larger than changes-only (%d)", len(m.rows), changesOnly)
	}
	if m.rows[m.cursor].Node.Name != "test" {
		t.Errorf("cursor anchor lost: on %q, want test", m.rows[m.cursor].Node.Name)
	}
	// The previously-hidden build branch is now present.
	if !rowsContain(m.rows, "compile") {
		t.Error("full tree should include the unchanged 'compile' step")
	}
}

func TestNavigationClampsAndJumps(t *testing.T) {
	m := sized(NewModel(sampleDiff(), "abc", "def"))

	// Up at the top stays at 0.
	nm, _ := m.Update(keyMsg("up"))
	m = nm.(Model)
	if m.cursor != 0 {
		t.Errorf("cursor=%d after up at top, want 0", m.cursor)
	}
	// G jumps to the bottom.
	nm, _ = m.Update(keyMsg("G"))
	m = nm.(Model)
	if m.cursor != len(m.rows)-1 {
		t.Errorf("cursor=%d after G, want %d", m.cursor, len(m.rows)-1)
	}
}

func TestQuitKey(t *testing.T) {
	m := sized(NewModel(sampleDiff(), "abc", "def"))
	_, cmd := m.Update(keyMsg("q"))
	if cmd == nil {
		t.Fatal("q should return a quit command")
	}
	if msg := cmd(); msg == nil {
		t.Error("quit command should produce a message")
	} else if _, ok := msg.(tea.QuitMsg); !ok {
		t.Errorf("q produced %T, want tea.QuitMsg", msg)
	}
}

func TestViewRendersHeaderAndChanges(t *testing.T) {
	m := sized(NewModel(sampleDiff(), "commitA", "commitB"))
	out := utils.StripANSI(m.View())

	for _, want := range []string{"Trace Diff", "commitA", "commitB", "test", "typecheck", "run specs"} {
		if !strings.Contains(out, want) {
			t.Errorf("view missing %q:\n%s", want, out)
		}
	}
	// Unchanged context is hidden by default.
	if strings.Contains(out, "compile") {
		t.Errorf("changes-only view should not show 'compile':\n%s", out)
	}
	// Wall-clock delta is shown (10m → 13m).
	if !strings.Contains(out, "→") {
		t.Errorf("view missing duration arrow:\n%s", out)
	}
}

func TestEmptyDiffNavigationDoesNotPanic(t *testing.T) {
	// Two identical traces → changes-only view has zero rows. Navigating must
	// not panic (regression: G drove the cursor to -1 and View indexed rows[-1]).
	same := func() []*analyzer.TreeNode {
		return []*analyzer.TreeNode{tn("CI", "workflow", "success", 5*time.Minute,
			tn("test", "job", "success", 5*time.Minute))}
	}
	m := sized(NewModel(analyzer.DiffTraces(same(), same()), "a", "b"))
	if len(m.rows) != 0 {
		t.Fatalf("identical traces should yield 0 changes-only rows, got %d", len(m.rows))
	}
	for _, k := range []string{"G", "down", "up", "n", "N", "u"} {
		nm, _ := m.Update(keyMsg(k))
		m = nm.(Model)
		_ = m.View() // must not panic
	}
}

func TestViewNeverExceedsTerminalHeight(t *testing.T) {
	d := sampleDiff()
	for _, h := range []int{1, 2, 5, 8, 12, 24} {
		nm, _ := NewModel(d, "a", "b").Update(tea.WindowSizeMsg{Width: 40, Height: h})
		m := nm.(Model)
		lines := strings.Count(m.View(), "\n") + 1
		if lines > h {
			t.Errorf("height=%d: View rendered %d lines (> height)", h, lines)
		}
	}
}

func TestRowNeverExceedsWidth(t *testing.T) {
	d := sampleDiff()
	for _, w := range []int{10, 20, 40, 80} {
		nm, _ := NewModel(d, "a", "b").Update(tea.WindowSizeMsg{Width: w, Height: 24})
		m := nm.(Model)
		for _, line := range strings.Split(utils.StripANSI(m.View()), "\n") {
			if w2 := len([]rune(line)); w2 > w {
				t.Errorf("width=%d: line %q is %d wide", w, line, w2)
			}
		}
	}
}

func keyMsg(s string) tea.KeyMsg {
	switch s {
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "G":
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("G")}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

func rowsContain(rows []analyzer.FlatDiffNode, name string) bool {
	for _, r := range rows {
		if r.Node.Name == name {
			return true
		}
	}
	return false
}
