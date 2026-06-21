package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
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

func renderToString(d *analyzer.TraceDiff, a, b string) string {
	var buf bytes.Buffer
	RenderTraceDiff(&buf, d, a, b)
	return utils.StripANSI(buf.String())
}

func TestRenderTraceDiffFull(t *testing.T) {
	before := []*analyzer.TreeNode{
		tn("CI", "workflow", "success", 10*time.Minute,
			tn("build", "job", "success", 4*time.Minute,
				tn("compile", "step", "success", 4*time.Minute)),
			tn("test", "job", "success", 8*time.Minute,
				tn("run specs", "step", "success", 8*time.Minute)),
			tn("lint", "job", "success", 2*time.Minute),
		),
	}
	after := []*analyzer.TreeNode{
		tn("CI", "workflow", "failure", 12*time.Minute,
			tn("build", "job", "success", 4*time.Minute,
				tn("compile", "step", "success", 4*time.Minute)),
			tn("test", "job", "failure", 12*time.Minute,
				tn("run specs", "step", "failure", 12*time.Minute)),
			tn("typecheck", "job", "success", 3*time.Minute),
		),
	}

	d := analyzer.DiffTraces(before, after)
	out := renderToString(d, "abc123", "def456")
	t.Logf("\n%s", out)

	// Header + labels.
	mustContain(t, out, "Trace Diff")
	mustContain(t, out, "abc123")
	mustContain(t, out, "def456")
	// Wall clock summary.
	mustContain(t, out, "10m → 12m")
	// Counts.
	mustContain(t, out, "1 added")
	mustContain(t, out, "1 removed")
	mustContain(t, out, "status flip")
	// Status flips section names the regressed job.
	mustContain(t, out, "success → failure")
	// Top movers attribute to the responsible leaf, the added & removed jobs.
	mustContain(t, out, "run specs")
	mustContain(t, out, "typecheck")
	mustContain(t, out, "lint")
	// The unchanged build/compile branch is pruned from the tree.
	if strings.Contains(treeSection(out), "compile") {
		t.Errorf("unchanged 'compile' step should be hidden from the diff tree:\n%s", out)
	}
}

func TestRenderTraceDiffNoChanges(t *testing.T) {
	before := []*analyzer.TreeNode{tn("CI", "workflow", "success", 5*time.Minute)}
	after := []*analyzer.TreeNode{tn("CI", "workflow", "success", 5*time.Minute)}
	out := renderToString(analyzer.DiffTraces(before, after), "a", "b")
	mustContain(t, out, "no semantic differences")
}

func TestRenderTraceDiffMarkdownAndJSON(t *testing.T) {
	before := []*analyzer.TreeNode{
		tn("CI", "workflow", "success", 8*time.Minute,
			tn("test", "job", "success", 8*time.Minute,
				tn("run specs", "step", "success", 8*time.Minute))),
	}
	after := []*analyzer.TreeNode{
		tn("CI", "workflow", "failure", 12*time.Minute,
			tn("test", "job", "failure", 12*time.Minute,
				tn("run specs", "step", "failure", 12*time.Minute))),
	}
	d := analyzer.DiffTraces(before, after)

	var md bytes.Buffer
	RenderTraceDiffMarkdown(&md, d, "abc123", "def456")
	mds := md.String()
	mustContain(t, mds, "## Trace Diff")
	mustContain(t, mds, "```diff")
	mustContain(t, mds, "Status flips")
	mustContain(t, mds, "| span |")

	// A span name containing a pipe must not break the GFM movers table.
	pb := []*analyzer.TreeNode{tn("CI", "workflow", "success", 5*time.Minute, tn("test | x", "job", "success", 5*time.Minute))}
	pa := []*analyzer.TreeNode{tn("CI", "workflow", "success", 9*time.Minute, tn("test | x", "job", "success", 9*time.Minute))}
	var pmd bytes.Buffer
	RenderTraceDiffMarkdown(&pmd, analyzer.DiffTraces(pb, pa), "a", "b")
	if !strings.Contains(pmd.String(), `test \| x`) {
		t.Errorf("pipe in span name not escaped for markdown table:\n%s", pmd.String())
	}

	var jsonbuf bytes.Buffer
	if err := RenderTraceDiffJSON(&jsonbuf, d, "abc123", "def456"); err != nil {
		t.Fatalf("json render: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(jsonbuf.Bytes(), &parsed); err != nil {
		t.Fatalf("json not parseable: %v\n%s", err, jsonbuf.String())
	}
	wall, ok := parsed["wall"].(map[string]any)
	if !ok {
		t.Fatalf("json missing wall object: %v", parsed)
	}
	if wall["delta_sec"].(float64) != 240 {
		t.Errorf("wall.delta_sec = %v, want 240", wall["delta_sec"])
	}
	flips, _ := parsed["status_flips"].([]any)
	if len(flips) != 3 {
		t.Errorf("json status_flips = %d, want 3", len(flips))
	}
}

func TestPadNameWideRunes(t *testing.T) {
	// CJK glyphs are two columns wide; padding must use display width so the
	// timing column still aligns.
	got := padName("テスト", 10) // 3 wide runes = 6 columns
	if w := lipgloss.Width(got); w != 10 {
		t.Errorf("padName wide-rune width = %d, want 10 (%q)", w, got)
	}
	// Truncation also respects display width and stays within budget.
	long := padName("テストテストテスト", 6)
	if w := lipgloss.Width(long); w > 6 {
		t.Errorf("truncated width = %d, want <= 6 (%q)", w, long)
	}
}

func TestPctSuffixSubOnePercent(t *testing.T) {
	// A nonzero delta that rounds to 0% must not read "(+0%)".
	if got := pctSuffix(time.Second, 5*time.Minute); got != " (<1%)" {
		t.Errorf("pctSuffix(1s/5m) = %q, want \" (<1%%)\"", got)
	}
	if got := pctSuffix(time.Duration(0), 5*time.Minute); got != " (+0%)" && got != "" {
		// exact-zero delta is allowed to render +0% or nothing, just not <1%.
		if got == " (<1%)" {
			t.Errorf("zero delta should not render <1%%, got %q", got)
		}
	}
}

func TestMarkdownEscapesBacktickInLabel(t *testing.T) {
	before := []*analyzer.TreeNode{tn("CI", "workflow", "success", 5*time.Minute, tn("a`b", "job", "success", 5*time.Minute))}
	after := []*analyzer.TreeNode{tn("CI", "workflow", "failure", 9*time.Minute, tn("a`b", "job", "failure", 9*time.Minute))}
	var md bytes.Buffer
	RenderTraceDiffMarkdown(&md, analyzer.DiffTraces(before, after), "lbl`x", "y")
	out := md.String()
	if strings.Contains(out, "lbl`x`") || strings.Contains(out, "`a`b`") {
		t.Errorf("backtick in label/name not neutralized for markdown:\n%s", out)
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("output missing %q in:\n%s", needle, haystack)
	}
}

// treeSection returns just the portion of the report after the "Tree" header.
func treeSection(out string) string {
	if i := strings.Index(out, "Tree"); i >= 0 {
		return out[i:]
	}
	return ""
}
