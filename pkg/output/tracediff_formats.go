package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// RenderTraceDiffMarkdown writes the diff as GitHub-flavored markdown, suitable
// for dropping into a PR comment comparing two commits' CI runs.
func RenderTraceDiffMarkdown(w io.Writer, d *analyzer.TraceDiff, beforeLabel, afterLabel string) {
	fmt.Fprintln(w, "## Trace Diff")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "- **before:** %s\n", mdCell(beforeLabel))
	fmt.Fprintf(w, "- **after:** %s\n", mdCell(afterLabel))
	fmt.Fprintln(w)

	wall := d.WallDelta()
	fmt.Fprintf(w, "**Wall clock:** %s → %s (**%s**%s)\n\n",
		utils.HumanizeTime(d.WallBefore.Seconds()),
		utils.HumanizeTime(d.WallAfter.Seconds()),
		signedDur(wall), pctSuffix(wall, d.WallBefore))

	if !d.HasChanges() {
		fmt.Fprintln(w, "✅ No semantic differences.")
		return
	}

	var counts []string
	if d.NumAdded > 0 {
		counts = append(counts, fmt.Sprintf("%d added", d.NumAdded))
	}
	if d.NumRemoved > 0 {
		counts = append(counts, fmt.Sprintf("%d removed", d.NumRemoved))
	}
	if d.NumChanged > 0 {
		counts = append(counts, fmt.Sprintf("%d changed", d.NumChanged))
	}
	if d.NumRenamed > 0 {
		counts = append(counts, fmt.Sprintf("%d renamed", d.NumRenamed))
	}
	if f := len(d.StatusFlips()); f > 0 {
		counts = append(counts, fmt.Sprintf("%d status flip%s", f, plural(f)))
	}
	fmt.Fprintf(w, "**Changes:** %s\n\n", strings.Join(counts, " · "))

	if flips := d.StatusFlips(); len(flips) > 0 {
		fmt.Fprintln(w, "### ⚠️ Status flips")
		for _, n := range flips {
			emoji := "❌"
			if n.OutcomeAfter == "success" {
				emoji = "✅"
			}
			fmt.Fprintf(w, "- %s %s — %s → %s\n", emoji, mdCell(n.Name), n.OutcomeBefore, n.OutcomeAfter)
		}
		fmt.Fprintln(w)
	}

	if movers := d.TopMovers(8); len(movers) > 0 {
		parents := parentNames(d.Roots)
		fmt.Fprintln(w, "### Top movers")
		fmt.Fprintln(w, "| | span | Δ | before → after |")
		fmt.Fprintln(w, "|---|---|---|---|")
		for _, m := range movers {
			n := m.DiffNode
			name, delta, rng := diffDisplayName(n), markdownDelta(n), markdownRange(n)
			switch {
			case m.SelfOnly:
				name += " (own time)"
				delta, rng = signedDur(m.Delta), "own overhead"
			case parents[n] != "":
				name += fmt.Sprintf(" (%s)", parents[n])
			}
			fmt.Fprintf(w, "| %s | %s | %s | %s |\n",
				markdownMarker(n), mdCell(name), delta, rng)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, "### Tree")
	fmt.Fprintln(w, "```diff")
	for _, row := range d.Flatten(true) {
		n := row.Node
		indent := strings.Repeat("  ", row.Depth)
		status := ""
		if n.StatusChanged() {
			status = fmt.Sprintf("   [%s → %s]", n.OutcomeBefore, n.OutcomeAfter)
		}
		fmt.Fprintf(w, "%s %s%s   %s%s\n",
			markdownMarker(n), indent, diffDisplayName(n), markdownRange(n), status)
	}
	fmt.Fprintln(w, "```")
}

// mdCell renders a span name as a GFM table cell wrapped in a code span,
// escaping the characters that would otherwise break the cell: a literal pipe
// (column separator) and a backtick (would close the code span).
func mdCell(name string) string {
	name = strings.ReplaceAll(name, "`", "'")
	name = strings.ReplaceAll(name, "|", "\\|")
	return "`" + name + "`"
}

func markdownMarker(n *analyzer.DiffNode) string {
	switch n.Kind {
	case analyzer.Added:
		return "+"
	case analyzer.Removed:
		return "-"
	case analyzer.Changed:
		return "~"
	case analyzer.Renamed:
		return "R"
	default:
		return " "
	}
}

func markdownRange(n *analyzer.DiffNode) string {
	switch n.Kind {
	case analyzer.Added:
		return utils.HumanizeTime(n.DurAfter.Seconds()) + " (new)"
	case analyzer.Removed:
		return utils.HumanizeTime(n.DurBefore.Seconds()) + " (gone)"
	default:
		return fmt.Sprintf("%s → %s", utils.HumanizeTime(n.DurBefore.Seconds()), utils.HumanizeTime(n.DurAfter.Seconds()))
	}
}

func markdownDelta(n *analyzer.DiffNode) string {
	switch n.Kind {
	case analyzer.Added:
		return "+" + utils.HumanizeTime(n.DurAfter.Seconds())
	case analyzer.Removed:
		return "-" + utils.HumanizeTime(n.DurBefore.Seconds())
	default:
		return signedDur(n.DurDelta) + pctSuffix(n.DurDelta, n.DurBefore)
	}
}

// --- JSON ---

type jsonTraceDiff struct {
	Before      string          `json:"before"`
	After       string          `json:"after"`
	Wall        jsonWall        `json:"wall"`
	Summary     jsonDiffSummary `json:"summary"`
	StatusFlips []jsonFlip      `json:"status_flips"`
	TopMovers   []jsonMover     `json:"top_movers"`
	Tree        []jsonDiffNode  `json:"tree"`
}

type jsonWall struct {
	BeforeSec float64 `json:"before_sec"`
	AfterSec  float64 `json:"after_sec"`
	DeltaSec  float64 `json:"delta_sec"`
	DeltaPct  float64 `json:"delta_pct"`
}

type jsonDiffSummary struct {
	Added       int `json:"added"`
	Removed     int `json:"removed"`
	Changed     int `json:"changed"`
	Renamed     int `json:"renamed"`
	StatusFlips int `json:"status_flips"`
}

type jsonFlip struct {
	Name     string `json:"name"`
	Category string `json:"category"`
	Before   string `json:"before"`
	After    string `json:"after"`
}

type jsonMover struct {
	Name string `json:"name"`
	// OldName: the prior name when this mover is a rename match; empty otherwise.
	OldName string `json:"old_name,omitempty"`
	// SelfOnly: the attributed change is in this node's own scheduling/overhead
	// rather than its whole subtree; AttributedSec is then the self-time delta.
	SelfOnly      bool    `json:"self_only,omitempty"`
	Parent        string  `json:"parent,omitempty"`
	Category      string  `json:"category"`
	Kind          string  `json:"kind"`
	BeforeSec     float64 `json:"before_sec"`
	AfterSec      float64 `json:"after_sec"`
	DeltaSec      float64 `json:"delta_sec"`
	DeltaPct      float64 `json:"delta_pct"`
	AttributedSec float64 `json:"attributed_sec"`
}

type jsonDiffNode struct {
	Name          string         `json:"name"`
	OldName       string         `json:"old_name,omitempty"`
	Category      string         `json:"category"`
	Kind          string         `json:"kind"`
	BeforeSec     float64        `json:"before_sec"`
	AfterSec      float64        `json:"after_sec"`
	DeltaSec      float64        `json:"delta_sec"`
	DeltaPct      float64        `json:"delta_pct"`
	OutcomeBefore string         `json:"outcome_before,omitempty"`
	OutcomeAfter  string         `json:"outcome_after,omitempty"`
	Children      []jsonDiffNode `json:"children,omitempty"`
}

// RenderTraceDiffJSON writes a machine-readable diff (pipe to jq).
func RenderTraceDiffJSON(w io.Writer, d *analyzer.TraceDiff, beforeLabel, afterLabel string) error {
	parents := parentNames(d.Roots)
	out := jsonTraceDiff{
		Before: beforeLabel,
		After:  afterLabel,
		Wall: jsonWall{
			BeforeSec: d.WallBefore.Seconds(),
			AfterSec:  d.WallAfter.Seconds(),
			DeltaSec:  d.WallDelta().Seconds(),
			DeltaPct:  pct(d.WallDelta(), d.WallBefore),
		},
		Summary: jsonDiffSummary{
			Added:       d.NumAdded,
			Removed:     d.NumRemoved,
			Changed:     d.NumChanged,
			Renamed:     d.NumRenamed,
			StatusFlips: len(d.StatusFlips()),
		},
		StatusFlips: []jsonFlip{},
		TopMovers:   []jsonMover{},
		Tree:        []jsonDiffNode{},
	}
	for _, n := range d.StatusFlips() {
		out.StatusFlips = append(out.StatusFlips, jsonFlip{
			Name: n.Name, Category: n.Category, Before: n.OutcomeBefore, After: n.OutcomeAfter,
		})
	}
	for _, m := range d.TopMovers(8) {
		n := m.DiffNode
		parent := parents[n]
		if m.SelfOnly {
			parent = ""
		}
		out.TopMovers = append(out.TopMovers, jsonMover{
			Name: n.Name, OldName: n.OldName, SelfOnly: m.SelfOnly, Parent: parent, Category: n.Category, Kind: n.Kind.String(),
			BeforeSec: n.DurBefore.Seconds(), AfterSec: n.DurAfter.Seconds(),
			DeltaSec: n.DurDelta.Seconds(), DeltaPct: pct(n.DurDelta, n.DurBefore),
			AttributedSec: m.Delta.Seconds(),
		})
	}
	for _, n := range d.Roots {
		out.Tree = append(out.Tree, toJSONNode(n))
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func toJSONNode(n *analyzer.DiffNode) jsonDiffNode {
	j := jsonDiffNode{
		Name: n.Name, OldName: n.OldName, Category: n.Category, Kind: n.Kind.String(),
		BeforeSec: n.DurBefore.Seconds(), AfterSec: n.DurAfter.Seconds(),
		DeltaSec: n.DurDelta.Seconds(), DeltaPct: pct(n.DurDelta, n.DurBefore),
		OutcomeBefore: n.OutcomeBefore, OutcomeAfter: n.OutcomeAfter,
	}
	for _, c := range n.Children {
		j.Children = append(j.Children, toJSONNode(c))
	}
	return j
}

func pct(delta, base interface{ Seconds() float64 }) float64 {
	if base.Seconds() <= 0 {
		return 0
	}
	return delta.Seconds() / base.Seconds() * 100
}
