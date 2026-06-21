package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// diffNameWidth is the column budget for the marker + indented name in the
// tree and mover rows. Long names are truncated to keep the timing columns
// aligned.
const diffNameWidth = 42

// RenderTraceDiff writes a git-diff-style semantic comparison of two traces:
// what wall time changed, which jobs/steps were added, removed, slowed, sped
// up, or flipped pass/fail — attributed to the responsible spans.
//
// beforeLabel and afterLabel identify the two sides (e.g. commit SHAs, run
// URLs, or file names).
func RenderTraceDiff(w io.Writer, d *analyzer.TraceDiff, beforeLabel, afterLabel string) {
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s\n", titleStyle.Render("Trace Diff"))
	fmt.Fprintf(w, "  %s %s\n", dimStyle.Render("- before"), labelStyle.Render(beforeLabel))
	fmt.Fprintf(w, "  %s  %s\n", dimStyle.Render("+ after"), labelStyle.Render(afterLabel))

	renderDiffSummary(w, d)

	if !d.HasChanges() {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  %s\n", successStyle.Render("✓ no semantic differences"))
		fmt.Fprintln(w)
		return
	}

	renderStatusFlips(w, d)
	renderTopMovers(w, d)
	renderDiffTree(w, d)
	fmt.Fprintln(w)
}

func renderDiffSummary(w io.Writer, d *analyzer.TraceDiff) {
	fmt.Fprintln(w)
	wallDelta := d.WallDelta()
	fmt.Fprintf(w, "  %s  %s → %s   %s\n",
		labelStyle.Render("wall clock"),
		utils.HumanizeTime(d.WallBefore.Seconds()),
		utils.HumanizeTime(d.WallAfter.Seconds()),
		deltaStyle(wallDelta).Render(signedDur(wallDelta)+pctSuffix(wallDelta, d.WallBefore)))

	var parts []string
	if d.NumAdded > 0 {
		parts = append(parts, successOrPlain(fmt.Sprintf("%d added", d.NumAdded)))
	}
	if d.NumRemoved > 0 {
		parts = append(parts, fmt.Sprintf("%d removed", d.NumRemoved))
	}
	if d.NumChanged > 0 {
		parts = append(parts, fmt.Sprintf("%d changed", d.NumChanged))
	}
	if flips := len(d.StatusFlips()); flips > 0 {
		parts = append(parts, failureStyle.Render(fmt.Sprintf("%d status flip%s", flips, plural(flips))))
	}
	if len(parts) == 0 {
		parts = append(parts, dimStyle.Render("no structural changes"))
	}
	fmt.Fprintf(w, "  %s  %s\n", labelStyle.Render("changes   "), strings.Join(parts, dimStyle.Render(" · ")))
}

func renderStatusFlips(w io.Writer, d *analyzer.TraceDiff) {
	flips := d.StatusFlips()
	if len(flips) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s\n", subheaderStyle.Render("Status flips"))
	for _, n := range flips {
		icon, style := statusGlyph(n.OutcomeBefore, n.OutcomeAfter)
		fmt.Fprintf(w, "    %s %s  %s\n",
			style.Render(icon),
			padName(n.Name, diffNameWidth-4),
			style.Render(fmt.Sprintf("%s → %s", n.OutcomeBefore, n.OutcomeAfter)))
	}
}

func renderTopMovers(w io.Writer, d *analyzer.TraceDiff) {
	movers := d.TopMovers(8)
	if len(movers) == 0 {
		return
	}
	parents := parentNames(d.Roots)
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s %s\n", subheaderStyle.Render("Top movers"),
		dimStyle.Render("(wall-clock change attributed to the responsible span)"))
	for _, m := range movers {
		n := m.DiffNode
		marker, mstyle := diffMarker(n)
		styled, plain := n.Name, n.Name
		switch {
		case m.SelfOnly:
			styled += dimStyle.Render(" (own time)")
			plain += " (own time)"
		case parents[n] != "":
			styled += dimStyle.Render(" (" + parents[n] + ")")
			plain += " (" + parents[n] + ")"
		}
		fmt.Fprintf(w, "    %s %s  %s\n",
			mstyle.Render(marker),
			padNameStyled(styled, plain, diffNameWidth-4),
			moverTiming(m))
	}
}

func renderDiffTree(w io.Writer, d *analyzer.TraceDiff) {
	rows := d.Flatten(true) // hide unchanged subtrees, keep changed structure
	if len(rows) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s %s\n", subheaderStyle.Render("Tree"),
		dimStyle.Render("(- removed  + added  ~ changed)"))
	for _, row := range rows {
		n := row.Node
		marker, mstyle := diffMarker(n)
		indent := strings.Repeat("  ", row.Depth)
		name := indent + n.Name
		timing := nodeTiming(n)
		status := ""
		if n.StatusChanged() {
			_, style := statusGlyph(n.OutcomeBefore, n.OutcomeAfter)
			status = "  " + style.Render(fmt.Sprintf("%s → %s", n.OutcomeBefore, n.OutcomeAfter))
		}
		fmt.Fprintf(w, "  %s %s  %s%s\n",
			mstyle.Render(marker),
			padName(name, diffNameWidth),
			timing,
			status)
	}
}

// --- timing / formatting helpers ---

// nodeTiming renders the "before → after  Δ" column for a tree row.
func nodeTiming(n *analyzer.DiffNode) string {
	switch n.Kind {
	case analyzer.Added:
		return fmt.Sprintf("%s %s", successStyle.Render("→ "+utils.HumanizeTime(n.DurAfter.Seconds())), dimStyle.Render("new"))
	case analyzer.Removed:
		return fmt.Sprintf("%s %s", dimStyle.Render(utils.HumanizeTime(n.DurBefore.Seconds())+" →"), dimStyle.Render("gone"))
	default:
		base := fmt.Sprintf("%s → %s", utils.HumanizeTime(n.DurBefore.Seconds()), utils.HumanizeTime(n.DurAfter.Seconds()))
		if n.DurDelta == 0 {
			return dimStyle.Render(base)
		}
		return fmt.Sprintf("%s  %s", base,
			deltaStyle(n.DurDelta).Render(signedDur(n.DurDelta)+pctSuffix(n.DurDelta, n.DurBefore)))
	}
}

// moverTiming is the compact timing used in the Top movers list.
func moverTiming(m analyzer.Mover) string {
	n := m.DiffNode
	switch n.Kind {
	case analyzer.Added:
		return successStyle.Render(fmt.Sprintf("+%s", utils.HumanizeTime(n.DurAfter.Seconds()))) + dimStyle.Render(" new")
	case analyzer.Removed:
		return dimStyle.Render(fmt.Sprintf("-%s gone", utils.HumanizeTime(n.DurBefore.Seconds())))
	default:
		if m.SelfOnly {
			// Attribute only the node's own-time change; its full before→after
			// would conflate the child regression we already blamed separately.
			return deltaStyle(m.Delta).Render(signedDur(m.Delta)) + dimStyle.Render(" own overhead")
		}
		return deltaStyle(n.DurDelta).Render(signedDur(n.DurDelta)+pctSuffix(n.DurDelta, n.DurBefore)) +
			dimStyle.Render(fmt.Sprintf("  %s → %s", utils.HumanizeTime(n.DurBefore.Seconds()), utils.HumanizeTime(n.DurAfter.Seconds())))
	}
}

func diffMarker(n *analyzer.DiffNode) (string, lipgloss.Style) {
	switch n.Kind {
	case analyzer.Added:
		return "+", successStyle
	case analyzer.Removed:
		return "-", dimStyle
	case analyzer.Changed:
		if n.Improved() {
			return "~", successStyle
		}
		return "~", failureStyle
	default:
		return " ", dimStyle
	}
}

// deltaStyle colours a duration delta: slower is red, faster is green.
func deltaStyle(delta interface{ Seconds() float64 }) lipgloss.Style {
	s := delta.Seconds()
	switch {
	case s > 0:
		return failureStyle
	case s < 0:
		return successStyle
	default:
		return dimStyle
	}
}

// signedDur formats a duration with an explicit + for positives (negatives and
// zero are already handled by HumanizeTime).
func signedDur(d interface{ Seconds() float64 }) string {
	s := utils.HumanizeTime(d.Seconds())
	if d.Seconds() > 0 {
		return "+" + s
	}
	return s
}

// pctSuffix renders " (+37%)" given a delta and its baseline; empty when there
// is no meaningful baseline. A nonzero delta that rounds to 0% renders "(<1%)"
// rather than the misleading "(+0%)".
func pctSuffix(delta interface{ Seconds() float64 }, base interface{ Seconds() float64 }) string {
	if base.Seconds() <= 0 {
		return ""
	}
	pct := delta.Seconds() / base.Seconds() * 100
	if pct != 0 && pct > -0.5 && pct < 0.5 {
		return " (<1%)"
	}
	sign := ""
	if pct > 0 {
		sign = "+"
	}
	return fmt.Sprintf(" (%s%.0f%%)", sign, pct)
}

// statusGlyph picks an icon and colour for an outcome transition.
func statusGlyph(before, after string) (string, lipgloss.Style) {
	switch {
	case after == "failure":
		return "✗", failureStyle
	case before == "failure" && after == "success":
		return "✓", successStyle
	case after == "skipped":
		return "○", dimStyle
	default:
		return "•", warningStyle
	}
}

// parentNames maps each diff node to its parent's name, for context in the
// flat mover list.
func parentNames(roots []*analyzer.DiffNode) map[*analyzer.DiffNode]string {
	m := make(map[*analyzer.DiffNode]string)
	var walk func(parent string, ns []*analyzer.DiffNode)
	walk = func(parent string, ns []*analyzer.DiffNode) {
		for _, n := range ns {
			m[n] = parent
			walk(n.Name, n.Children)
		}
	}
	walk("", roots)
	return m
}

func padName(s string, width int) string {
	w := lipgloss.Width(s)
	if w > width {
		return truncateDisplay(s, width)
	}
	return s + strings.Repeat(" ", width-w)
}

// padNameStyled pads to width using the *plain* display width while emitting
// the styled string, so ANSI escapes and wide runes don't break alignment.
func padNameStyled(styled, plain string, width int) string {
	w := lipgloss.Width(plain)
	if w > width {
		return truncateDisplay(plain, width) // styling is dropped only when truncating
	}
	return styled + strings.Repeat(" ", width-w)
}

// truncateDisplay trims s to a target display width (counting wide runes as 2
// columns), appending an ellipsis. The result never exceeds width columns.
func truncateDisplay(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if width == 1 {
		return "…"
	}
	var b strings.Builder
	used := 0
	for _, r := range s {
		rw := lipgloss.Width(string(r))
		if used+rw > width-1 { // leave a column for the ellipsis
			break
		}
		b.WriteRune(r)
		used += rw
	}
	b.WriteString("…")
	return b.String()
}

func successOrPlain(s string) string { return successStyle.Render(s) }

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
