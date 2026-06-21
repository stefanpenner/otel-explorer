package diff

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// View renders the full screen: header, scrollable tree, footer. The assembled
// lines are clamped to the terminal height so the alt-screen is never corrupted
// on a short terminal (bubbletea repaints exactly height lines).
func (m Model) View() string {
	var lines []string
	lines = append(lines, strings.Split(m.header(), "\n")...)
	lines = append(lines, strings.Split(m.body(), "\n")...)
	lines = append(lines, strings.Split(m.footer(), "\n")...)
	// Clamp every line to the terminal width so nothing wraps (a wrapped line
	// would throw off the height accounting below and corrupt the alt-screen).
	if m.width > 0 {
		for i, ln := range lines {
			if lipgloss.Width(ln) > m.width {
				lines[i] = truncateVisible(ln, m.width)
			}
		}
	}
	if m.height > 0 && len(lines) > m.height {
		lines = lines[:m.height]
	}
	return strings.Join(lines, "\n")
}

func (m Model) header() string {
	d := m.diff
	wall := d.WallDelta()
	title := titleStyle.Render("Trace Diff")
	before := dimStyle.Render("- " + m.beforeLabel)
	after := dimStyle.Render("+ " + m.afterLabel)

	summary := fmt.Sprintf("%s → %s  %s",
		utils.HumanizeTime(d.WallBefore.Seconds()),
		utils.HumanizeTime(d.WallAfter.Seconds()),
		deltaStyle(wall.Seconds()).Render(signed(wall.Seconds())))

	var parts []string
	if d.NumAdded > 0 {
		parts = append(parts, okStyle.Render(fmt.Sprintf("+%d", d.NumAdded)))
	}
	if d.NumRemoved > 0 {
		parts = append(parts, dimStyle.Render(fmt.Sprintf("-%d", d.NumRemoved)))
	}
	if d.NumChanged > 0 {
		parts = append(parts, warnStyle.Render(fmt.Sprintf("~%d", d.NumChanged)))
	}
	if f := len(d.StatusFlips()); f > 0 {
		parts = append(parts, badStyle.Render(fmt.Sprintf("%d flip%s", f, plural(f))))
	}
	counts := strings.Join(parts, dimStyle.Render(" · "))
	if counts == "" {
		counts = okStyle.Render("no changes")
	}

	lines := []string{
		title + "   " + headStyle.Render("wall ") + summary + "   " + counts,
		before,
		after,
		dimStyle.Render(strings.Repeat("─", max(1, m.width))),
	}
	return strings.Join(lines, "\n")
}

func (m Model) body() string {
	h := m.bodyHeight()
	if len(m.rows) == 0 {
		return okStyle.Render("  ✓ no semantic differences")
	}
	var b strings.Builder
	end := m.top + h
	if end > len(m.rows) {
		end = len(m.rows)
	}
	for i := m.top; i < end; i++ {
		b.WriteString(m.renderRow(m.rows[i], i == m.cursor))
		if i < end-1 {
			b.WriteString("\n")
		}
	}
	// Pad to a stable height so the footer doesn't jump around.
	for i := end - m.top; i < h; i++ {
		b.WriteString("\n")
	}
	return b.String()
}

func (m Model) renderRow(row analyzer.FlatDiffNode, selected bool) string {
	n := row.Node
	marker, mstyle := marker(n)
	indent := strings.Repeat("  ", row.Depth)

	left := mstyle.Render(marker) + " " + indent + n.Name
	right := rowTiming(n)
	if n.StatusChanged() {
		_, st := statusStyle(n.OutcomeAfter)
		right += "  " + st.Render(n.OutcomeBefore+"→"+n.OutcomeAfter)
	}

	width := m.width
	if width < 1 {
		width = 1
	}
	var line string
	if gap := width - lipgloss.Width(left) - lipgloss.Width(right) - 1; gap >= 1 {
		line = " " + left + strings.Repeat(" ", gap) + right
	} else {
		line = " " + left + " " + right
	}
	// Never exceed the terminal width — otherwise the row wraps and the
	// alt-screen line accounting drifts. Truncation drops styling on the
	// overflowing tail, which only happens on very narrow terminals.
	if lipgloss.Width(line) > width {
		line = truncateVisible(line, width)
	}
	if selected {
		return selStyle.Render(padLine(line, width))
	}
	return line
}

func (m Model) footer() string {
	pos := ""
	if len(m.rows) > 0 {
		pos = fmt.Sprintf("%d/%d", m.cursor+1, len(m.rows))
	}
	sep := dimStyle.Render(strings.Repeat("─", max(1, m.width)))
	if m.showHelp {
		help := "↑/↓ move · n/N next/prev change · u show/hide unchanged · g/G top/bottom · ? help · q quit"
		return sep + "\n" + dimStyle.Render(help) + "\n" + m.detailLine()
	}
	mode := "changes only"
	if !m.hideUnchanged {
		mode = "full tree"
	}
	hint := dimStyle.Render(fmt.Sprintf("%s · u: %s · ?: help · q: quit", pos, mode))
	return sep + "\n" + hint + "\n" + m.detailLine()
}

// detailLine shows the selected node's before/after in full.
func (m Model) detailLine() string {
	if m.cursor < 0 || m.cursor >= len(m.rows) {
		return ""
	}
	n := m.rows[m.cursor].Node
	cat := n.Category
	if cat == "" {
		cat = "span"
	}
	switch n.Kind {
	case analyzer.Added:
		return whiteStyle.Render(n.Name) + dimStyle.Render(fmt.Sprintf("  [%s] added · %s", cat, utils.HumanizeTime(n.DurAfter.Seconds())))
	case analyzer.Removed:
		return whiteStyle.Render(n.Name) + dimStyle.Render(fmt.Sprintf("  [%s] removed · was %s", cat, utils.HumanizeTime(n.DurBefore.Seconds())))
	default:
		detail := fmt.Sprintf("  [%s] %s → %s  %s", cat,
			utils.HumanizeTime(n.DurBefore.Seconds()),
			utils.HumanizeTime(n.DurAfter.Seconds()),
			signed(n.DurDelta.Seconds()))
		if n.StatusChanged() {
			detail += fmt.Sprintf("  %s → %s", n.OutcomeBefore, n.OutcomeAfter)
		}
		return whiteStyle.Render(n.Name) + dimStyle.Render(detail)
	}
}

// --- formatting helpers ---

func marker(n *analyzer.DiffNode) (string, lipgloss.Style) {
	switch n.Kind {
	case analyzer.Added:
		return "+", okStyle
	case analyzer.Removed:
		return "-", dimStyle
	case analyzer.Changed:
		if n.Improved() {
			return "~", okStyle
		}
		return "~", badStyle
	default:
		return " ", dimStyle
	}
}

func rowTiming(n *analyzer.DiffNode) string {
	switch n.Kind {
	case analyzer.Added:
		return okStyle.Render("+" + utils.HumanizeTime(n.DurAfter.Seconds()))
	case analyzer.Removed:
		return dimStyle.Render("-" + utils.HumanizeTime(n.DurBefore.Seconds()))
	default:
		base := dimStyle.Render(fmt.Sprintf("%s→%s", utils.HumanizeTime(n.DurBefore.Seconds()), utils.HumanizeTime(n.DurAfter.Seconds())))
		if n.DurDelta == 0 {
			return base
		}
		return base + " " + deltaStyle(n.DurDelta.Seconds()).Render(signed(n.DurDelta.Seconds()))
	}
}

func deltaStyle(seconds float64) lipgloss.Style {
	switch {
	case seconds > 0:
		return badStyle
	case seconds < 0:
		return okStyle
	default:
		return dimStyle
	}
}

func statusStyle(outcome string) (string, lipgloss.Style) {
	switch outcome {
	case "success":
		return "✓", okStyle
	case "failure":
		return "✗", badStyle
	case "skipped":
		return "○", dimStyle
	default:
		return "•", warnStyle
	}
}

func signed(seconds float64) string {
	s := utils.HumanizeTime(seconds)
	if seconds > 0 {
		return "+" + s
	}
	return s
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// padLine pads a (possibly styled) line with spaces to a visible width so a
// selection background fills the row.
func padLine(line string, width int) string {
	w := lipgloss.Width(line)
	if w >= width {
		return line
	}
	return line + strings.Repeat(" ", width-w)
}

// truncateVisible trims a styled string to a visible width, adding an ellipsis.
// It is conservative: it works on the plain text and re-emits without styling
// when truncation is needed (truncation only happens on very narrow terminals).
func truncateVisible(s string, width int) string {
	if width < 1 {
		width = 1
	}
	plain := stripStyle(s)
	r := []rune(plain)
	if len(r) <= width {
		return s
	}
	if width == 1 {
		return string(r[:1])
	}
	return string(r[:width-1]) + "…"
}

func stripStyle(s string) string { return utils.StripANSI(s) }
