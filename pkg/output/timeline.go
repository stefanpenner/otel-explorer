package output

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/sdk/trace"
)

// RenderOTelTimeline renders a generic OTel span tree as a terminal waterfall.
func RenderOTelTimeline(w io.Writer, spans []trace.ReadOnlySpan, globalEarliest, globalLatest time.Time, enricher enrichment.Enricher) {
	if len(spans) == 0 {
		return
	}
	roots := analyzer.BuildTreeFromSpans(spans, globalEarliest, globalLatest, enricher)
	if len(roots) == 0 {
		return
	}

	// Find overall time bounds
	earliest := globalEarliest
	latest := globalLatest

	if earliest.IsZero() || latest.IsZero() {
		earliest = roots[0].StartTime
		latest = roots[0].EndTime
		var walk func([]*analyzer.TreeNode)
		walk = func(nodes []*analyzer.TreeNode) {
			for _, n := range nodes {
				if n.StartTime.Before(earliest) {
					earliest = n.StartTime
				}
				if n.EndTime.After(latest) {
					latest = n.EndTime
				}
				walk(n.Children)
			}
		}
		walk(roots)
	}

	if earliest.IsZero() || latest.IsZero() {
		return
	}

	totalDuration := latest.Sub(earliest)
	scale := 60

	startTime := earliest.Format("15:04:05")
	endTime := latest.Format("15:04:05")
	durationStr := utils.HumanizeTime(totalDuration.Seconds())
	if totalDuration <= 0 {
		// All spans share a single timestamp. Clamp to avoid 0/0 = NaN in the
		// bar math below (int(NaN) is architecture-dependent in Go).
		totalDuration = 1
	}

	headerText := fmt.Sprintf(" Start: %s   End: %s   Duration: %s", startTime, endTime, durationStr)
	headerCells := len(headerText) // This is all ASCII
	padding := (scale + 2) - headerCells

	if padding < 0 {
		padding = 0
	}

	fmt.Fprintf(w, "┌%s┐\n", strings.Repeat("─", scale+2))
	fmt.Fprintf(w, "│%s%s│\n", headerText, strings.Repeat(" ", padding))
	fmt.Fprintf(w, "├%s┤\n", strings.Repeat("─", scale+2))

	for _, root := range roots {
		renderNode(w, root, 0, earliest, totalDuration, scale)
	}

	fmt.Fprintf(w, "└%s┘\n", strings.Repeat("─", scale+2))
}

func renderNode(w io.Writer, node *analyzer.TreeNode, depth int, globalStart time.Time, totalDuration time.Duration, scale int) {
	start, dur, visible := clampToWindow(node.StartTime, node.EndTime, globalStart, totalDuration)
	if !visible {
		return // entirely outside the window
	}

	pad, glyph, rest := timelineBar(node.Hints, start, dur, totalDuration, scale)
	colored := colorizeText(glyph, node.Hints.Color)
	name, clock := timelineLabel(node, dur)
	indent := strings.Repeat("  ", depth)

	fmt.Fprintf(w, "│%s%s%s  │ %s%s %s\n", pad, colored, rest, indent, name, clock)

	for _, child := range node.Children {
		renderNode(w, child, depth+1, globalStart, totalDuration, scale)
	}
}

func clampToWindow(start, end, globalStart time.Time, total time.Duration) (offset, dur time.Duration, visible bool) {
	if start.Before(globalStart) {
		start = globalStart
	}
	windowEnd := globalStart.Add(total)
	if end.After(windowEnd) {
		end = windowEnd
	}
	if end.Before(start) {
		return 0, 0, false
	}
	return start.Sub(globalStart), end.Sub(start), true
}

func timelineBar(h enrichment.SpanHints, start, duration, total time.Duration, scale int) (pad, glyph, rest string) {
	startPos := int(float64(start) / float64(total) * float64(scale))
	barLength := maxInt(1, int(float64(duration)/float64(total)*float64(scale)))
	clampedLength := minInt(barLength, scale-startPos)

	pad = strings.Repeat(" ", maxInt(0, startPos))

	barChar := h.BarChar
	if barChar == "" {
		barChar = "█"
	}

	cells := maxInt(1, clampedLength)
	glyph = strings.Repeat(barChar, cells)
	if h.IsMarker {
		// A marker is one glyph. Reserve its display width, not a repeated bar.
		cells = markerWidth(barChar)
		glyph = barChar
	}
	rest = strings.Repeat(" ", maxInt(0, scale-startPos-cells))
	return pad, glyph, rest
}

// timelineLabel is the row caption. Hierarchy is the depth indent in renderNode,
// not spaces inside the icon: a padded leaf lines up with the next depth and
// looks like a sibling.
func timelineLabel(node *analyzer.TreeNode, duration time.Duration) (name, clock string) {
	h := node.Hints

	icon := h.Icon
	if icon == "" {
		icon = "• "
	}

	label := node.Name
	if h.User != "" {
		label = fmt.Sprintf("%s by %s", label, h.User)
	}
	if h.URL != "" {
		label = utils.MakeClickableLink(h.URL, label)
	}
	// Semantic detail (model, route, statement, token usage) so HTTP, DB, RPC,
	// and GenAI spans read at a glance. Markers carry no detail.
	if h.Detail != "" && !h.IsMarker {
		if extra := enrichment.NonRedundantDetail(node.Name, h.Detail); extra != "" {
			label = fmt.Sprintf("%s  %s", label, colorizeText(extra, "gray"))
		}
	}

	name = displayName(h, icon, label)
	clock = fmt.Sprintf("(%s)", utils.HumanizeTime(duration.Seconds()))
	if h.IsMarker {
		clock = ""
	}
	return name, clock
}

func displayName(h enrichment.SpanHints, icon, label string) string {
	if h.IsMarker {
		ch := h.BarChar
		if ch == "" {
			ch = "█"
		}
		if markerWidth(ch) == 1 {
			return fmt.Sprintf("%s     %s", icon, label)
		}
		return fmt.Sprintf("%s    %s", icon, label)
	}
	if h.Outcome == "failure" {
		return fmt.Sprintf("%s %s ❌", icon, label)
	}
	return fmt.Sprintf("%s %s", icon, label)
}

// markerWidth measures the marker glyph's display width. Guessing from the
// event type drifted from the glyphs enrichment actually emits, leaving
// marker rows a column short of the box border.
func markerWidth(barChar string) int {
	if w := lipgloss.Width(barChar); w > 0 {
		return w
	}
	return 1
}

// colorizeText applies terminal color based on color name.
func colorizeText(text, color string) string {
	switch color {
	case "green":
		return utils.GreenText(text)
	case "red":
		return utils.RedText(text)
	case "blue":
		return utils.BlueText(text)
	case "yellow":
		return utils.YellowText(text)
	case "gray":
		return utils.GrayText(text)
	default:
		return utils.BlueText(text)
	}
}

func countReviewEvents(events []analyzer.ReviewEvent, eventType string) int {
	count := 0
	for _, ev := range events {
		if ev.Type == eventType {
			count++
		}
	}
	return count
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
