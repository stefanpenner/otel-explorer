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

// markerWidth measures the marker glyph's display width. Guessing from the
// event type drifted from the glyphs enrichment actually emits, leaving
// marker rows a column short of the box border.
func markerWidth(barChar string) int {
	if w := lipgloss.Width(barChar); w > 0 {
		return w
	}
	return 1
}

func renderNode(w io.Writer, node *analyzer.TreeNode, depth int, globalStart time.Time, totalDuration time.Duration, scale int) {
	h := node.Hints

	// Clamp start and end times to the global window for visualization
	startT := node.StartTime
	if startT.Before(globalStart) {
		startT = globalStart
	}
	endT := node.EndTime
	if endT.After(globalStart.Add(totalDuration)) {
		endT = globalStart.Add(totalDuration)
	}

	if endT.Before(startT) {
		return // Span is entirely outside the window
	}

	start := startT.Sub(globalStart)
	duration := endT.Sub(startT)

	startPos := int(float64(start) / float64(totalDuration) * float64(scale))
	barLength := maxInt(1, int(float64(duration)/float64(totalDuration)*float64(scale)))
	clampedLength := minInt(barLength, scale-startPos)

	padding := strings.Repeat(" ", maxInt(0, startPos))

	// Use hints for icon. Do NOT bake indentation spaces into the icon: the depth
	// indent (below) is what conveys hierarchy. Padding the leaf icon with spaces
	// made a depth-N leaf align with a depth-(N+1) child, so child spans (e.g. a
	// tool span under its step) looked like siblings instead of nested.
	icon := h.Icon
	if icon == "" {
		icon = "• "
	}

	statusIcon := "  "
	if h.Outcome == "failure" {
		statusIcon = "❌"
	}

	// Build bar
	barChar := h.BarChar
	if barChar == "" {
		barChar = "█"
	}

	coloredBar := strings.Repeat(barChar, maxInt(1, clampedLength))
	markerCells := 1
	if h.IsMarker {
		// Markers render as a single glyph; reserve its actual display width
		markerCells = markerWidth(barChar)
		coloredBar = colorizeText(barChar, h.Color)
	} else {
		coloredBar = colorizeText(coloredBar, h.Color)
	}

	indent := strings.Repeat("  ", depth)
	remainingCount := scale - startPos - maxInt(1, clampedLength)
	if h.IsMarker {
		remainingCount = scale - startPos - markerCells
	}
	remaining := strings.Repeat(" ", maxInt(0, remainingCount))

	label := node.Name
	if h.User != "" {
		label = fmt.Sprintf("%s by %s", label, h.User)
	}
	if h.URL != "" {
		label = utils.MakeClickableLink(h.URL, label)
	}
	// Append semantic detail (model, route, statement, token usage, …) so that
	// generic OTel spans — HTTP, DB, RPC, GenAI — read at a glance in the
	// waterfall rather than as anonymous bars. Markers carry no detail.
	if h.Detail != "" && !h.IsMarker {
		if extra := enrichment.NonRedundantDetail(node.Name, h.Detail); extra != "" {
			label = fmt.Sprintf("%s  %s", label, colorizeText(extra, "gray"))
		}
	}

	// Pad icons to ensure consistent labeling alignment
	var displayName string
	if h.IsMarker {
		if markerCells == 1 {
			displayName = fmt.Sprintf("%s     %s", icon, label)
		} else {
			displayName = fmt.Sprintf("%s    %s", icon, label)
		}
	} else {
		if statusIcon != "  " {
			displayName = fmt.Sprintf("%s %s %s", icon, label, statusIcon)
		} else {
			displayName = fmt.Sprintf("%s %s", icon, label)
		}
	}

	durationDisplay := fmt.Sprintf("(%s)", utils.HumanizeTime(duration.Seconds()))
	if h.IsMarker {
		durationDisplay = ""
	}

	fmt.Fprintf(w, "│%s%s%s  │ %s%s %s\n",
		padding, coloredBar, remaining,
		indent, displayName, durationDisplay)

	for _, child := range node.Children {
		renderNode(w, child, depth+1, globalStart, totalDuration, scale)
	}
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
