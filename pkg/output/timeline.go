package output

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/sdk/trace"
)

// SpanNode represents a node in the OTel span hierarchy tree.
type SpanNode struct {
	Span     trace.ReadOnlySpan
	Attrs    map[string]string
	Hints    enrichment.SpanHints
	Children []*SpanNode
}

// BuildSpanTree constructs a hierarchy of spans based on ParentSpanID.
func BuildSpanTree(spans []trace.ReadOnlySpan) []*SpanNode {
	nodes := make(map[string]*SpanNode)
	var roots []*SpanNode

	// Create nodes for all spans
	for _, s := range spans {
		nodes[s.SpanContext().SpanID().String()] = &SpanNode{Span: s}
	}

	// Link children to parents
	for _, s := range spans {
		node := nodes[s.SpanContext().SpanID().String()]
		if node == nil {
			continue
		}
		parentID := s.Parent().SpanID().String()

		if parentID == "0000000000000000" {
			roots = append(roots, node)
		} else if parent, ok := nodes[parentID]; ok {
			parent.Children = append(parent.Children, node)
		} else {
			roots = append(roots, node)
		}
	}

	sortNodes(roots)
	for _, n := range nodes {
		sortNodes(n.Children)
	}

	return roots
}

// BuildEnrichedSpanTree filters spans using the enricher, deduplicates, and builds the tree.
func BuildEnrichedSpanTree(spans []trace.ReadOnlySpan, enricher enrichment.Enricher, globalEarliest, globalLatest time.Time) []*SpanNode {
	var filtered []trace.ReadOnlySpan
	seenDedup := make(map[string]struct{})

	for _, s := range spans {
		attrs := make(map[string]string)
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}

		isZeroDuration := s.EndTime().Before(s.StartTime()) || s.EndTime().Equal(s.StartTime())
		hints := enricher.Enrich(s.Name(), attrs, isZeroDuration)
		if hints.Category == "" {
			continue
		}

		if !globalEarliest.IsZero() && s.EndTime().Before(globalEarliest) {
			continue
		}
		if !globalLatest.IsZero() && s.StartTime().After(globalLatest) {
			continue
		}

		if hints.DedupKey != "" {
			if _, seen := seenDedup[hints.DedupKey]; seen {
				continue
			}
			seenDedup[hints.DedupKey] = struct{}{}
		}

		filtered = append(filtered, s)
	}

	if len(filtered) == 0 {
		return nil
	}

	// Build tree, then enrich each node
	roots := BuildSpanTree(filtered)
	enrichNodes(roots, enricher)
	return roots
}

// enrichNodes enriches each SpanNode in-place with attrs and hints.
func enrichNodes(nodes []*SpanNode, enricher enrichment.Enricher) {
	for _, n := range nodes {
		n.Attrs = make(map[string]string)
		for _, a := range n.Span.Attributes() {
			n.Attrs[string(a.Key)] = a.Value.AsString()
		}
		isZeroDuration := n.Span.EndTime().Before(n.Span.StartTime()) || n.Span.EndTime().Equal(n.Span.StartTime())
		n.Hints = enricher.Enrich(n.Span.Name(), n.Attrs, isZeroDuration)
		enrichNodes(n.Children, enricher)
	}
}

func sortNodes(nodes []*SpanNode) {
	sort.Slice(nodes, func(i, j int) bool {
		sI, sJ := nodes[i].Span, nodes[j].Span
		if sI.StartTime().Equal(sJ.StartTime()) {
			// Use hints sort priority if available, otherwise fall back to type
			if nodes[i].Hints.SortPriority != nodes[j].Hints.SortPriority {
				return nodes[i].Hints.SortPriority < nodes[j].Hints.SortPriority
			}
			typeI := getSpanType(sI)
			typeJ := getSpanType(sJ)
			if typeI != typeJ {
				if typeI == "marker" {
					return true
				}
				if typeJ == "marker" {
					return false
				}
			}
		}
		return sI.StartTime().Before(sJ.StartTime())
	})
}

func getSpanType(s trace.ReadOnlySpan) string {
	for _, attr := range s.Attributes() {
		if attr.Key == "type" {
			return attr.Value.AsString()
		}
	}
	return ""
}

// RenderOTelTimeline renders a generic OTel span tree as a terminal waterfall.
func RenderOTelTimeline(w io.Writer, spans []trace.ReadOnlySpan, globalEarliest, globalLatest time.Time, enricher enrichment.Enricher) {
	if len(spans) == 0 {
		return
	}
	roots := BuildEnrichedSpanTree(spans, enricher, globalEarliest, globalLatest)
	if len(roots) == 0 {
		return
	}

	// Find overall time bounds
	earliest := globalEarliest
	latest := globalLatest

	if earliest.IsZero() || latest.IsZero() {
		earliest = roots[0].Span.StartTime()
		latest = roots[0].Span.EndTime()
		var walk func([]*SpanNode)
		walk = func(nodes []*SpanNode) {
			for _, n := range nodes {
				if n.Span.StartTime().Before(earliest) {
					earliest = n.Span.StartTime()
				}
				if n.Span.EndTime().After(latest) {
					latest = n.Span.EndTime()
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

func getMarkerWidth(eventType string) int {
	switch eventType {
	case "merged", "default":
		return 1 // ◆, ▲ are 1 cell
	case "commit":
		return 2 // 📍 is 2 cells
	case "push":
		return 2 // 🚀 is 2 cells
	case "comment", "commented":
		return 2 // 💬 is 2 cells
	case "approved":
		return 2 // ✅ is 2 cells
	case "changes_requested":
		return 2 // ❌ is 2 cells
	default:
		return 2
	}
}

func renderNode(w io.Writer, node *SpanNode, depth int, globalStart time.Time, totalDuration time.Duration, scale int) {
	s := node.Span
	h := node.Hints

	// Clamp start and end times to the global window for visualization
	startT := s.StartTime()
	if startT.Before(globalStart) {
		startT = globalStart
	}
	endT := s.EndTime()
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

	// Use hints for icon, falling back for step alignment
	icon := h.Icon
	if icon == "" {
		icon = "• "
	}
	// Steps need leading spaces for indentation alignment
	if h.IsLeaf && icon == "↳" {
		icon = "  ↳"
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
	markerWidth := 1
	if h.IsMarker {
		// Markers render as a single character
		markerWidth = getMarkerWidth(h.EventType)
		coloredBar = colorizeText(h.BarChar, h.Color)
	} else {
		coloredBar = colorizeText(coloredBar, h.Color)
	}

	indent := strings.Repeat("  ", depth)
	remainingCount := scale - startPos - maxInt(1, clampedLength)
	if h.IsMarker {
		remainingCount = scale - startPos - markerWidth
	}
	remaining := strings.Repeat(" ", maxInt(0, remainingCount))

	label := s.Name()
	if h.User != "" {
		label = fmt.Sprintf("%s by %s", label, h.User)
	}
	if h.URL != "" {
		label = utils.MakeClickableLink(h.URL, label)
	}

	// Pad icons to ensure consistent labeling alignment
	var displayName string
	if h.IsMarker {
		if markerWidth == 1 {
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
