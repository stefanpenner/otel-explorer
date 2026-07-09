package results

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// timelineHyperlink wraps text in OSC 8 hyperlink with underline disabled.
func timelineHyperlink(url, text string) string {
	if url == "" {
		return text
	}
	// \x1b[24m disables underline
	// id parameter ensures terminals treat each link independently
	return fmt.Sprintf("\x1b]8;id=%s;%s\x07\x1b[24m%s\x1b[24m\x1b]8;;\x07", url, url, text)
}

// renderMarker renders a marker character with proper padding, handling width consistently.
// Returns left padding + styled marker + right padding, totaling exactly 'width' visual characters.
func renderMarker(markerChar string, style lipgloss.Style, startPos, width int, url string, applyStyle bool) string {
	// Use a fixed width for known markers to avoid terminal inconsistencies
	markerWidth := getMarkerWidth(markerChar)

	// Clamp position
	if startPos < 0 {
		startPos = 0
	}
	if startPos > width-markerWidth {
		startPos = width - markerWidth
	}
	if startPos < 0 {
		startPos = 0
	}

	leftPadCount := startPos
	rightPadCount := width - startPos - markerWidth
	if rightPadCount < 0 {
		rightPadCount = 0
	}

	// Build the content (styled marker with hyperlink)
	var styledMarker string
	if applyStyle {
		styledMarker = style.Render(markerChar)
	} else {
		styledMarker = markerChar
	}
	content := timelineHyperlink(url, styledMarker)

	// Build result with exact padding
	result := strings.Repeat(" ", leftPadCount) + content + strings.Repeat(" ", rightPadCount)

	// Validate and fix total width - measure only visible characters
	actualWidth := leftPadCount + markerWidth + rightPadCount
	if actualWidth < width {
		// Add missing spaces at end
		result += strings.Repeat(" ", width-actualWidth)
	}

	return result
}

// GetCharWidth returns the visual width of a character/emoji.
// Uses fixed values for known characters to ensure consistency across renders.
// This is exported so view.go can use it too.
func GetCharWidth(char string) int {
	switch char {
	case "📋", "⚙️", "❌":
		return 2
	case "◆", "✓", "✗", "▲", "|", "↳", "◷", "○", "▼", "▶", " ", "●", "◇", "◈", "★":
		return 1
	case "◆ ", "▲ ", "• ", "● ", "◇ ", "◈ ":
		return 2
	default:
		return lipgloss.Width(char)
	}
}

// getMarkerWidth returns the visual width of a marker character.
func getMarkerWidth(char string) int {
	return GetCharWidth(char)
}

// barGeometry holds the computed columns for a timeline bar.
type barGeometry struct {
	startPos  int  // 0-based start column
	barLength int  // number of columns for the bar (>=1)
	isZero    bool // true → render as a 1-column marker at startPos
}

// computeBarGeometry is the shared math for all 5 timeline-bar renderers:
// guard against degenerate windows, clamp item times to global bounds, detect
// zero-duration items, and compute start/length with minimum-1 and width
// clamping. Returns ok=false for degenerate inputs (caller renders blanks).
func computeBarGeometry(item TreeItem, globalStart, globalEnd time.Time, width int) (barGeometry, bool) {
	if globalEnd.Before(globalStart) || globalEnd.Equal(globalStart) || width <= 0 {
		return barGeometry{}, false
	}
	totalDuration := globalEnd.Sub(globalStart)

	itemStart := item.StartTime
	itemEnd := item.EndTime
	if itemStart.Before(globalStart) {
		itemStart = globalStart
	}
	if itemEnd.After(globalEnd) {
		itemEnd = globalEnd
	}

	if itemEnd.Before(itemStart) || itemEnd.Equal(itemStart) {
		startOffset := itemStart.Sub(globalStart)
		startPos := int(float64(startOffset) / float64(totalDuration) * float64(width))
		if startPos < 0 {
			startPos = 0
		}
		if startPos > width-1 {
			startPos = width - 1
		}
		return barGeometry{startPos: startPos, barLength: 1, isZero: true}, true
	}

	startOffset := itemStart.Sub(globalStart)
	endOffset := itemEnd.Sub(globalStart)
	startPos := int(float64(startOffset) / float64(totalDuration) * float64(width))
	endPos := int(float64(endOffset) / float64(totalDuration) * float64(width))

	barLength := endPos - startPos
	if barLength < 1 {
		barLength = 1
	}
	if startPos < 0 {
		startPos = 0
	}
	if startPos > width-1 {
		startPos = width - 1
	}
	if startPos+barLength > width {
		barLength = width - startPos
	}
	if barLength < 1 {
		barLength = 1
	}
	return barGeometry{startPos: startPos, barLength: barLength, isZero: false}, true
}

// RenderTimelineBar renders a timeline bar for a tree item
func RenderTimelineBar(item TreeItem, globalStart, globalEnd time.Time, width int, url string) string {
	geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
	if !ok {
		return strings.Repeat(" ", width)
	}
	if geo.isZero {
		markerChar, style := getBarStyle(item)
		if !item.Hints.IsMarker {
			markerChar = "|"
		}
		return renderMarker(markerChar, style, geo.startPos, width, url, true)
	}

	barChar, style := getBarStyle(item)
	leftPad := strings.Repeat(" ", geo.startPos)
	bar := buildBarWithDuration(barChar, geo.barLength, item, style, nil)
	rightPad := strings.Repeat(" ", width-geo.startPos-geo.barLength)
	return leftPad + timelineHyperlink(url, bar) + rightPad
}

// RenderTimelineBarSelected renders a timeline bar with dimmed colors and selection background
func RenderTimelineBarSelected(item TreeItem, globalStart, globalEnd time.Time, width int, url string) string {
	geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
	if !ok {
		return SelectedBgStyle.Render(strings.Repeat(" ", width))
	}
	if geo.isZero {
		markerChar, style := getBarStyleSelected(item)
		if !item.Hints.IsMarker {
			markerChar = "|"
		}
		return renderMarker(markerChar, style, geo.startPos, width, url, true)
	}

	barChar, style := getBarStyleSelected(item)
	labelStyle := lipgloss.NewStyle().Foreground(ColorWhite).Background(ColorSelectionBg)

	leftPad := SelectedBgStyle.Render(strings.Repeat(" ", geo.startPos))
	bar := buildBarWithDuration(barChar, geo.barLength, item, style, &labelStyle)
	rightPad := SelectedBgStyle.Render(strings.Repeat(" ", width-geo.startPos-geo.barLength))
	return leftPad + timelineHyperlink(url, bar) + rightPad
}

// renderTimelineBarWithBg renders a timeline bar with normal colors but applies
// bgStyle to the empty space (left/right padding) for a subtle row tint.
func renderTimelineBarWithBg(item TreeItem, globalStart, globalEnd time.Time, width int, url string, bgStyle lipgloss.Style) string {
	geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
	if !ok {
		return bgStyle.Render(strings.Repeat(" ", width))
	}
	if geo.isZero {
		markerChar, style := getBarStyle(item)
		if !item.Hints.IsMarker {
			markerChar = "|"
		}
		leftPad := bgStyle.Render(strings.Repeat(" ", geo.startPos))
		rightPad := bgStyle.Render(strings.Repeat(" ", width-geo.startPos-1))
		return leftPad + style.Render(markerChar) + rightPad
	}

	barChar, style := getBarStyle(item)
	leftPad := bgStyle.Render(strings.Repeat(" ", geo.startPos))
	bar := strings.Repeat(barChar, geo.barLength)
	rightPad := bgStyle.Render(strings.Repeat(" ", width-geo.startPos-geo.barLength))
	styledBar := style.Render(bar)
	return leftPad + timelineHyperlink(url, styledBar) + rightPad
}

// buildBarWithDuration renders a bar string, overlaying a short duration label
// inside the bar when there's enough room (bar length >= label length + 2).
// labelStyle controls the style of the duration text; if nil, barStyle is used for everything.
func buildBarWithDuration(barChar string, barLength int, item TreeItem, barStyle lipgloss.Style, labelStyle *lipgloss.Style) string {
	if item.StartTime.IsZero() || item.EndTime.IsZero() {
		return barStyle.Render(strings.Repeat(barChar, barLength))
	}
	dur := item.EndTime.Sub(item.StartTime).Seconds()
	if dur <= 0 {
		return barStyle.Render(strings.Repeat(barChar, barLength))
	}
	label := utils.HumanizeTime(dur)
	// Need at least 1 bar char on each side of the label
	if barLength < len(label)+2 {
		return barStyle.Render(strings.Repeat(barChar, barLength))
	}
	// Center the label in the bar
	leftBars := (barLength - len(label)) / 2
	rightBars := barLength - leftBars - len(label)
	// Use explicit labelStyle if provided, otherwise derive one from barStyle
	// with a subtle dark background so the numbers stand out slightly
	ls := barStyle.Background(ColorBarLabelBg)
	if labelStyle != nil {
		ls = *labelStyle
	}
	return barStyle.Render(strings.Repeat(barChar, leftBars)) +
		ls.Render(label) +
		barStyle.Render(strings.Repeat(barChar, rightBars))
}

// getBarStyle returns the bar character and style based on item hints.
func getBarStyle(item TreeItem) (string, lipgloss.Style) {
	barChar := item.Hints.BarChar
	if barChar == "" {
		barChar = "█"
	}
	style := hintsToBarStyle(item)
	return barChar, style
}

// hintsToBarStyle maps hints color/outcome to a lipgloss bar style.
func hintsToBarStyle(item TreeItem) lipgloss.Style {
	if item.Hints.Outcome == "failure" && !item.Hints.IsRequired {
		return BarFailureNonBlockingStyle
	}
	return colorToBarStyle(item.Hints.Color)
}

// colorToBarStyle maps a color name to a bar style.
func colorToBarStyle(color string) lipgloss.Style {
	switch color {
	case "green":
		return BarSuccessStyle
	case "red":
		return BarFailureStyle
	case "blue":
		return BarPendingStyle
	case "gray":
		return BarSkippedStyle
	case "yellow":
		return BarFailureNonBlockingStyle
	}
	return BarSkippedStyle
}

// getBarStyleSelected returns the bar character and dimmed style for selected items
func getBarStyleSelected(item TreeItem) (string, lipgloss.Style) {
	barChar := item.Hints.BarChar
	if barChar == "" {
		barChar = "█"
	}
	style := hintsToBarStyleSelected(item)
	return barChar, style
}

// hintsToBarStyleSelected maps hints to a selected bar style.
func hintsToBarStyleSelected(item TreeItem) lipgloss.Style {
	if item.Hints.Outcome == "failure" && !item.Hints.IsRequired {
		return BarFailureNonBlockingSelectedStyle
	}
	return colorToBarStyleSelected(item.Hints.Color)
}

// colorToBarStyleSelected maps a color name to a selected bar style.
func colorToBarStyleSelected(color string) lipgloss.Style {
	switch color {
	case "green":
		return BarSuccessSelectedStyle
	case "red":
		return BarFailureSelectedStyle
	case "blue":
		return BarPendingSelectedStyle
	case "gray":
		return BarSkippedSelectedStyle
	case "yellow":
		return BarFailureNonBlockingSelectedStyle
	}
	return BarSkippedSelectedStyle
}

// getChildMarkerStyle returns the dimmed style for a child marker based on its outcome
func getChildMarkerStyle(child *TreeItem) lipgloss.Style {
	switch child.Hints.Outcome {
	case "success":
		return BarChildSuccessStyle
	case "failure":
		return BarChildFailureStyle
	default:
		return BarChildDefaultStyle
	}
}

// getChildMarkerStyleSelected returns the dimmed+selected style for a child marker
func getChildMarkerStyleSelected(child *TreeItem) lipgloss.Style {
	switch child.Hints.Outcome {
	case "success":
		return BarChildSuccessSelectedStyle
	case "failure":
		return BarChildFailureSelectedStyle
	default:
		return BarChildDefaultSelectedStyle
	}
}

// childMarkerPos holds the timeline position and style for a single child marker
type childMarkerPos struct {
	pos   int
	style lipgloss.Style
}

// computeChildPositions calculates the timeline position for each immediate child.
// Hidden children (present in hiddenState) are excluded from markers.
func computeChildPositions(children []*TreeItem, globalStart, globalEnd time.Time, width int, styleFn func(*TreeItem) lipgloss.Style, hiddenState map[string]bool) []childMarkerPos {
	totalDuration := globalEnd.Sub(globalStart)
	if totalDuration <= 0 || width <= 0 {
		return nil
	}

	var positions []childMarkerPos
	for _, child := range children {
		if hiddenState[child.ID] {
			continue
		}
		childStart := child.StartTime
		if childStart.IsZero() {
			continue
		}
		if childStart.Before(globalStart) {
			childStart = globalStart
		}
		if childStart.After(globalEnd) {
			childStart = globalEnd
		}

		pos := int(float64(childStart.Sub(globalStart)) / float64(totalDuration) * float64(width))
		if pos >= width {
			pos = width - 1
		}
		if pos < 0 {
			pos = 0
		}

		positions = append(positions, childMarkerPos{pos: pos, style: styleFn(child)})
	}
	return positions
}

// renderTimelineWithChildren builds a timeline bar with child markers overlaid.
// The buffer is filled with child markers first, then the parent bar overwrites on top.
// styleFn selects the appropriate child style variant (normal vs selected).
// If bgStyle is non-nil, empty space gets that background (for search-match rows).
// If selected is true, parent uses selected styles and padding gets selection bg.
func renderTimelineWithChildren(item TreeItem, globalStart, globalEnd time.Time, width int, url string, selected bool, bgStyle *lipgloss.Style, hiddenState map[string]bool) string {
	if globalEnd.Before(globalStart) || globalEnd.Equal(globalStart) || width <= 0 {
		if selected {
			return SelectedBgStyle.Render(strings.Repeat(" ", width))
		}
		if bgStyle != nil {
			return bgStyle.Render(strings.Repeat(" ", width))
		}
		return strings.Repeat(" ", width)
	}

	totalDuration := globalEnd.Sub(globalStart)

	// Choose child style function based on mode
	childStyleFn := getChildMarkerStyle
	if selected {
		childStyleFn = getChildMarkerStyleSelected
	}

	// Compute child marker positions
	childPositions := computeChildPositions(item.Children, globalStart, globalEnd, width, childStyleFn, hiddenState)

	// Build buffer tracking what's at each position
	type cell struct {
		isChild bool
		style   lipgloss.Style
	}
	buf := make([]cell, width)

	// Place child markers
	for _, cp := range childPositions {
		buf[cp.pos] = cell{isChild: true, style: cp.style}
	}

	// Compute parent bar range
	parentStart := item.StartTime
	parentEnd := item.EndTime
	if parentStart.Before(globalStart) {
		parentStart = globalStart
	}
	if parentEnd.After(globalEnd) {
		parentEnd = globalEnd
	}

	isZeroDuration := parentEnd.Before(parentStart) || parentEnd.Equal(parentStart)

	var parentStartPos, parentBarLen int
	if isZeroDuration {
		startOffset := parentStart.Sub(globalStart)
		parentStartPos = int(float64(startOffset) / float64(totalDuration) * float64(width))
		if parentStartPos >= width {
			parentStartPos = width - 1
		}
		if parentStartPos < 0 {
			parentStartPos = 0
		}
		parentBarLen = 1
	} else {
		startOffset := parentStart.Sub(globalStart)
		endOffset := parentEnd.Sub(globalStart)
		parentStartPos = int(float64(startOffset) / float64(totalDuration) * float64(width))
		endPos := int(float64(endOffset) / float64(totalDuration) * float64(width))
		parentBarLen = endPos - parentStartPos
		if parentBarLen < 1 {
			parentBarLen = 1
		}
		if parentStartPos < 0 {
			parentStartPos = 0
		}
		if parentStartPos > width-1 {
			parentStartPos = width - 1
		}
		if parentStartPos+parentBarLen > width {
			parentBarLen = width - parentStartPos
		}
		if parentBarLen < 1 {
			parentBarLen = 1
		}
	}

	// Get parent bar character and style
	var barChar string
	var parentStyle lipgloss.Style
	if selected {
		barChar, parentStyle = getBarStyleSelected(item)
	} else {
		barChar, parentStyle = getBarStyle(item)
	}

	// For zero-duration non-marker, use | as indicator
	if isZeroDuration && !item.Hints.IsMarker {
		barChar = "|"
	}

	// Now build the output string by scanning the buffer and grouping runs
	var result strings.Builder
	i := 0
	for i < width {
		if i >= parentStartPos && i < parentStartPos+parentBarLen {
			// Parent bar region — render with duration label
			end := parentStartPos + parentBarLen
			if end > width {
				end = width
			}
			count := end - i
			var labelStyle *lipgloss.Style
			if selected {
				ls := lipgloss.NewStyle().Foreground(ColorWhite).Background(ColorSelectionBg)
				labelStyle = &ls
			}
			bar := buildBarWithDuration(barChar, count, item, parentStyle, labelStyle)
			if !selected && bgStyle == nil {
				bar = timelineHyperlink(url, bar)
			}
			result.WriteString(bar)
			i = end
		} else if buf[i].isChild {
			// Child marker
			result.WriteString(buf[i].style.Render("·"))
			i++
		} else {
			// Empty space — collect consecutive spaces
			j := i
			for j < width && j != parentStartPos && !buf[j].isChild {
				if j >= parentStartPos && j < parentStartPos+parentBarLen {
					break
				}
				j++
			}
			spaces := strings.Repeat(" ", j-i)
			if selected {
				spaces = SelectedBgStyle.Render(spaces)
			} else if bgStyle != nil {
				spaces = bgStyle.Render(spaces)
			}
			result.WriteString(spaces)
			i = j
		}
	}

	return result.String()
}

// RenderTimelineBarWithChildren renders a timeline bar with dimmed child markers for collapsed items.
// Hidden children are excluded from the child markers.
func RenderTimelineBarWithChildren(item TreeItem, globalStart, globalEnd time.Time, width int, url string, hiddenState map[string]bool) string {
	return renderTimelineWithChildren(item, globalStart, globalEnd, width, url, false, nil, hiddenState)
}

// RenderTimelineBarWithChildrenSelected renders a timeline bar with child markers and selection background.
// Hidden children are excluded from the child markers.
func RenderTimelineBarWithChildrenSelected(item TreeItem, globalStart, globalEnd time.Time, width int, url string, hiddenState map[string]bool) string {
	return renderTimelineWithChildren(item, globalStart, globalEnd, width, url, true, nil, hiddenState)
}

// renderTimelineBarWithChildrenBg renders a timeline bar with child markers and a custom background.
// Hidden children are excluded from the child markers.
func renderTimelineBarWithChildrenBg(item TreeItem, globalStart, globalEnd time.Time, width int, url string, bg lipgloss.Style, hiddenState map[string]bool) string {
	return renderTimelineWithChildren(item, globalStart, globalEnd, width, url, false, &bg, hiddenState)
}

// RenderTimelineBarDimmed renders a timeline bar in gray for items after the logical end.
// It preserves the bar shape but uses BarSkippedStyle (gray) for all elements.
func RenderTimelineBarDimmed(item TreeItem, globalStart, globalEnd time.Time, width int) string {
	geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
	if !ok {
		return strings.Repeat(" ", width)
	}
	if geo.isZero {
		markerChar := "|"
		if item.Hints.IsMarker {
			markerChar, _ = getBarStyle(item)
		}
		return renderMarker(markerChar, BarSkippedStyle, geo.startPos, width, "", true)
	}

	barChar, _ := getBarStyle(item)
	leftPad := strings.Repeat(" ", geo.startPos)
	bar := strings.Repeat(barChar, geo.barLength)
	rightPad := strings.Repeat(" ", width-geo.startPos-geo.barLength)
	return leftPad + BarSkippedStyle.Render(bar) + rightPad
}

// RenderTimelineBarDimmedSelected renders a dimmed timeline bar with selection background
func RenderTimelineBarDimmedSelected(item TreeItem, globalStart, globalEnd time.Time, width int) string {
	geo, ok := computeBarGeometry(item, globalStart, globalEnd, width)
	if !ok {
		return SelectedBgStyle.Render(strings.Repeat(" ", width))
	}
	if geo.isZero {
		markerChar := "|"
		if item.Hints.IsMarker {
			markerChar, _ = getBarStyle(item)
		}
		return renderMarker(markerChar, BarSkippedSelectedStyle, geo.startPos, width, "", true)
	}

	barChar, _ := getBarStyle(item)
	leftPad := SelectedBgStyle.Render(strings.Repeat(" ", geo.startPos))
	bar := strings.Repeat(barChar, geo.barLength)
	rightPad := SelectedBgStyle.Render(strings.Repeat(" ", width-geo.startPos-geo.barLength))
	return leftPad + BarSkippedSelectedStyle.Render(bar) + rightPad
}

// overlayLogicalEndLine replaces the character at visual column `col` in an
// ANSI-styled timeline string with a yellow "│". The replacement preserves
// total visible width. col must be in [0, width). If col < 0 the string is
// returned unchanged.
//
// When `selected` is true the marker gets a selection-bg behind it.
func overlayLogicalEndLine(timeline string, col, width int, selected bool) string {
	if col < 0 || col >= width {
		return timeline
	}

	// Walk the string tracking visible position. We split into three parts:
	// [before col] [char at col] [after col]
	// We rebuild: before + styled "│" + after
	bytes := []byte(timeline)
	visPos := 0
	i := 0
	beforeEnd := 0  // byte offset where col starts
	afterStart := 0 // byte offset where col+1 starts
	found := false

	for i < len(bytes) && visPos <= col {
		if bytes[i] == '\x1b' {
			// Skip ANSI escape sequence
			j := i + 1
			if j < len(bytes) && bytes[j] == '[' {
				// CSI sequence: ESC [ ... final_byte
				j++
				for j < len(bytes) && bytes[j] < 0x40 {
					j++
				}
				if j < len(bytes) {
					j++ // skip final byte
				}
			} else if j < len(bytes) && bytes[j] == ']' {
				// OSC sequence: ESC ] ... ST (ST = ESC \ or BEL)
				j++
				for j < len(bytes) {
					if bytes[j] == '\x07' {
						j++
						break
					}
					if bytes[j] == '\x1b' && j+1 < len(bytes) && bytes[j+1] == '\\' {
						j += 2
						break
					}
					j++
				}
			}
			i = j
			continue
		}

		// Visible character — decode UTF-8 rune
		if visPos == col {
			beforeEnd = i
			// Skip this one rune
			r := 1
			if bytes[i] >= 0x80 {
				// Multi-byte UTF-8: find rune length
				for r < 4 && i+r < len(bytes) && (bytes[i+r]&0xC0) == 0x80 {
					r++
				}
			}
			// The character at col might be wider than 1, but we treat it as 1 column
			// since timeline positions are 1:1 with width
			afterStart = i + r
			// Continue past any trailing ANSI sequences that belong to this char
			j := afterStart
			for j < len(bytes) && bytes[j] == '\x1b' {
				k := j + 1
				if k < len(bytes) && bytes[k] == '[' {
					k++
					for k < len(bytes) && bytes[k] < 0x40 {
						k++
					}
					if k < len(bytes) {
						k++
					}
				} else if k < len(bytes) && bytes[k] == ']' {
					k++
					for k < len(bytes) {
						if bytes[k] == '\x07' {
							k++
							break
						}
						if bytes[k] == '\x1b' && k+1 < len(bytes) && bytes[k+1] == '\\' {
							k += 2
							break
						}
						k++
					}
				}
				j = k
			}
			afterStart = j
			found = true
			break
		}

		// Advance past this rune
		if bytes[i] < 0x80 {
			i++
		} else {
			r := 1
			for r < 4 && i+r < len(bytes) && (bytes[i+r]&0xC0) == 0x80 {
				r++
			}
			i += r
		}
		visPos++
	}

	if !found {
		return timeline
	}

	// Build the replacement
	markerStyle := LogicalEndBadgeStyle
	if selected {
		markerStyle = LogicalEndBadgeStyle.Background(ColorSelectionBg)
	}
	marker := markerStyle.Render("│")

	return string(bytes[:beforeEnd]) + marker + string(bytes[afterStart:])
}

