package results

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

const (
	defaultTreeWidth = 55
	minTreeWidth     = 25
	maxTreeWidth     = 120
	treeWidthStep    = 5
	horizontalPad    = 2 // left/right padding for main view
)

// highlightMatch splits name into before/match/after and styles the match portion
// with charStyle, and the rest with rowStyle. The match is case-insensitive.
// All matching is done in rune space so multibyte/case-length-changing runes
// (e.g. Ⱥ -> ⱥ) cannot misalign or overrun the original string.
func highlightMatch(name, query string, charStyle, rowStyle lipgloss.Style) string {
	nameRunes := []rune(name)
	lowerName := make([]rune, len(nameRunes))
	for i, r := range nameRunes {
		lowerName[i] = unicode.ToLower(r)
	}
	lowerQuery := []rune(strings.ToLower(query))
	idx := runeIndex(lowerName, lowerQuery)
	if idx < 0 {
		return rowStyle.Render(name)
	}
	before := string(nameRunes[:idx])
	match := string(nameRunes[idx : idx+len(lowerQuery)])
	after := string(nameRunes[idx+len(lowerQuery):])

	var result string
	if before != "" {
		result += rowStyle.Render(before)
	}
	result += charStyle.Render(match)
	if after != "" {
		result += rowStyle.Render(after)
	}
	return result
}

// runeIndex returns the index in haystack of the first occurrence of needle,
// or -1 if needle is not present. An empty needle matches at index 0.
func runeIndex(haystack, needle []rune) int {
	if len(needle) == 0 {
		return 0
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		found := true
		for j, r := range needle {
			if haystack[i+j] != r {
				found = false
				break
			}
		}
		if found {
			return i
		}
	}
	return -1
}

// hyperlink wraps text in OSC 8 terminal hyperlink escape sequence.
// This makes the text clickable in supporting terminals (iTerm2, Kitty, WezTerm, etc.)
// The id parameter ensures terminals treat each link independently.
func hyperlink(url, text string) string {
	if url == "" {
		return text
	}
	// OSC 8 format with id: \x1b]8;id=ID;URL\x07TEXT\x1b]8;;\x07
	// Using URL as ID ensures same URLs are grouped, different URLs are independent
	return fmt.Sprintf("\x1b]8;id=%s;%s\x07%s\x1b]8;;\x07", url, url, text)
}

// infoColorStyle returns a style for info items based on the color hint.
func infoColorStyle(color string) lipgloss.Style {
	switch color {
	case "blue":
		return lipgloss.NewStyle().Foreground(ColorBlue)
	case "purple":
		return lipgloss.NewStyle().Foreground(ColorPurple)
	case "green":
		return lipgloss.NewStyle().Foreground(ColorGreen)
	case "red":
		return lipgloss.NewStyle().Foreground(ColorRed)
	default:
		return FooterStyle
	}
}

// renderDiffLabel renders a diff-style label like "Files: 3 changed (+45 / -23)"
// with the additions in green and deletions in red.
func renderDiffLabel(name, url string) string {
	// Parse the structured format: "Files: N changed (+A / -D)"
	addStyle := lipgloss.NewStyle().Foreground(ColorGreen)
	delStyle := lipgloss.NewStyle().Foreground(ColorRed)
	grayStyle := FooterStyle

	// Find the (+...) and (-...) parts
	plusIdx := strings.Index(name, "(+")
	if plusIdx < 0 {
		// Fallback: just render gray with hyperlink
		return hyperlink(url, grayStyle.Render(name))
	}

	prefix := name[:plusIdx]
	rest := name[plusIdx:]

	// rest looks like "(+45 / -23)"
	slashIdx := strings.Index(rest, " / ")
	if slashIdx < 0 {
		return hyperlink(url, grayStyle.Render(name))
	}

	addPart := rest[1:slashIdx]  // "+45"
	delPart := rest[slashIdx+3:] // "-23)"
	delPart = strings.TrimSuffix(delPart, ")")

	rendered := grayStyle.Render(prefix+"(") +
		addStyle.Render(addPart) +
		grayStyle.Render(" / ") +
		delStyle.Render(delPart) +
		grayStyle.Render(")")

	return hyperlink(url, rendered)
}

// colorForRate returns a style based on the success rate value
func colorForRate(rate float64) lipgloss.Style {
	switch {
	case rate >= 100:
		return lipgloss.NewStyle().Foreground(ColorGreen)
	case rate >= 80:
		return lipgloss.NewStyle().Foreground(ColorOffWhite) // normal
	case rate >= 50:
		return lipgloss.NewStyle().Foreground(ColorYellow)
	default:
		return lipgloss.NewStyle().Foreground(ColorMagenta)
	}
}

// renderHeader renders the title bar with statistics
func (m Model) renderHeader() string {
	width := m.width
	if width < 40 {
		width = 40
	}
	totalWidth := width - horizontalPad*2
	if totalWidth < 1 {
		totalWidth = 80
	}
	contentWidth := totalWidth - 4 // minus "│ " and " │"
	if contentWidth < 10 {
		contentWidth = 10
	}

	// Styles
	numStyle := lipgloss.NewStyle().Foreground(ColorBlue)
	sep := HeaderCountStyle.Render(" • ")

	// Build top border with embedded title badge
	titleBadge := ModalFloatingTitle.Render(" otel-explorer ")
	titleBadgeWidth := lipgloss.Width(titleBadge)
	leftPad := 2 // chars after "╭"
	rightPad := max(1, totalWidth-2-leftPad-titleBadgeWidth)
	topBorder := BorderStyle.Render("╭"+strings.Repeat("─", leftPad)) +
		titleBadge +
		BorderStyle.Render(strings.Repeat("─", rightPad)+"╮")

	// Helper to build a line with left content and optional right content
	buildLine := func(left, leftPlain, right, rightPlain string) string {
		leftWidth := lipgloss.Width(leftPlain)
		rightWidth := lipgloss.Width(rightPlain)
		middlePad := contentWidth - leftWidth - rightWidth
		if middlePad < 1 {
			middlePad = 1
		}
		return BorderStyle.Render("│") + " " + left + strings.Repeat(" ", middlePad) + right + " " + BorderStyle.Render("│")
	}

	// Helper to build a simple left-aligned line
	buildLeftLine := func(content, plain string) string {
		w := lipgloss.Width(plain)
		pad := contentWidth - w
		if pad < 0 {
			pad = 0
		}
		return BorderStyle.Render("│") + " " + content + strings.Repeat(" ", pad) + " " + BorderStyle.Render("│")
	}

	// Line 1: Success rates (was line 2)

	// Calculate rates
	successRate := float64(0)
	if m.displayedSummary.TotalRuns > 0 {
		successRate = float64(m.displayedSummary.SuccessfulRuns) / float64(m.displayedSummary.TotalRuns) * 100
	}
	jobSuccessRate := float64(0)
	if m.displayedSummary.TotalJobs > 0 {
		jobSuccessRate = float64(m.displayedSummary.TotalJobs-m.displayedSummary.FailedJobs) / float64(m.displayedSummary.TotalJobs) * 100
	}

	// Line 2: Success rates (left) + Counts (right)
	// Left side: "Workflows: 100% • Jobs: 100%"
	leftStyled := HeaderCountStyle.Render("Traces: ") + colorForRate(successRate).Render(fmt.Sprintf("%.0f%%", successRate)) +
		sep + HeaderCountStyle.Render("Spans: ") + colorForRate(jobSuccessRate).Render(fmt.Sprintf("%.0f%%", jobSuccessRate))
	leftPlain := fmt.Sprintf("Traces: %.0f%% • Spans: %.0f%%", successRate, jobSuccessRate)

	line1 := buildLine(leftStyled, leftPlain, "", "")

	// Line 3: Times (left) + Concurrency (right)
	wallTime := utils.HumanizeTime(float64(m.displayedWallTimeMs) / 1000)
	computeTime := utils.HumanizeTime(float64(m.displayedComputeMs) / 1000)

	leftStyled3 := HeaderCountStyle.Render("Wall: ") + numStyle.Render(wallTime) +
		sep + HeaderCountStyle.Render("Compute: ") + numStyle.Render(computeTime)
	leftPlain3 := fmt.Sprintf("Wall: %s • Compute: %s", wallTime, computeTime)

	// Add effective time when logical end marker is set
	if m.logicalEndID != "" {
		effectiveSecs := m.logicalEndTime.Sub(m.chartStart).Seconds()
		if effectiveSecs < 0 {
			effectiveSecs = 0
		}
		effectiveTime := utils.HumanizeTime(effectiveSecs)
		leftStyled3 += sep + HeaderCountStyle.Render("Effective: ") + LogicalEndBadgeStyle.Render(effectiveTime)
		leftPlain3 += fmt.Sprintf(" • Effective: %s", effectiveTime)
	}

	line2 := buildLine(leftStyled3, leftPlain3, "", "")

	// Line 4: Queue + Retry + Billable (conditional)
	line4 := ""
	{
		var parts []string
		var partsPlain []string

		// Queue time
		if m.displayedSummary.QueueCount > 0 {
			avgQ := utils.HumanizeTime(m.displayedSummary.AvgQueueTimeMs / 1000)
			maxQ := utils.HumanizeTime(m.displayedSummary.MaxQueueTimeMs / 1000)
			parts = append(parts, HeaderCountStyle.Render("Queue: avg ")+numStyle.Render(avgQ)+HeaderCountStyle.Render(" / max ")+numStyle.Render(maxQ))
			partsPlain = append(partsPlain, fmt.Sprintf("Queue: avg %s / max %s", avgQ, maxQ))
		}

		// Retry rate
		if m.displayedSummary.RetriedRuns > 0 && m.displayedSummary.TotalRuns > 0 {
			retryPct := fmt.Sprintf("%.0f%%", float64(m.displayedSummary.RetriedRuns)/float64(m.displayedSummary.TotalRuns)*100)
			parts = append(parts, HeaderCountStyle.Render("Retries: ")+numStyle.Render(retryPct))
			partsPlain = append(partsPlain, fmt.Sprintf("Retries: %s", retryPct))
		}

		// Billable total
		var totalBillableMs int64
		for _, ms := range m.displayedSummary.BillableMs {
			totalBillableMs += ms
		}
		if totalBillableMs > 0 {
			billStr := utils.HumanizeTime(float64(totalBillableMs) / 1000)
			parts = append(parts, HeaderCountStyle.Render("Billable: ")+numStyle.Render(billStr))
			partsPlain = append(partsPlain, fmt.Sprintf("Billable: %s", billStr))
		}

		// Artifacts
		if m.artifactsCount > 0 {
			artLabel := fmt.Sprintf("%d", m.artifactsCount)
			if m.artifactsSize != "" {
				artLabel += " (" + m.artifactsSize + ")"
			}
			parts = append(parts, HeaderCountStyle.Render("Artifacts: ")+numStyle.Render(artLabel))
			partsPlain = append(partsPlain, fmt.Sprintf("Artifacts: %s", artLabel))
		}

		if len(parts) > 0 {
			styled4 := strings.Join(parts, sep)
			plain4 := strings.Join(partsPlain, " • ")
			line4 = "\n" + buildLeftLine(styled4, plain4)
		}
	}

	return topBorder + "\n" + line1 + "\n" + line2 + line4
}

// renderTimeAxis renders the time axis row that sits above the timeline
// It shows start time aligned with left edge, duration centered, end time at right edge
func (m Model) renderTimeAxis() string {
	width := m.width
	if width < 40 {
		width = 40
	}
	totalWidth := width - horizontalPad*2
	if totalWidth < 1 {
		totalWidth = 80
	}

	// Match the structure of item rows: │ space tree │ timeline │
	treeW := m.treeWidth
	availableW := totalWidth - 4 // 3 border chars + 1 left padding
	timelineW := availableW - treeW
	if timelineW < 10 {
		timelineW = 10
	}

	// Tree part is empty (just padding to align with timeline)
	treePart := strings.Repeat(" ", treeW)

	// Build time axis for the timeline area
	if m.chartStart.IsZero() || m.chartEnd.IsZero() {
		// No time data, just return empty row
		return BorderStyle.Render("│") + " " + treePart + SeparatorStyle.Render("│") + strings.Repeat(" ", timelineW) + BorderStyle.Render("│")
	}

	startTime := m.chartStart.Format("15:04:05")
	endTime := m.chartEnd.Format("15:04:05")
	durationSecs := m.chartEnd.Sub(m.chartStart).Seconds()
	if durationSecs < 0 {
		durationSecs = 0
	}
	duration := utils.HumanizeTime(durationSecs)

	// Style for numeric values
	numStyle := lipgloss.NewStyle().Foreground(ColorBlue)

	startW := lipgloss.Width(startTime)
	durW := lipgloss.Width(duration)
	endW := lipgloss.Width(endTime)

	// Calculate gaps: start...duration...end to fill timelineW
	// We want duration roughly centered
	totalTextW := startW + durW + endW
	remainingSpace := timelineW - totalTextW
	if remainingSpace < 2 {
		remainingSpace = 2
	}

	// Put duration in center, start at left, end at right
	leftGap := (timelineW-durW)/2 - startW
	if leftGap < 1 {
		leftGap = 1
	}
	rightGap := timelineW - startW - leftGap - durW - endW
	if rightGap < 1 {
		rightGap = 1
	}

	// Build the timeline axis content as a rune buffer so we can overlay the ▼ marker
	axisRunes := make([]rune, timelineW)
	// Fill: start text, left gap dashes, duration text, right gap dashes, end text
	pos := 0
	for _, r := range startTime {
		if pos < timelineW {
			axisRunes[pos] = r
			pos++
		}
	}
	for i := 0; i < leftGap && pos < timelineW; i++ {
		axisRunes[pos] = '─'
		pos++
	}
	for _, r := range duration {
		if pos < timelineW {
			axisRunes[pos] = r
			pos++
		}
	}
	for i := 0; i < rightGap && pos < timelineW; i++ {
		axisRunes[pos] = '─'
		pos++
	}
	for _, r := range endTime {
		if pos < timelineW {
			axisRunes[pos] = r
			pos++
		}
	}
	// Fill remaining with spaces
	for pos < timelineW {
		axisRunes[pos] = ' '
		pos++
	}

	// Overlay ▼ marker at logical end position if set
	logicalEndPos := -1
	if m.logicalEndID != "" && !m.logicalEndTime.IsZero() && !m.chartStart.IsZero() && !m.chartEnd.IsZero() {
		chartDuration := m.chartEnd.Sub(m.chartStart)
		if chartDuration > 0 {
			endOffset := m.logicalEndTime.Sub(m.chartStart)
			logicalEndPos = int(float64(endOffset) / float64(chartDuration) * float64(timelineW))
			if logicalEndPos >= timelineW {
				logicalEndPos = timelineW - 1
			}
			if logicalEndPos < 0 {
				logicalEndPos = 0
			}
			axisRunes[logicalEndPos] = '▼'
		}
	}

	// Build styled output character by character
	var timelineContent strings.Builder
	for i, r := range axisRunes {
		ch := string(r)
		if i == logicalEndPos {
			timelineContent.WriteString(LogicalEndBadgeStyle.Render(ch))
		} else if r == '─' {
			timelineContent.WriteString(ch)
		} else {
			timelineContent.WriteString(numStyle.Render(ch))
		}
	}

	return BorderStyle.Render("│") + " " + treePart + SeparatorStyle.Render("│") + timelineContent.String() + BorderStyle.Render("│")
}

// renderItem paints an info row, or one tree cell, its timeline, then the borders.
func (m Model) renderItem(item TreeItem, isSelected bool, itemIdx int) string {
	timelineW := timelineWidth(m.width, m.treeWidth)
	if item.ItemType == ItemTypeInfo {
		return m.renderInfoItem(item, isSelected, itemIdx, m.treeWidth, timelineW)
	}

	tree, pad := m.paintTreeCell(item, itemIdx, isSelected)
	bar := m.itemTimeline(item, isSelected, timelineW)
	return m.finishItemRow(tree, pad, bar, item, isSelected)
}

// timelineWidth is the bar width inside │ + space + tree + │ + bar + │.
func timelineWidth(viewWidth, treeW int) int {
	if viewWidth < 40 {
		viewWidth = 40
	}
	total := viewWidth - horizontalPad*2
	if total < 1 {
		total = 80
	}
	return max(total-4-treeW, 10)
}

func (m Model) renderInfoItem(item TreeItem, isSelected bool, itemIdx, treeW, timelineW int) string {
	indent, _ := treeIndent(m.connectorsAt(itemIdx), item.Depth, ' ')
	indent += "  "
	pad := max(treeW-lipgloss.Width(indent+item.Name), 0)
	left := BorderStyle.Render("│") + " "
	mid := SeparatorStyle.Render("│")
	right := BorderStyle.Render("│")
	gap := strings.Repeat(" ", timelineW)
	if isSelected {
		return left + SelectedStyle.Render(indent+item.Name) + SelectedBgStyle.Render(strings.Repeat(" ", pad)) + mid + SelectedBgStyle.Render(gap) + right
	}
	return left + FooterStyle.Render(indent) + m.infoText(item) + strings.Repeat(" ", pad) + mid + gap + right
}

func (m Model) infoText(item TreeItem) string {
	if m.hiddenState[item.ID] || (m.isFocused && !m.focusedIDs[item.ID]) {
		return HiddenStyle.Render(item.Name)
	}
	if item.Hints.Category == "diff" {
		return renderDiffLabel(item.Name, item.Hints.URL)
	}
	return hyperlink(item.Hints.URL, infoColorStyle(item.Hints.Color).Render(item.Name))
}

func (m Model) connectorsAt(itemIdx int) []rune {
	if itemIdx < 0 || itemIdx >= len(m.treeConnectors) {
		return nil
	}
	return m.treeConnectors[itemIdx]
}

func treeIndent(connectors []rune, depth int, fallback rune) (styled, plain string) {
	var styledBuf, plainBuf strings.Builder
	for i := 0; i < depth; i++ {
		ch := fallback
		if i < len(connectors) {
			ch = connectors[i]
		}
		switch ch {
		case '├':
			styledBuf.WriteString(IndentGuideStyle.Render("├─"))
			plainBuf.WriteString("├─")
		case '└':
			styledBuf.WriteString(IndentGuideStyle.Render("└─"))
			plainBuf.WriteString("└─")
		case '│':
			styledBuf.WriteString(IndentGuideStyle.Render("│"))
			styledBuf.WriteString(" ")
			plainBuf.WriteString("│ ")
		default:
			styledBuf.WriteString("  ")
			plainBuf.WriteString("  ")
		}
	}
	return styledBuf.String(), plainBuf.String()
}

func (m Model) paintTreeCell(item TreeItem, itemIdx int, isSelected bool) (string, int) {
	indent, indentPlain := treeIndent(m.connectorsAt(itemIdx), item.Depth, '│')
	expand := expandMark(item.HasChildren, m.expandedState[item.ID])
	icon := getItemIcon(item)
	status := getStatusIcon(item)
	badges := getBadges(item)

	logicalEnd := item.ID == m.logicalEndID
	afterEnd := m.isAfterLogicalEnd(item)
	hidden := m.hiddenState[item.ID]
	dim := m.isFocused && !m.focusedIDs[item.ID]
	search := m.searchMatchIDs[item.ID]

	endW, hiddenW := 0, 0
	if logicalEnd {
		endW = 6 // len(" [end]")
	}
	if hidden {
		hiddenW = 2 // len(" ⊘")
	}

	name := m.rowName(item)
	hint, hintW := sourceHint(item.SourceHint)
	dur, durW := rowDuration(item)
	iconW := GetCharWidth(icon)
	badgeW := getBadgesWidth(badges)
	statusW := GetCharWidth(status)
	const expandW, gapW = 1, 1
	fixed := item.Depth*2 + expandW + gapW + iconW + gapW + hintW + durW + badgeW + endW + hiddenW + gapW + statusW
	name = fitName(name, max(m.treeWidth-fixed, 5))
	pad := max(m.treeWidth-fixed-lipgloss.Width(name), 0)

	switch {
	case isSelected || search:
		shown := linkedName(name, m.searchQuery, item.Hints.URL, isSelected, search)
		prefix := fmt.Sprintf("%s%s %s %s", indent, expand, icon, shown)
		style, bg := tintedStyle(isSelected, hidden, afterEnd)
		return tintedRow(prefix, hint, dur, badges, item, logicalEnd, hidden, style, bg), pad
	case dim || hidden || afterEnd:
		return mutedRow(indentPlain, expand, icon, name, hint, dur, badges, status, dim, hidden), pad
	default:
		shown := hyperlink(item.Hints.URL, name)
		return activeRow(indent, expand, icon, shown, hint, dur, badges, item, logicalEnd), pad
	}
}

func (m Model) rowName(item TreeItem) string {
	name := item.DisplayName
	if item.Hints.User != "" && item.Hints.IsMarker {
		name = fmt.Sprintf("%s by %s", name, item.Hints.User)
	}
	if item.Hints.Detail != "" && !item.Hints.IsMarker {
		if extra := enrichment.NonRedundantDetail(item.DisplayName, item.Hints.Detail); extra != "" {
			name = name + "  " + extra
		}
	}
	if phase := m.logFetchPhase(item.ID); phase != "" {
		name = fmt.Sprintf("%s %s %s", name, m.spinner.View(), phase)
	}
	return name
}

func sourceHint(label string) (string, int) {
	if label == "" {
		return "", 0
	}
	plain := " ← " + label
	return plain, lipgloss.Width(plain)
}

func rowDuration(item TreeItem) (string, int) {
	if item.StartTime.IsZero() || item.EndTime.IsZero() {
		return "", 0
	}
	secs := item.EndTime.Sub(item.StartTime).Seconds()
	if secs < 0 {
		secs = 0
	}
	s := fmt.Sprintf(" (%s)", utils.HumanizeTime(secs))
	return s, lipgloss.Width(s)
}

func expandMark(hasChildren, expanded bool) string {
	if !hasChildren {
		return " "
	}
	if expanded {
		return "▼"
	}
	return "▶"
}

func fitName(name string, maxWidth int) string {
	if lipgloss.Width(name) <= maxWidth {
		return name
	}
	var b strings.Builder
	w := 0
	for _, r := range name {
		rw := lipgloss.Width(string(r))
		if w+rw+3 > maxWidth {
			break
		}
		b.WriteRune(r)
		w += rw
	}
	return b.String() + "..."
}

func linkedName(name, query, url string, selected, search bool) string {
	shown := name
	if search && query != "" {
		charStyle := SearchCharStyle
		rowStyle := SearchRowStyle
		if selected {
			charStyle = SearchCharSelectedStyle
			rowStyle = SelectedStyle
		}
		shown = highlightMatch(name, query, charStyle, rowStyle)
	}
	return hyperlink(url, shown)
}

func tintedStyle(selected, hidden, afterEnd bool) (lipgloss.Style, lipgloss.Color) {
	if !selected {
		return SearchRowStyle, ColorSearchRowBg
	}
	style := SelectedStyle
	if hidden || afterEnd {
		style = HiddenSelectedStyle
	}
	return style, ColorSelectionBg
}

// tintedRow paints one selected or search row.
// Each segment carries the background: lipgloss Render ends in a reset.
func tintedRow(prefix, hint, dur, badges string, item TreeItem, logicalEnd, hidden bool, row lipgloss.Style, bg lipgloss.Color) string {
	tree := row.Render(prefix)
	if hint != "" {
		tree += lipgloss.NewStyle().Foreground(SourceHintColor(item.SourceHint)).Background(bg).Render(hint)
	}
	if dur != "" {
		tree += FooterStyle.Background(bg).Render(dur)
	}
	tree += row.Render(badges)
	if logicalEnd {
		tree += LogicalEndBadgeStyle.Background(bg).Render(" [end]")
	}
	tree += row.Render(" ") + getStyledStatusIconWithBg(item, bg)
	if hidden {
		tree += HiddenBadgeStyle.Background(bg).Render(" ⊘")
	}
	return tree
}

// mutedRow paints dim, hidden, and after-end rows as plain text.
// Inner ANSI from guides, links, or status colors would override the dim.
func mutedRow(indentPlain, expand, icon, name, hint, dur, badges, status string, dim, hidden bool) string {
	body := fmt.Sprintf("%s%s %s %s%s%s%s %s", indentPlain, expand, icon, name, hint, dur, badges, status)
	if dim {
		mark := ""
		if hidden {
			mark = " ⊘"
		}
		return FocusDimStyle.Render(body + mark)
	}
	painted := HiddenStyle.Render(body)
	if hidden {
		painted += HiddenBadgeStyle.Render(" ⊘")
	}
	return painted
}

func activeRow(indent, expand, icon, shown, hint, dur, badges string, item TreeItem, logicalEnd bool) string {
	styledHint := ""
	if hint != "" {
		styledHint = lipgloss.NewStyle().Foreground(SourceHintColor(item.SourceHint)).Render(hint)
	}
	styledDur := ""
	if dur != "" {
		styledDur = FooterStyle.Render(dur)
	}
	end := ""
	if logicalEnd {
		end = LogicalEndBadgeStyle.Render(" [end]")
	}
	return fmt.Sprintf("%s%s %s %s", indent, expand, icon, shown) + styledHint + styledDur + badges + end + fmt.Sprintf(" %s", getStyledStatusIcon(item))
}

func (m Model) itemTimeline(item TreeItem, isSelected bool, timelineW int) string {
	hidden := m.hiddenState[item.ID]
	afterEnd := m.isAfterLogicalEnd(item)
	search := m.searchMatchIDs[item.ID]
	collapsed := item.HasChildren && !m.expandedState[item.ID]

	var bar string
	switch {
	case hidden && isSelected:
		bar = SelectedBgStyle.Render(strings.Repeat(" ", timelineW))
	case hidden:
		bar = strings.Repeat(" ", timelineW)
	case afterEnd && isSelected:
		bar = RenderTimelineBarDimmedSelected(item, m.chartStart, m.chartEnd, timelineW)
	case afterEnd:
		bar = RenderTimelineBarDimmed(item, m.chartStart, m.chartEnd, timelineW)
	case isSelected && collapsed:
		bar = RenderTimelineBarWithChildrenSelected(item, m.chartStart, m.chartEnd, timelineW, "", m.hiddenState)
	case isSelected:
		bar = RenderTimelineBarSelected(item, m.chartStart, m.chartEnd, timelineW, "")
	case search && collapsed:
		bar = renderTimelineBarWithChildrenBg(item, m.chartStart, m.chartEnd, timelineW, item.Hints.URL, SearchRowBgStyle, m.hiddenState)
	case search:
		bar = renderTimelineBarWithBg(item, m.chartStart, m.chartEnd, timelineW, item.Hints.URL, SearchRowBgStyle)
	case collapsed:
		bar = RenderTimelineBarWithChildren(item, m.chartStart, m.chartEnd, timelineW, item.Hints.URL, m.hiddenState)
	default:
		bar = RenderTimelineBar(item, m.chartStart, m.chartEnd, timelineW, item.Hints.URL)
	}
	if col := m.logicalEndCol(timelineW); col >= 0 {
		bar = overlayLogicalEndLine(bar, col, timelineW, isSelected)
	}
	return bar
}

// finishItemRow joins borders, the tree cell, and the bar.
// Selection and search paint their own pad: a later reset would drop the background.
// A hidden row wraps the pad too, so the gray covers the gap.
func (m Model) finishItemRow(tree string, pad int, bar string, item TreeItem, isSelected bool) string {
	left := BorderStyle.Render("│") + " "
	mid := SeparatorStyle.Render("│")
	right := BorderStyle.Render("│")
	switch {
	case isSelected:
		return left + tree + SelectedBgStyle.Render(strings.Repeat(" ", pad)) + mid + bar + right
	case m.searchMatchIDs[item.ID]:
		return left + tree + SearchRowBgStyle.Render(strings.Repeat(" ", pad)) + mid + bar + right
	case m.hiddenState[item.ID]:
		tree += strings.Repeat(" ", pad)
		return left + HiddenStyle.Render(tree) + mid + bar + right
	default:
		tree += strings.Repeat(" ", pad)
		return left + tree + mid + bar + right
	}
}

// getItemIcon returns the icon for an item type.
// Uses hints.Icon when available, falling back to type-based defaults for
// synthetic items (URLGroup, ActivityGroup) that have no enrichment hints.
func getItemIcon(item TreeItem) string {
	switch item.ItemType {
	case ItemTypeURLGroup:
		return "◆ " // width 2
	case ItemTypeActivityGroup:
		if item.Hints.Icon != "" {
			return item.Hints.Icon
		}
		return "◇ " // width 2
	default:
		if item.Hints.Icon != "" {
			return item.Hints.Icon
		}
		return "• " // width 1 + 1 space = 2
	}
}

// getStatusIcon returns the status icon based on outcome
func getStatusIcon(item TreeItem) string {
	switch item.Hints.Outcome {
	case "pending":
		return "◷"
	case "success":
		return "✓"
	case "failure":
		return "✗"
	case "skipped":
		return "○"
	default:
		return " "
	}
}

// getStyledStatusIcon returns the status icon with color applied
func getStyledStatusIcon(item TreeItem) string {
	switch item.Hints.Outcome {
	case "pending":
		return PendingStyle.Render("◷")
	case "success":
		return SuccessStyle.Render("✓")
	case "failure":
		return FailureStyle.Render("✗")
	case "skipped":
		return SkippedStyle.Render("○")
	default:
		return " "
	}
}

// getStyledStatusIconWithBg returns the status icon with color and background
func getStyledStatusIconWithBg(item TreeItem, bg lipgloss.Color) string {
	bgStyle := lipgloss.NewStyle().Background(bg)
	switch item.Hints.Outcome {
	case "pending":
		return PendingStyle.Background(bg).Render("◷")
	case "success":
		return SuccessStyle.Background(bg).Render("✓")
	case "failure":
		return FailureStyle.Background(bg).Render("✗")
	case "skipped":
		return SkippedStyle.Background(bg).Render("○")
	default:
		return bgStyle.Render(" ")
	}
}

// getBadges returns badges for required and bottleneck status
func getBadges(item TreeItem) string {
	badges := ""
	if item.Hints.IsRequired {
		badges += " ●"
	}
	if item.IsBottleneck {
		badges += " ★"
	}
	return badges
}

// getBadgesWidth calculates the width of badges using fixed emoji widths
func getBadgesWidth(badges string) int {
	width := 0
	for _, r := range badges {
		s := string(r)
		width += GetCharWidth(s)
	}
	return width
}

// renderBreadcrumb renders a path breadcrumb for the selected item
func (m Model) renderBreadcrumb(totalWidth int) string {
	if len(m.visibleItems) == 0 || m.cursor >= len(m.visibleItems) {
		return ""
	}

	item := m.visibleItems[m.cursor]

	// Build path from item's parent chain
	var parts []string
	parts = append(parts, item.DisplayName)

	// Walk up parent IDs to build breadcrumb
	current := item.ParentID
	seen := make(map[string]bool)
	for current != "" && !seen[current] {
		seen[current] = true
		found := false
		for _, vi := range m.visibleItems {
			if vi.ID == current {
				parts = append([]string{vi.DisplayName}, parts...)
				current = vi.ParentID
				found = true
				break
			}
		}
		if !found {
			break
		}
	}

	// Render breadcrumb
	contentWidth := totalWidth - 4 // borders + padding
	if contentWidth < 10 {
		contentWidth = 10
	}

	var breadcrumb strings.Builder
	for i, part := range parts {
		if i > 0 {
			breadcrumb.WriteString(BreadcrumbSepStyle.Render(" › "))
		}
		if i == len(parts)-1 {
			breadcrumb.WriteString(BreadcrumbActiveStyle.Render(part))
		} else {
			breadcrumb.WriteString(BreadcrumbStyle.Render(part))
		}
	}

	bc := breadcrumb.String()
	bcWidth := lipgloss.Width(bc)
	if bcWidth > contentWidth {
		// Truncate from the left, keeping the rightmost parts
		bc = BreadcrumbSepStyle.Render("…› ") + BreadcrumbActiveStyle.Render(parts[len(parts)-1])
	}

	pad := contentWidth - lipgloss.Width(bc)
	if pad < 0 {
		pad = 0
	}

	return BorderStyle.Render("│") + " " + bc + strings.Repeat(" ", pad) + " " + BorderStyle.Render("│")
}

// renderFooter renders a LazyVim-style statusline with mode pill and segments
func (m Model) renderFooter() string {
	width := m.width
	if width < 40 {
		width = 40
	}
	totalWidth := width - horizontalPad*2
	if totalWidth < 1 {
		totalWidth = 80
	}

	sep := StatusSep.Render("│")

	// Left segments: contextual info
	var segments []string
	var segmentsPlain []string

	if m.searchQuery != "" && !m.isSearching {
		seg := StatusSegment.Foreground(ColorYellow).Render("/" + m.searchQuery)
		segments = append(segments, seg)
		segmentsPlain = append(segmentsPlain, " /"+m.searchQuery+" ")
	}

	if m.sortMode != SortByStartTime {
		seg := StatusSegment.Render("sort:" + m.sortMode.String())
		segments = append(segments, seg)
		segmentsPlain = append(segmentsPlain, " sort:"+m.sortMode.String()+" ")
	}

	if m.treeWidth != defaultTreeWidth {
		seg := StatusSegmentDim.Render(fmt.Sprintf("tree:%d", m.treeWidth))
		segments = append(segments, seg)
		segmentsPlain = append(segmentsPlain, fmt.Sprintf(" tree:%d ", m.treeWidth))
	}

	// Right side: help hint
	var helpHint string
	var helpHintPlain string
	mode := HelpModeNormal
	if m.isSearching {
		mode = HelpModeSearch
	} else if m.searchQuery != "" {
		mode = HelpModeSearchActive
	} else if m.showDetailModal {
		mode = HelpModeModal
	}
	helpHint = StatusSegmentDim.Render(m.keys.ShortHelpForMode(mode))
	helpHintPlain = " " + m.keys.ShortHelpForMode(mode) + " "

	// Build the line
	contentWidth := totalWidth - 2 // borders

	// Assemble left part
	var left, leftPlain string
	for i, seg := range segments {
		if i > 0 {
			left += sep
			leftPlain += "│"
		}
		left += seg
		leftPlain += segmentsPlain[i]
	}

	leftWidth := lipgloss.Width(leftPlain)
	rightWidth := lipgloss.Width(helpHintPlain)

	midPad := contentWidth - leftWidth - rightWidth
	if midPad < 1 {
		midPad = 1
	}

	statusLine := BorderStyle.Render("│") + " " + left + strings.Repeat(" ", midPad) + helpHint + BorderStyle.Render("│")

	// Breadcrumb line above status (only in normal mode with items)
	breadcrumb := ""
	if !m.isSearching && !m.showDetailModal && len(m.visibleItems) > 0 {
		breadcrumb = m.renderBreadcrumb(totalWidth) + "\n"
	}

	bottomBorder := BorderStyle.Render("╰" + strings.Repeat("─", max(0, totalWidth-2)) + "╯")

	return breadcrumb + statusLine + "\n" + bottomBorder
}

// placeModalCentered renders the modal centered on a dim background
func placeModalCentered(modal string, width, height int) string {
	modalLines := strings.Split(modal, "\n")

	// Get modal dimensions
	modalHeight := len(modalLines)
	modalWidth := 0
	for _, line := range modalLines {
		w := lipgloss.Width(line)
		if w > modalWidth {
			modalWidth = w
		}
	}

	// Calculate vertical padding to center
	topPadding := (height - modalHeight) / 2
	if topPadding < 0 {
		topPadding = 0
	}

	// Calculate horizontal padding to center
	leftPadding := (width - modalWidth) / 2
	if leftPadding < 0 {
		leftPadding = 0
	}

	// Build the output
	var result strings.Builder

	// Add top padding lines
	for i := 0; i < topPadding; i++ {
		result.WriteString(strings.Repeat(" ", width))
		result.WriteString("\n")
	}

	// Add modal lines with horizontal centering
	for _, line := range modalLines {
		lineWidth := lipgloss.Width(line)
		rightPadding := width - leftPadding - lineWidth
		if rightPadding < 0 {
			rightPadding = 0
		}
		result.WriteString(strings.Repeat(" ", leftPadding))
		result.WriteString(line)
		result.WriteString(strings.Repeat(" ", rightPadding))
		result.WriteString("\n")
	}

	// Add bottom padding to fill the screen
	linesWritten := topPadding + modalHeight
	for i := linesWritten; i < height; i++ {
		result.WriteString(strings.Repeat(" ", width))
		result.WriteString("\n")
	}

	return result.String()
}

// renderHelpModal renders the help modal with all key bindings
func (m Model) renderHelpModal() string {
	var b strings.Builder

	// Title
	// Title is rendered as floating badge on border

	// Key bindings
	keyStyle := lipgloss.NewStyle().Foreground(ColorBlue).Width(12)
	descStyle := lipgloss.NewStyle().Foreground(ColorWhite)

	for _, binding := range m.keys.FullHelp() {
		b.WriteString(keyStyle.Render(binding[0]))
		b.WriteString(descStyle.Render(binding[1]))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(FooterStyle.Render("Press Esc or ? to close"))

	rendered := ModalStyle.Render(b.String())
	return overlayFloatingTitle(rendered, " Keyboard Shortcuts ")
}

// renderInspectorLine renders a single inspector tree entry as a styled line.
func renderInspectorLine(entry FlatInspectorEntry, isSelected bool, maxValueWidth int, searchQuery string) string {
	indent := strings.Repeat("  ", entry.Depth)

	var indicator string
	if len(entry.Node.Children) > 0 {
		if entry.Node.Expanded {
			indicator = ModalIndicatorStyle.Render("▼ ")
		} else {
			childCount := countDescendants(entry.Node)
			indicator = ModalIndicatorStyle.Render(fmt.Sprintf("▶ (%d) ", childCount))
		}
	} else {
		indicator = "  "
	}

	var line string
	if entry.Node.IsSection {
		line = indent + indicator + ModalTitleStyle.Render("── "+entry.Node.Label+" ──")
	} else if entry.Node.Value != "" && entry.Node.Label != "" {
		label := ModalGroupLabelStyle.Render(entry.Node.Label)
		value := entry.Node.Value
		if entry.Node.IsURL {
			value = hyperlink(value, value)
		} else if lipgloss.Width(value) > maxValueWidth && maxValueWidth > 3 {
			value = ansi.Truncate(value, maxValueWidth, "...")
		}
		line = indent + indicator + label + " " + ModalValueStyle.Render(value)
	} else if entry.Node.Value != "" {
		value := entry.Node.Value
		if lipgloss.Width(value) > maxValueWidth && maxValueWidth > 3 {
			value = ansi.Truncate(value, maxValueWidth, "...")
		}
		line = indent + indicator + ModalValueStyle.Render(value)
	} else {
		line = indent + indicator + ModalGroupLabelStyle.Render(entry.Node.Label)
	}

	if isSelected {
		line = ModalSelectedStyle.Render(line)
	}

	return line
}

// countDescendants returns the total number of descendants (recursive) of a node.
func countDescendants(n *InspectorNode) int {
	count := len(n.Children)
	for _, ch := range n.Children {
		count += countDescendants(ch)
	}
	return count
}

// renderInspectorSidebar renders the left pane as vertical tabs.
// The selected tab has a connected look: " ▎Label  " with no right border,
// while unselected tabs are dimmed with a right border separator.
func (m Model) renderInspectorSidebar(maxHeight int) string {
	sidebarWidth := 20
	var lines []string

	for i, section := range m.inspectorNodes {
		label := SectionLabel(section)
		if lipgloss.Width(label) > sidebarWidth-4 {
			label = ansi.Truncate(label, sidebarWidth-4, "...")
		}

		isSelected := i == m.inspectorSidebarIdx
		if isSelected {
			// Active tab: accent bar + bold label + open right edge
			accent := ModalSidebarFocusCursor.Render("▎")
			if m.inspectorFocusLeft {
				lines = append(lines, accent+ModalSidebarSelectedStyle.Render(" "+label+" "))
			} else {
				lines = append(lines, accent+ModalValueStyle.Render(" "+label+" "))
			}
		} else {
			lines = append(lines, " "+ModalSidebarDimStyle.Render(" "+label))
		}
	}

	// Pad to fill height
	for len(lines) < maxHeight {
		lines = append(lines, "")
	}
	if len(lines) > maxHeight {
		lines = lines[:maxHeight]
	}

	// Pad each line to sidebar width
	var sb strings.Builder
	for i, line := range lines {
		w := lipgloss.Width(line)
		sb.WriteString(line)
		if w < sidebarWidth {
			sb.WriteString(strings.Repeat(" ", sidebarWidth-w))
		}
		if i < len(lines)-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// renderInspectorBreadcrumb renders the breadcrumb trail for inspector navigation.
func (m Model) renderInspectorBreadcrumb() string {
	if len(m.inspectorBreadcrumb) == 0 {
		return ""
	}
	var parts []string
	for _, entry := range m.inspectorBreadcrumb {
		name := entry.item.DisplayName
		if lipgloss.Width(name) > 20 {
			name = ansi.Truncate(name, 20, "...")
		}
		parts = append(parts, ModalBreadcrumbStyle.Render(name))
	}
	// Current item
	curName := m.modalItem.DisplayName
	if lipgloss.Width(curName) > 20 {
		curName = ansi.Truncate(curName, 20, "...")
	}
	parts = append(parts, ModalBreadcrumbActiveStyle.Render(curName))
	sep := ModalBreadcrumbSepStyle.Render(" › ")
	return strings.Join(parts, sep)
}

// renderInspectorHeader renders the header with name and summary info.
func (m Model) renderInspectorHeader(maxWidth int) string {
	item := m.modalItem
	if item == nil {
		return ""
	}

	// Name line
	name := ModalTitleStyle.Render(item.DisplayName)

	// Summary badges
	var badges []string
	if item.Hints.Outcome != "" {
		var style lipgloss.Style
		switch item.Hints.Outcome {
		case "success":
			style = SuccessStyle
		case "failure":
			style = FailureStyle
		case "skipped":
			style = SkippedStyle
		default:
			style = PendingStyle
		}
		badges = append(badges, style.Render(item.Hints.Outcome))
	}
	if !item.StartTime.IsZero() && !item.EndTime.IsZero() {
		dur := item.EndTime.Sub(item.StartTime).Seconds()
		if dur < 0 {
			dur = 0
		}
		badges = append(badges, FooterStyle.Render(utils.HumanizeTime(dur)))
	}
	if item.ItemType != ItemTypeURLGroup {
		badges = append(badges, FooterStyle.Render(item.ItemType.String()))
	}
	if item.Hints.ServiceName != "" {
		badges = append(badges, FooterStyle.Render(item.Hints.ServiceName))
	}
	if item.ScopeName != "" && item.ScopeName != "github.com/stefanpenner/otel-explorer/pkg/analyzer" {
		badges = append(badges, FooterStyle.Render(item.ScopeName))
	}
	if item.SpanID != "" {
		sid := item.SpanID
		if len(sid) > 10 {
			sid = sid[:10]
		}
		badges = append(badges, FooterStyle.Render(sid))
	}

	badgeLine := strings.Join(badges, FooterStyle.Render(" • "))

	header := name
	if badgeLine != "" {
		header += "  " + badgeLine
	}

	// Truncate if too wide
	if lipgloss.Width(header) > maxWidth-4 {
		header = ansi.Truncate(header, maxWidth-4, "…")
	}

	// URL on second line (clickable)
	if item.Hints.URL != "" {
		urlText := item.Hints.URL
		if lipgloss.Width(urlText) > maxWidth-4 {
			urlText = ansi.Truncate(urlText, maxWidth-4, "…")
		}
		urlLine := hyperlink(item.Hints.URL, FooterStyle.Render(urlText))
		header += "\n" + urlLine
	}

	return header
}

// modalContentHeight returns the number of content lines visible in the
// detail modal for the given modal maxHeight, mirroring renderDetailModal.
func (m Model) modalContentHeight(maxHeight int) int {
	contentMaxHeight := maxHeight - 4 // 2 for border, 2 for padding
	contentMaxHeight -= 4             // header (name + badges + separator) + footer
	if m.modalItem != nil && m.modalItem.Hints.URL != "" {
		contentMaxHeight-- // URL line in header
	}
	if len(m.inspectorBreadcrumb) > 0 {
		contentMaxHeight-- // breadcrumb line
	}
	if m.inspectorSearching || m.inspectorSearchQuery != "" {
		contentMaxHeight-- // search bar
	}
	if contentMaxHeight < 5 {
		contentMaxHeight = 5
	}
	return contentMaxHeight
}

// modalMaxScroll returns the maximum useful modalScroll value for the current
// window size, matching renderDetailModal's internal clamping.
func (m Model) modalMaxScroll() int {
	height := m.height
	if height < 10 {
		height = 10 // View() enforces the same minimum
	}
	maxScroll := len(m.inspectorFlat) - m.modalContentHeight(height-4)
	if maxScroll < 0 {
		return 0
	}
	return maxScroll
}

// renderDetailModal renders the two-pane detail modal for an item.
// Returns the rendered modal and the maximum scroll value.
func (m Model) renderDetailModal(maxHeight, maxWidth int) (string, int) {
	if m.modalItem == nil {
		return "", 0
	}

	flat := m.inspectorFlat

	// Calculate available height for content
	contentMaxHeight := m.modalContentHeight(maxHeight)
	hasBreadcrumb := len(m.inspectorBreadcrumb) > 0

	// Build search match set for highlighting
	searchMatchSet := make(map[int]bool)
	for _, idx := range m.inspectorSearchMatches {
		searchMatchSet[idx] = true
	}

	// -- Sidebar (vertical tabs) --
	sidebarWidth := 22
	sidebar := m.renderInspectorSidebar(contentMaxHeight)

	// Separator column
	var sepCol strings.Builder
	for i := 0; i < contentMaxHeight; i++ {
		sepCol.WriteString(SeparatorStyle.Render("│"))
		if i < contentMaxHeight-1 {
			sepCol.WriteString("\n")
		}
	}

	// -- Right pane (tree) --
	rightWidth := maxWidth - sidebarWidth - 7
	if rightWidth < 30 {
		rightWidth = 30
	}

	maxValueWidth := rightWidth - 10
	if maxValueWidth < 30 {
		maxValueWidth = 30
	}

	// Auto-scroll to keep cursor visible
	totalLines := len(flat)
	scroll := m.modalScroll
	if !m.inspectorFocusLeft && totalLines > 0 {
		if m.inspectorCursor < scroll {
			scroll = m.inspectorCursor
		}
		if m.inspectorCursor >= scroll+contentMaxHeight {
			scroll = m.inspectorCursor - contentMaxHeight + 1
		}
	}
	maxScroll := totalLines - contentMaxHeight
	if maxScroll < 0 {
		maxScroll = 0
	}
	if scroll > maxScroll {
		scroll = maxScroll
	}

	// Render right pane lines
	var rightLines []string
	if totalLines == 0 {
		rightLines = append(rightLines, ModalSidebarDimStyle.Render("  (empty)"))
	}
	for i, entry := range flat {
		isSelected := !m.inspectorFocusLeft && i == m.inspectorCursor
		line := renderInspectorLine(entry, isSelected, maxValueWidth, m.inspectorSearchQuery)

		if searchMatchSet[i] && !isSelected {
			line = ModalSearchMatchStyle.Render("│") + line
		} else {
			line = " " + line
		}

		rightLines = append(rightLines, line)
	}

	// Get visible right pane lines
	endIdx := scroll + contentMaxHeight
	if endIdx > len(rightLines) {
		endIdx = len(rightLines)
	}
	startIdx := scroll
	if startIdx > len(rightLines) {
		startIdx = len(rightLines)
	}
	visibleRight := rightLines[startIdx:endIdx]

	for len(visibleRight) < contentMaxHeight {
		visibleRight = append(visibleRight, "")
	}

	// Calculate max right pane width
	maxRightWidth := 0
	for _, line := range visibleRight {
		w := lipgloss.Width(line)
		if w > maxRightWidth {
			maxRightWidth = w
		}
	}
	if maxRightWidth < rightWidth {
		maxRightWidth = rightWidth
	}

	// Pad right lines to consistent width
	var rightPane strings.Builder
	for i, line := range visibleRight {
		w := lipgloss.Width(line)
		rightPane.WriteString(line)
		if w < maxRightWidth {
			rightPane.WriteString(strings.Repeat(" ", maxRightWidth-w))
		}
		if i < len(visibleRight)-1 {
			rightPane.WriteString("\n")
		}
	}

	// Join panes horizontally
	content := lipgloss.JoinHorizontal(lipgloss.Top, sidebar, sepCol.String(), rightPane.String())
	totalContentWidth := lipgloss.Width(content)

	// Build full modal content
	var b strings.Builder

	// Breadcrumb (if navigated into children)
	if hasBreadcrumb {
		bc := m.renderInspectorBreadcrumb()
		b.WriteString(bc + "\n")
	}

	// Header: name + summary badges
	header := m.renderInspectorHeader(totalContentWidth)
	b.WriteString(header + "\n")
	// Separator between header and content
	sepLine := strings.Repeat("─", totalContentWidth)
	b.WriteString(SeparatorStyle.Render(sepLine) + "\n")

	b.WriteString(content)
	b.WriteString("\n")

	// Search bar
	if m.inspectorSearching {
		searchPrompt := ModalSearchBarStyle.Render("/ " + m.inspectorSearchQuery + "█")
		if len(m.inspectorSearchMatches) > 0 {
			searchPrompt += FooterStyle.Render(fmt.Sprintf("  %d matches", len(m.inspectorSearchMatches)))
		}
		b.WriteString("\n" + searchPrompt)
	} else if m.inspectorSearchQuery != "" {
		matchInfo := FooterStyle.Render(fmt.Sprintf("/%s  %d matches • n/N jump • Esc clear", m.inspectorSearchQuery, len(m.inspectorSearchMatches)))
		b.WriteString("\n" + matchInfo)
	}

	// Footer
	b.WriteString("\n")
	var footerParts []string
	if totalLines > 0 {
		footerParts = append(footerParts, fmt.Sprintf("[%d/%d]", m.inspectorCursor+1, totalLines))
	}
	if m.inspectorCopyMsg != "" {
		footerParts = append(footerParts, ModalCopyFeedbackStyle.Render(m.inspectorCopyMsg))
	}
	footerParts = append(footerParts, "Tab pane • ←→ expand • /search • c copy • o open • [/] item • Esc back")
	footerText := FooterStyle.Render(strings.Join(footerParts, " "))
	b.WriteString(footerText)

	// Apply modal style
	modalStyle := ModalStyle.MaxWidth(maxWidth)
	renderedModal := modalStyle.Render(b.String())

	// Overlay floating title
	title := " Span Details "
	renderedModal = overlayFloatingTitle(renderedModal, title)

	// Add scrollbar for right pane
	if maxScroll > 0 {
		visibleCount := endIdx - startIdx
		renderedModal = addScrollbarToModal(renderedModal, scroll, maxScroll, visibleCount, totalLines)
	}

	return renderedModal, maxScroll
}

// overlayFloatingTitle replaces part of the first line of the modal
// (the top border) with a styled title badge, giving a "floating window" look.
func overlayFloatingTitle(modal, title string) string {
	lines := strings.Split(modal, "\n")
	if len(lines) == 0 {
		return modal
	}

	badge := ModalFloatingTitle.Render(title)
	badgeWidth := lipgloss.Width(badge)
	topLine := lines[0]
	topWidth := lipgloss.Width(topLine)

	// Need room for corner + padding + badge + padding + corner
	insertAt := 3
	if insertAt+badgeWidth >= topWidth-1 {
		return modal // too narrow, skip
	}

	// Use ANSI-aware truncation to splice the badge in
	prefix := ansi.Truncate(topLine, insertAt, "")
	suffix := ansi.TruncateLeft(topLine, insertAt+badgeWidth, "")
	lines[0] = prefix + badge + suffix

	return strings.Join(lines, "\n")
}

// addScrollbarToModal adds a scrollbar column to the right of the modal border
// The scrollbar is 80% of the modal height, vertically centered
func addScrollbarToModal(modal string, scroll, maxScroll, visibleCount, totalLines int) string {
	lines := strings.Split(modal, "\n")
	if len(lines) == 0 {
		return modal
	}

	// Calculate scrollbar track dimensions (80% height, centered)
	totalHeight := len(lines)
	trackHeight := totalHeight * 80 / 100
	if trackHeight < 3 {
		trackHeight = min(3, totalHeight)
	}
	topPadding := (totalHeight - trackHeight) / 2
	bottomPadding := totalHeight - trackHeight - topPadding

	// Calculate thumb size and position within the track
	thumbSize := max(1, trackHeight*visibleCount/totalLines)
	if thumbSize > trackHeight {
		thumbSize = trackHeight
	}

	thumbStart := 0
	if maxScroll > 0 {
		thumbStart = scroll * (trackHeight - thumbSize) / maxScroll
	}
	thumbEnd := thumbStart + thumbSize

	// Scrollbar style (subtle, matches separator)
	scrollStyle := lipgloss.NewStyle().Foreground(ColorGrayDim)
	thumbChar := scrollStyle.Render("┃")
	trackChar := scrollStyle.Render("│")

	var result strings.Builder
	for i, line := range lines {
		result.WriteString(line)

		// Determine scrollbar character for this line
		trackIndex := i - topPadding
		if i < topPadding || i >= totalHeight-bottomPadding {
			// Outside track area - no scrollbar character, just space
			result.WriteString(" ")
		} else if trackIndex >= thumbStart && trackIndex < thumbEnd {
			result.WriteString(thumbChar)
		} else {
			result.WriteString(trackChar)
		}

		if i < len(lines)-1 {
			result.WriteString("\n")
		}
	}

	return result.String()
}
