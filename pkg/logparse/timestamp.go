package logparse

import (
	"fmt"
	"strings"
	"time"
)

// TimestampParser is a generic fallback parser that creates spans based on
// timestamp gaps between log lines. It works on any log format.
//
// It recognizes GHA ##[group]/##[endgroup] markers as structural delimiters:
// groups become parent spans with their content lines as children.
type TimestampParser struct {
	// MinGapDuration is the minimum gap between log lines to start a new span.
	// Defaults to 1 second if zero.
	MinGapDuration time.Duration

	// MinSpanDuration filters out spans shorter than this duration.
	// Defaults to 100ms if zero.
	MinSpanDuration time.Duration
}

func (p *TimestampParser) Name() string { return "timestamp" }

// Match always returns true — this is the fallback parser.
func (p *TimestampParser) Match(lines []LogLine) bool { return true }

// groupBlock represents a ##[group]...##[endgroup] section in the log.
type groupBlock struct {
	name    string
	start   time.Time
	end     time.Time
	lines   []LogLine // lines inside the group (excluding markers)
	lineNum int       // line number of the ##[group] marker
}

// splitGroups partitions log lines into top-level lines and group blocks.
// Returns (top-level lines, group blocks). Top-level lines are those outside
// any ##[group]...##[endgroup] pair.
func splitGroups(lines []LogLine) ([]LogLine, []groupBlock) {
	var topLevel []LogLine
	var groups []groupBlock
	var current *groupBlock

	for _, l := range lines {
		content := l.Content
		if name, ok := strings.CutPrefix(content, "##[group]"); ok {
			current = &groupBlock{
				name:    name,
				start:   l.Time,
				lineNum: l.LineNum,
			}
			continue
		}
		if strings.HasPrefix(content, "##[endgroup]") {
			if current != nil {
				current.end = l.Time
				groups = append(groups, *current)
				current = nil
			}
			continue
		}
		if current != nil {
			current.lines = append(current.lines, l)
		} else {
			topLevel = append(topLevel, l)
		}
	}
	// Unclosed group: treat remaining lines as group content
	if current != nil {
		if len(current.lines) > 0 {
			current.end = current.lines[len(current.lines)-1].Time
		} else {
			current.end = current.start
		}
		groups = append(groups, *current)
	}

	return topLevel, groups
}

func (p *TimestampParser) Parse(lines []LogLine, stepStart, stepEnd time.Time) []ParsedSpan {
	if len(lines) == 0 {
		return nil
	}

	topLevel, groups := splitGroups(lines)

	// If there are groups, build a span tree with groups as parents
	if len(groups) > 0 {
		return p.parseWithGroups(topLevel, groups, stepStart, stepEnd)
	}

	// No groups — use the original gap-based parsing on all lines
	return p.parseGapBased(lines, stepStart, stepEnd)
}

// parseWithGroups builds spans from a mix of top-level lines and group blocks.
// Each group becomes a parent span; substantive lines within groups become children.
// Top-level lines between groups become their own spans if they have meaningful duration.
func (p *TimestampParser) parseWithGroups(topLevel []LogLine, groups []groupBlock, stepStart, stepEnd time.Time) []ParsedSpan {
	// Collect all span-producing elements with their start times for ordering
	var elements []spanElement

	// Process each group into a parent span with children
	for i, g := range groups {
		endTime := g.end
		// If there's a next group, the group span extends to the next group's start
		if i+1 < len(groups) {
			endTime = groups[i+1].start
		} else if !stepEnd.IsZero() {
			// Last group extends to the step end (or next top-level line)
			endTime = stepEnd
		}

		groupLineNum := fmt.Sprintf("%d", g.lineNum)
		groupSpan := ParsedSpan{
			Name:      TruncateName(g.name, 80),
			StartTime: g.start,
			EndTime:   endTime,
			Attributes: map[string]string{
				"log.line_number": groupLineNum,
				"log.group":      g.name,
			},
		}

		// Parse children within the group using gap-based parsing
		if len(g.lines) > 0 {
			children := p.parseGapBased(g.lines, g.start, endTime)
			// Override child line numbers to point to the group header,
			// because GHA collapses groups by default and deep links to
			// lines inside collapsed groups don't scroll correctly.
			for i := range children {
				children[i].Attributes["log.line_number"] = groupLineNum
			}
			groupSpan.Children = children
		}

		elements = append(elements, spanElement{start: g.start, span: groupSpan})
	}

	// Process top-level lines (outside groups) into gap-based spans
	if len(topLevel) > 0 {
		topSpans := p.parseGapBased(topLevel, stepStart, stepEnd)
		for _, s := range topSpans {
			elements = append(elements, spanElement{start: s.StartTime, span: s})
		}
	}

	// Sort by start time to maintain chronological order
	sortSpanElements(elements)

	spans := make([]ParsedSpan, len(elements))
	for i, e := range elements {
		spans[i] = e.span
	}
	return spans
}

// spanElement pairs a start time with a span for sorting.
type spanElement struct {
	start time.Time
	span  ParsedSpan
}

// sortSpanElements sorts elements by start time (simple insertion sort for small slices).
func sortSpanElements(elements []spanElement) {
	for i := 1; i < len(elements); i++ {
		for j := i; j > 0 && elements[j].start.Before(elements[j-1].start); j-- {
			elements[j], elements[j-1] = elements[j-1], elements[j]
		}
	}
}

// parseGapBased groups consecutive lines by timestamp gaps and returns spans.
// This is the original parsing logic, now used both at top level and within groups.
func (p *TimestampParser) parseGapBased(lines []LogLine, regionStart, regionEnd time.Time) []ParsedSpan {
	if len(lines) == 0 {
		return nil
	}

	minGap := p.MinGapDuration
	if minGap == 0 {
		minGap = time.Second
	}
	minSpan := p.MinSpanDuration
	if minSpan == 0 {
		minSpan = 100 * time.Millisecond
	}

	// Group consecutive lines where inter-line gaps are < minGap
	type group struct {
		lines []LogLine
		start time.Time
		end   time.Time
	}
	var groups []group
	current := group{
		lines: []LogLine{lines[0]},
		start: lines[0].Time,
		end:   lines[0].Time,
	}

	for i := 1; i < len(lines); i++ {
		gap := lines[i].Time.Sub(current.end)
		if gap >= minGap {
			groups = append(groups, current)
			current = group{
				lines: []LogLine{lines[i]},
				start: lines[i].Time,
				end:   lines[i].Time,
			}
		} else {
			current.lines = append(current.lines, lines[i])
			current.end = lines[i].Time
		}
	}
	groups = append(groups, current)

	// A single group means no meaningful sub-step decomposition — skip.
	if len(groups) <= 1 {
		return nil
	}

	// Convert groups to spans, using the next group's start as the end time
	var spans []ParsedSpan
	for i, g := range groups {
		var endTime time.Time
		if i+1 < len(groups) {
			endTime = groups[i+1].start
		} else {
			endTime = regionEnd
		}

		duration := endTime.Sub(g.start)
		if duration < minSpan {
			continue
		}

		name := spanNameFromLines(g.lines)
		// Skip groups where no substantive line could be found
		if strings.HasPrefix(name, "log group") {
			continue
		}
		spans = append(spans, ParsedSpan{
			Name:      name,
			StartTime: g.start,
			EndTime:   endTime,
			Attributes: map[string]string{
				"log.line_count":  fmt.Sprintf("%d", len(g.lines)),
				"log.line_number": fmt.Sprintf("%d", g.lines[0].LineNum),
			},
		})
	}

	return spans
}

// spanNameFromLines picks a representative name from a group of log lines.
// It prefers the first non-empty, substantive line content.
func spanNameFromLines(lines []LogLine) string {
	for _, l := range lines {
		content := strings.TrimSpace(l.Content)
		if isBoilerplateLine(content) {
			continue
		}
		if content != "" {
			return TruncateName(content, 80)
		}
	}
	if len(lines) > 0 {
		return fmt.Sprintf("log group (line %d)", lines[0].LineNum)
	}
	return "log group"
}

// isBoilerplateLine returns true for log lines that are GHA workflow
// commands, action parameter blocks, or other non-substantive content
// that shouldn't become span names.
func isBoilerplateLine(content string) bool {
	if content == "" {
		return true
	}
	// GHA workflow commands
	if strings.HasPrefix(content, "##[") {
		return true
	}
	// Action parameter lines ("with:", "  key: value", "env:")
	if content == "with:" || content == "env:" {
		return true
	}
	// Indented key-value pairs from action parameters
	if strings.HasPrefix(content, "  ") && strings.Contains(content, ": ") {
		return true
	}
	// Git shell commands (prefixed with [command])
	if strings.HasPrefix(content, "[command]") {
		return true
	}
	return false
}
