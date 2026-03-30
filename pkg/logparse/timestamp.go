package logparse

import (
	"fmt"
	"strings"
	"time"
	"unicode"
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
	return filterBySignificance(p.parseGapBased(lines, stepStart, stepEnd), stepStart, stepEnd)
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
			children := filterBySignificance(p.parseGapBased(g.lines, g.start, endTime), g.start, endTime)
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

	return groupByPrefix(collapseSpans(spans))
}

// filterBySignificance drops spans whose duration is less than 1% of the
// parent time range. This removes noise spans that don't meaningfully
// contribute to the flamechart.
func filterBySignificance(spans []ParsedSpan, parentStart, parentEnd time.Time) []ParsedSpan {
	parentDur := parentEnd.Sub(parentStart)
	if parentDur <= 0 || len(spans) == 0 {
		return spans
	}

	threshold := parentDur / 100 // 1%

	var result []ParsedSpan
	for _, s := range spans {
		dur := s.EndTime.Sub(s.StartTime)
		if dur >= threshold {
			// Also filter children recursively
			if len(s.Children) > 0 {
				s.Children = filterBySignificance(s.Children, s.StartTime, s.EndTime)
			}
			result = append(result, s)
		}
	}
	return result
}

// collapseSpans merges consecutive spans that have identical or non-substantive
// names into rolled-up summary spans. This reduces noise from repetitive log
// output (e.g., progress dots, repeated status lines).
func collapseSpans(spans []ParsedSpan) []ParsedSpan {
	if len(spans) == 0 {
		return spans
	}

	var result []ParsedSpan
	i := 0
	for i < len(spans) {
		s := spans[i]
		name := s.Name

		// Check if this span name is non-substantive (dots, punctuation, etc.)
		nonSubstantive := isNonSubstantiveName(name)

		// Look ahead for consecutive spans with the same name or that are
		// also non-substantive (merge different noise names together)
		j := i + 1
		for j < len(spans) {
			nextName := spans[j].Name
			if nonSubstantive && isNonSubstantiveName(nextName) {
				j++
			} else if name == nextName {
				j++
			} else {
				break
			}
		}

		count := j - i
		if count == 1 && !nonSubstantive {
			// Single, substantive span — keep as-is
			result = append(result, s)
		} else if nonSubstantive && count == 1 {
			// Single non-substantive span — skip it entirely if short
			dur := s.EndTime.Sub(s.StartTime)
			if dur >= 2*time.Second {
				s.Name = fmt.Sprintf("(%s)", s.Name)
				result = append(result, s)
			}
			// Otherwise drop it
		} else {
			// Multiple consecutive similar spans — merge into one
			last := spans[j-1]
			totalLines := 0
			for k := i; k < j; k++ {
				if lc, ok := spans[k].Attributes["log.line_count"]; ok {
					var n int
					fmt.Sscanf(lc, "%d", &n)
					totalLines += n
				}
			}
			dur := last.EndTime.Sub(s.StartTime)
			merged := ParsedSpan{
				Name:      fmt.Sprintf("%s (x%d, %s)", name, count, formatDuration(dur)),
				StartTime: s.StartTime,
				EndTime:   last.EndTime,
				Attributes: map[string]string{
					"log.line_count":  fmt.Sprintf("%d", totalLines),
					"log.line_number": s.Attributes["log.line_number"],
					"log.collapsed":   fmt.Sprintf("%d", count),
				},
			}
			result = append(result, merged)
		}
		i = j
	}
	return result
}

// isNonSubstantiveName returns true if a span name is just dots, punctuation,
// or other non-informative content that clutters the flamechart.
func isNonSubstantiveName(name string) bool {
	if name == "" {
		return true
	}
	// Check if the name is predominantly non-alphanumeric
	alphaCount := 0
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			alphaCount++
		}
	}
	// If less than 20% of characters are alphanumeric, it's noise
	if len(name) > 3 && float64(alphaCount)/float64(len(name)) < 0.2 {
		return true
	}
	return false
}

// groupByPrefix finds runs of 3+ consecutive spans that share a common leading
// word (e.g., "Compiling", "Downloading", "Checking") and nests them under a
// parent span named after that prefix. The varying suffix becomes each child's name.
//
// Minimum run length of 3 avoids false grouping of unrelated spans that happen
// to start with the same word.
const minPrefixRunLen = 3

func groupByPrefix(spans []ParsedSpan) []ParsedSpan {
	if len(spans) < minPrefixRunLen {
		return spans
	}

	var result []ParsedSpan
	i := 0
	for i < len(spans) {
		prefix := leadingWord(spans[i].Name)
		if prefix == "" {
			result = append(result, spans[i])
			i++
			continue
		}

		// Find run of consecutive spans with the same leading word
		j := i + 1
		for j < len(spans) && leadingWord(spans[j].Name) == prefix {
			j++
		}

		runLen := j - i
		if runLen < minPrefixRunLen {
			// Too few — emit individually
			for k := i; k < j; k++ {
				result = append(result, spans[k])
			}
			i = j
			continue
		}

		// Build parent span covering the full run
		parent := ParsedSpan{
			Name:      fmt.Sprintf("%s (x%d, %s)", prefix, runLen, formatDuration(spans[j-1].EndTime.Sub(spans[i].StartTime))),
			StartTime: spans[i].StartTime,
			EndTime:   spans[j-1].EndTime,
			Attributes: map[string]string{
				"log.line_number": spans[i].Attributes["log.line_number"],
				"log.collapsed":   fmt.Sprintf("%d", runLen),
			},
		}

		// Each span becomes a child, named by the suffix after the prefix
		for k := i; k < j; k++ {
			child := spans[k]
			suffix := strings.TrimSpace(strings.TrimPrefix(child.Name, prefix))
			if suffix == "" {
				suffix = child.Name
			}
			child.Name = suffix
			parent.Children = append(parent.Children, child)
		}

		result = append(result, parent)
		i = j
	}
	return result
}

// leadingWord returns the first whitespace-delimited word of s,
// or "" if s is empty or starts with non-letter characters.
func leadingWord(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Must start with a letter to be a meaningful verb/noun prefix
	if !unicode.IsLetter(rune(s[0])) {
		return ""
	}
	idx := strings.IndexByte(s, ' ')
	if idx < 0 {
		return "" // single-word name, no prefix to factor out
	}
	return s[:idx]
}

// formatDuration formats a duration in a human-friendly way.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
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
