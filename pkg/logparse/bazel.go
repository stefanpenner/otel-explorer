package logparse

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// bazelActionRe matches Bazel progress lines like "[123 / 456] Compiling foo.cc"
var bazelActionRe = regexp.MustCompile(`^\[(\d+)\s*/\s*(\d+)\]\s+(.+)$`)

// bazelAnalyzeRe matches "INFO: Analyzed target //foo:bar" lines
var bazelAnalyzeRe = regexp.MustCompile(`^INFO: Analyzed (?:target )?(.+)$`)

// bazelPhaseRe matches bazel phase progress like "Loading:", "Analyzing:", "Computing main repo mapping:"
var bazelPhaseRe = regexp.MustCompile(`^(Extracting Bazel|Starting local Bazel|Computing main repo|Loading|Analyzing)`)

// BazelParser extracts spans from Bazel build output.
type BazelParser struct{}

func (p *BazelParser) Name() string { return "bazel" }

func (p *BazelParser) Match(lines []LogLine) bool {
	for _, l := range lines {
		if bazelActionRe.MatchString(l.Content) {
			return true
		}
		if bazelAnalyzeRe.MatchString(l.Content) {
			return true
		}
	}
	return false
}

// bazelWaitingRe matches bazel progress lines that are just status updates
// like "N / M tests; no actions running" or "N / M tests; checking cached actions"
var bazelWaitingRe = regexp.MustCompile(`^\d+\s*/\s*\d+\s+tests;\s+(.+)$`)

// normalizeActionDesc extracts a stable description from a bazel action line
// for deduplication. Collapses waiting/status lines that differ only in counts.
func normalizeActionDesc(desc string) string {
	if m := bazelWaitingRe.FindStringSubmatch(desc); m != nil {
		return "waiting (" + m[1] + ")"
	}
	return desc
}

func (p *BazelParser) Parse(lines []LogLine, stepStart, stepEnd time.Time) []ParsedSpan {
	type entry struct {
		name    string // display name
		key     string // dedup key
		time    time.Time
		lineNum int
	}

	var entries []entry
	seenAction := false

	for _, l := range lines {
		// Check for bazel action lines [N / M] ...
		if m := bazelActionRe.FindStringSubmatch(l.Content); m != nil {
			seenAction = true
			desc := m[3]
			entries = append(entries, entry{
				name:    TruncateName(desc, 80),
				key:     normalizeActionDesc(desc),
				time:    l.Time,
				lineNum: l.LineNum,
			})
			continue
		}

		// Only capture phase lines before the first action line.
		// After actions start, phase lines are just interleaved progress.
		if !seenAction && bazelPhaseRe.MatchString(l.Content) {
			phase := l.Content
			if idx := strings.Index(phase, ":"); idx > 0 && idx < 40 {
				phase = strings.TrimSpace(phase[:idx])
			}
			entries = append(entries, entry{
				name:    TruncateName(phase, 80),
				key:     phase,
				time:    l.Time,
				lineNum: l.LineNum,
			})
			continue
		}
	}

	if len(entries) == 0 {
		return nil
	}

	// Merge consecutive entries with the same dedup key
	type mergedEntry struct {
		name    string
		key     string
		start   time.Time
		lineNum int
		count   int
	}
	var merged []mergedEntry
	current := mergedEntry{
		name:    entries[0].name,
		key:     entries[0].key,
		start:   entries[0].time,
		lineNum: entries[0].lineNum,
		count:   1,
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].key == current.key {
			current.count++
		} else {
			merged = append(merged, current)
			current = mergedEntry{
				name:    entries[i].name,
				key:     entries[i].key,
				start:   entries[i].time,
				lineNum: entries[i].lineNum,
				count:   1,
			}
		}
	}
	merged = append(merged, current)

	// Build spans covering the full step duration
	var spans []ParsedSpan

	// If the first entry starts well after stepStart, create a "Starting" span
	if merged[0].start.Sub(stepStart) > time.Second {
		spans = append(spans, ParsedSpan{
			Name:      "Starting",
			StartTime: stepStart,
			EndTime:   merged[0].start,
			Attributes: map[string]string{
				"log.line_number":          "1",
				"bazel.action.description": "Bazel startup and initialization",
			},
		})
	}

	for i, m := range merged {
		var endTime time.Time
		if i+1 < len(merged) {
			endTime = merged[i+1].start
		} else {
			endTime = stepEnd
		}

		name := m.name
		if m.count > 1 {
			name = fmt.Sprintf("%s (×%d)", name, m.count)
		}

		spans = append(spans, ParsedSpan{
			Name:      name,
			StartTime: m.start,
			EndTime:   endTime,
			Attributes: map[string]string{
				"bazel.action.description": m.key,
				"log.line_number":          fmt.Sprintf("%d", m.lineNum),
			},
		})
	}

	return spans
}
