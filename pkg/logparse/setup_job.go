package logparse

import (
	"fmt"
	"strings"
	"time"
)

// SetupJobParser extracts spans from GitHub Actions "Set up job" step output.
// It recognizes phases like action downloads, environment setup, and container prep.
type SetupJobParser struct{}

func (p *SetupJobParser) Name() string { return "setup-job" }

func (p *SetupJobParser) Match(lines []LogLine) bool {
	// Require the distinctive "Getting action download info" marker which
	// only appears in the Set up job step, not in checkout or other steps
	// that may contain stray "Download action repository" lines due to
	// second-precision timestamp boundary overlap.
	for _, l := range lines {
		if strings.Contains(l.Content, "Getting action download info") ||
			strings.Contains(l.Content, "Prepare workflow directory") {
			return true
		}
	}
	return false
}

// phaseMarkers maps line content prefixes to phase names.
var phaseMarkers = []struct {
	prefix string
	phase  string
}{
	{"Current runner version:", "Runner setup"},
	{"Getting action download info", "Resolve actions"},
	{"Download action repository", "Download actions"},
	{"Complete job name:", "Prepare job"},
	{"Prepare workflow directory", "Prepare workspace"},
	{"Prepare all required actions", "Prepare actions"},
	{"Getting action download URL", "Download actions"},
}

func (p *SetupJobParser) Parse(lines []LogLine, stepStart, stepEnd time.Time) []ParsedSpan {
	type phaseEntry struct {
		name    string
		time    time.Time
		lineNum int
	}

	var phases []phaseEntry
	lastPhase := ""

	for _, l := range lines {
		for _, m := range phaseMarkers {
			if strings.Contains(l.Content, m.prefix) {
				if m.phase != lastPhase {
					phases = append(phases, phaseEntry{
						name:    m.phase,
						time:    l.Time,
						lineNum: l.LineNum,
					})
					lastPhase = m.phase
				}
				break
			}
		}
	}

	if len(phases) == 0 {
		return nil
	}

	var spans []ParsedSpan
	for i, ph := range phases {
		var endTime time.Time
		if i+1 < len(phases) {
			endTime = phases[i+1].time
		} else {
			endTime = stepEnd
		}

		spans = append(spans, ParsedSpan{
			Name:      ph.name,
			StartTime: ph.time,
			EndTime:   endTime,
			Attributes: map[string]string{
				"gha.setup.phase": ph.name,
				"log.line_number": fmt.Sprintf("%d", ph.lineNum),
			},
		})
	}

	return spans
}
