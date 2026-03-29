package logparse

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var gradleTaskRe = regexp.MustCompile(`^> Task (:\S+)(.*)$`)

// GradleParser extracts spans from Gradle build output.
// It recognizes "> Task :path:name" lines and creates a span per task.
type GradleParser struct{}

func (p *GradleParser) Name() string { return "gradle" }

func (p *GradleParser) Match(lines []LogLine) bool {
	for _, l := range lines {
		if gradleTaskRe.MatchString(l.Content) {
			return true
		}
		if strings.Contains(l.Content, "BUILD SUCCESSFUL") || strings.Contains(l.Content, "BUILD FAILED") {
			return true
		}
	}
	return false
}

func (p *GradleParser) Parse(lines []LogLine, stepStart, stepEnd time.Time) []ParsedSpan {
	type taskEntry struct {
		name    string
		outcome string
		time    time.Time
		lineNum int
	}

	var tasks []taskEntry
	for _, l := range lines {
		m := gradleTaskRe.FindStringSubmatch(l.Content)
		if m == nil {
			continue
		}
		taskName := m[1]
		outcome := strings.TrimSpace(m[2])
		if outcome == "" {
			outcome = "SUCCESS"
		}
		tasks = append(tasks, taskEntry{
			name:    taskName,
			outcome: outcome,
			time:    l.Time,
			lineNum: l.LineNum,
		})
	}

	if len(tasks) == 0 {
		return nil
	}

	var spans []ParsedSpan
	for i, t := range tasks {
		var endTime time.Time
		if i+1 < len(tasks) {
			endTime = tasks[i+1].time
		} else {
			endTime = stepEnd
		}

		spans = append(spans, ParsedSpan{
			Name:      t.name,
			StartTime: t.time,
			EndTime:   endTime,
			Attributes: map[string]string{
				"gradle.task.name":    t.name,
				"gradle.task.outcome": t.outcome,
				"log.line_number":     fmt.Sprintf("%d", t.lineNum),
			},
		})
	}

	return spans
}
