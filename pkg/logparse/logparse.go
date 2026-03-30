// Package logparse extracts OTel-like spans from timestamped log output.
// It provides a generic framework with pluggable format-specific parsers
// for tools like Gradle, Bazel, and GitHub Actions setup steps.
package logparse

import (
	"bufio"
	"bytes"
	"regexp"
	"strings"
	"time"
)

// ansiEscape matches ANSI escape sequences (color codes, cursor movement, etc.)
var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// LogLine represents a single parsed log line with its timestamp and content.
type LogLine struct {
	Time    time.Time
	Content string
	LineNum int
}

// ParsedSpan represents a time-bounded region extracted from logs.
type ParsedSpan struct {
	Name       string
	StartTime  time.Time
	EndTime    time.Time
	Attributes map[string]string
	Children   []ParsedSpan
}

// Parser extracts structured time-series spans from log lines.
type Parser interface {
	// Name returns a human-readable identifier for this parser (e.g., "gradle", "bazel").
	Name() string

	// Match returns true if this parser can handle the given log content.
	// Receives all lines for heuristic detection.
	Match(lines []LogLine) bool

	// Parse extracts spans from the log lines.
	// stepStart/stepEnd provide the bounding timestamps from the GHA API.
	Parse(lines []LogLine, stepStart, stepEnd time.Time) []ParsedSpan
}

// ghaTimestampLen is the length of a GHA timestamp prefix like "2024-01-15T10:30:45.1234567Z ".
const ghaTimestampLen = 29 // 28 chars for timestamp + 1 space

// ParseLogLines parses GHA log text into structured LogLine entries.
// GHA format: "2024-01-15T10:30:45.1234567Z some content"
func ParseLogLines(raw []byte) []LogLine {
	var lines []LogLine
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if len(line) < ghaTimestampLen {
			// Line too short for timestamp; skip or attach to previous
			continue
		}

		tsStr := line[:28] // "2024-01-15T10:30:45.1234567Z"
		t, err := time.Parse("2006-01-02T15:04:05.0000000Z", tsStr)
		if err != nil {
			continue
		}

		content := stripANSI(line[ghaTimestampLen:])
		lines = append(lines, LogLine{
			Time:    t,
			Content: content,
			LineNum: lineNum,
		})
	}
	return lines
}

// stripANSI removes ANSI escape sequences from a string.
// Cargo, npm, and other tools emit colored output that interferes with
// span name extraction and prefix grouping.
func stripANSI(s string) string {
	if !strings.Contains(s, "\x1b") {
		return s // fast path: no escape sequences
	}
	return strings.TrimSpace(ansiEscape.ReplaceAllString(s, ""))
}

// TruncateName truncates a span name to maxLen, appending "..." if truncated.
func TruncateName(name string, maxLen int) string {
	name = strings.TrimSpace(name)
	if len(name) <= maxLen {
		return name
	}
	if maxLen <= 3 {
		return name[:maxLen]
	}
	return name[:maxLen-3] + "..."
}
