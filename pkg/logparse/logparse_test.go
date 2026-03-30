package logparse

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLogLines(t *testing.T) {
	raw := []byte(`2024-01-15T10:30:45.1234567Z First line
2024-01-15T10:30:46.0000000Z Second line
2024-01-15T10:30:47.5000000Z Third line
short line
2024-01-15T10:30:48.0000000Z Fourth line
`)

	lines := ParseLogLines(raw)
	require.Len(t, lines, 4)

	assert.Equal(t, "First line", lines[0].Content)
	assert.Equal(t, 1, lines[0].LineNum)
	assert.Equal(t, 2024, lines[0].Time.Year())

	assert.Equal(t, "Second line", lines[1].Content)
	assert.Equal(t, "Third line", lines[2].Content)
	assert.Equal(t, "Fourth line", lines[3].Content)
	assert.Equal(t, 5, lines[3].LineNum)
}

func TestParseLogLinesEmpty(t *testing.T) {
	lines := ParseLogLines([]byte{})
	assert.Empty(t, lines)
}

func TestTimestampParser(t *testing.T) {
	p := &TimestampParser{
		MinGapDuration:  500 * time.Millisecond,
		MinSpanDuration: 50 * time.Millisecond,
	}

	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lines := []LogLine{
		{Time: base, Content: "Starting download", LineNum: 1},
		{Time: base.Add(100 * time.Millisecond), Content: "Progress 50%", LineNum: 2},
		{Time: base.Add(200 * time.Millisecond), Content: "Progress 100%", LineNum: 3},
		// 2 second gap
		{Time: base.Add(2200 * time.Millisecond), Content: "Extracting files", LineNum: 4},
		{Time: base.Add(2300 * time.Millisecond), Content: "Done extracting", LineNum: 5},
		// 3 second gap
		{Time: base.Add(5300 * time.Millisecond), Content: "Running tests", LineNum: 6},
	}

	stepEnd := base.Add(6 * time.Second)
	spans := p.Parse(lines, base, stepEnd)

	require.Len(t, spans, 3)
	assert.Equal(t, "Starting download", spans[0].Name)
	assert.Equal(t, "Extracting files", spans[1].Name)
	assert.Equal(t, "Running tests", spans[2].Name)

	// Verify boundaries
	assert.Equal(t, base, spans[0].StartTime)
	assert.Equal(t, lines[3].Time, spans[0].EndTime) // next group start
	assert.Equal(t, stepEnd, spans[2].EndTime)        // last span ends at step end
}

func TestTimestampParserFiltersShortSpans(t *testing.T) {
	p := &TimestampParser{
		MinGapDuration:  time.Second,
		MinSpanDuration: time.Second,
	}

	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lines := []LogLine{
		{Time: base, Content: "Quick blip", LineNum: 1},
		// 2 second gap
		{Time: base.Add(2 * time.Second), Content: "Longer phase", LineNum: 2},
	}

	stepEnd := base.Add(5 * time.Second)
	spans := p.Parse(lines, base, stepEnd)

	// First span (base -> base+2s) should survive, second (base+2s -> base+5s) too
	require.Len(t, spans, 2)
}

func TestGradleParser(t *testing.T) {
	p := &GradleParser{}

	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lines := []LogLine{
		{Time: base, Content: "> Task :app:compileKotlin", LineNum: 1},
		{Time: base.Add(5 * time.Second), Content: "> Task :app:compileJava NO-SOURCE", LineNum: 2},
		{Time: base.Add(6 * time.Second), Content: "> Task :app:test", LineNum: 3},
		{Time: base.Add(15 * time.Second), Content: "BUILD SUCCESSFUL in 15s", LineNum: 4},
	}

	assert.True(t, p.Match(lines))

	stepEnd := base.Add(16 * time.Second)
	spans := p.Parse(lines, base, stepEnd)

	require.Len(t, spans, 3)
	assert.Equal(t, ":app:compileKotlin", spans[0].Name)
	assert.Equal(t, "SUCCESS", spans[0].Attributes["gradle.task.outcome"])

	assert.Equal(t, ":app:compileJava", spans[1].Name)
	assert.Equal(t, "NO-SOURCE", spans[1].Attributes["gradle.task.outcome"])

	assert.Equal(t, ":app:test", spans[2].Name)
	assert.Equal(t, stepEnd, spans[2].EndTime)
}

func TestBazelParser(t *testing.T) {
	p := &BazelParser{}

	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lines := []LogLine{
		{Time: base, Content: "INFO: Analyzed target //src:main", LineNum: 1},
		{Time: base.Add(time.Second), Content: "[1 / 10] Compiling src/main.cc", LineNum: 2},
		{Time: base.Add(3 * time.Second), Content: "[5 / 10] Linking src/main", LineNum: 3},
		{Time: base.Add(5 * time.Second), Content: "INFO: Build completed successfully", LineNum: 4},
	}

	assert.True(t, p.Match(lines))

	stepEnd := base.Add(6 * time.Second)
	spans := p.Parse(lines, base, stepEnd)

	require.Len(t, spans, 2)
	assert.Equal(t, "Compiling src/main.cc", spans[0].Name)
	assert.Equal(t, "Linking src/main", spans[1].Name)
}

func TestSetupJobParser(t *testing.T) {
	p := &SetupJobParser{}

	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lines := []LogLine{
		{Time: base, Content: "Current runner version: '2.311.0'", LineNum: 1},
		{Time: base.Add(time.Second), Content: "Getting action download info", LineNum: 2},
		{Time: base.Add(3 * time.Second), Content: "Download action repository 'actions/checkout@v4'", LineNum: 3},
		{Time: base.Add(5 * time.Second), Content: "Prepare workflow directory", LineNum: 4},
	}

	assert.True(t, p.Match(lines))

	stepEnd := base.Add(6 * time.Second)
	spans := p.Parse(lines, base, stepEnd)

	require.Len(t, spans, 4)
	assert.Equal(t, "Runner setup", spans[0].Name)
	assert.Equal(t, "Resolve actions", spans[1].Name)
	assert.Equal(t, "Download actions", spans[2].Name)
	assert.Equal(t, "Prepare workspace", spans[3].Name)
}

func TestRegistryDetectsGradle(t *testing.T) {
	reg := DefaultRegistry()

	lines := []LogLine{
		{Content: "> Task :app:build"},
	}

	p := reg.Detect(lines)
	assert.Equal(t, "gradle", p.Name())
}

func TestRegistryFallsBackToTimestamp(t *testing.T) {
	reg := DefaultRegistry()

	lines := []LogLine{
		{Content: "some random log output"},
	}

	p := reg.Detect(lines)
	assert.Equal(t, "timestamp", p.Name())
}

func TestTruncateName(t *testing.T) {
	assert.Equal(t, "short", TruncateName("short", 80))
	assert.Equal(t, "abcdefg...", TruncateName("abcdefghijklm", 10))
	assert.Equal(t, "", TruncateName("  ", 80))
}

func TestCollapseSpansRepeatingNames(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := []ParsedSpan{
		{Name: "Downloading package", StartTime: base, EndTime: base.Add(2 * time.Second),
			Attributes: map[string]string{"log.line_count": "3", "log.line_number": "1"}},
		{Name: "Downloading package", StartTime: base.Add(2 * time.Second), EndTime: base.Add(4 * time.Second),
			Attributes: map[string]string{"log.line_count": "2", "log.line_number": "4"}},
		{Name: "Downloading package", StartTime: base.Add(4 * time.Second), EndTime: base.Add(6 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "6"}},
		{Name: "Running tests", StartTime: base.Add(6 * time.Second), EndTime: base.Add(10 * time.Second),
			Attributes: map[string]string{"log.line_count": "5", "log.line_number": "7"}},
	}

	result := collapseSpans(spans)
	require.Len(t, result, 2)
	assert.Contains(t, result[0].Name, "Downloading package")
	assert.Contains(t, result[0].Name, "x3")
	assert.Equal(t, base, result[0].StartTime)
	assert.Equal(t, base.Add(6*time.Second), result[0].EndTime)
	assert.Equal(t, "6", result[0].Attributes["log.line_count"])
	assert.Equal(t, "Running tests", result[1].Name)
}

func TestCollapseSpansNonSubstantive(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := []ParsedSpan{
		{Name: "............................", StartTime: base, EndTime: base.Add(2 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "1"}},
		{Name: "----------------------------", StartTime: base.Add(2 * time.Second), EndTime: base.Add(4 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "2"}},
		{Name: "================================", StartTime: base.Add(4 * time.Second), EndTime: base.Add(6 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "3"}},
	}

	result := collapseSpans(spans)
	// All three non-substantive spans should merge into one
	require.Len(t, result, 1)
	assert.Contains(t, result[0].Name, "x3")
}

func TestCollapseSpansSingleNonSubstantiveShort(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := []ParsedSpan{
		{Name: "....", StartTime: base, EndTime: base.Add(500 * time.Millisecond),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "1"}},
	}

	result := collapseSpans(spans)
	// Short single non-substantive span should be dropped
	assert.Empty(t, result)
}

func TestIsNonSubstantiveName(t *testing.T) {
	assert.True(t, isNonSubstantiveName("............................"))
	assert.True(t, isNonSubstantiveName("----------------------------"))
	assert.True(t, isNonSubstantiveName("================================"))
	assert.True(t, isNonSubstantiveName(""))
	assert.False(t, isNonSubstantiveName("Downloading package"))
	assert.False(t, isNonSubstantiveName("Running tests"))
	assert.False(t, isNonSubstantiveName("[5 / 10] Compiling"))
}

func TestCollapseSpansPreservesUnique(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := []ParsedSpan{
		{Name: "Step A", StartTime: base, EndTime: base.Add(2 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "1"}},
		{Name: "Step B", StartTime: base.Add(2 * time.Second), EndTime: base.Add(4 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "2"}},
		{Name: "Step C", StartTime: base.Add(4 * time.Second), EndTime: base.Add(6 * time.Second),
			Attributes: map[string]string{"log.line_count": "1", "log.line_number": "3"}},
	}

	result := collapseSpans(spans)
	require.Len(t, result, 3)
	assert.Equal(t, "Step A", result[0].Name)
	assert.Equal(t, "Step B", result[1].Name)
	assert.Equal(t, "Step C", result[2].Name)
}

func TestTimestampParserGroups(t *testing.T) {
	p := &TimestampParser{
		MinGapDuration:  500 * time.Millisecond,
		MinSpanDuration: 50 * time.Millisecond,
	}

	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	lines := []LogLine{
		{Time: base, Content: "##[group]Setup Node.js", LineNum: 1},
		{Time: base.Add(100 * time.Millisecond), Content: "Downloading node 18.x", LineNum: 2},
		{Time: base.Add(2 * time.Second), Content: "Extracting...", LineNum: 3},
		{Time: base.Add(4 * time.Second), Content: "Adding to PATH", LineNum: 4},
		{Time: base.Add(4100 * time.Millisecond), Content: "##[endgroup]", LineNum: 5},
		{Time: base.Add(5 * time.Second), Content: "##[group]Install deps", LineNum: 6},
		{Time: base.Add(5100 * time.Millisecond), Content: "npm ci", LineNum: 7},
		{Time: base.Add(10 * time.Second), Content: "Done", LineNum: 8},
		{Time: base.Add(10100 * time.Millisecond), Content: "##[endgroup]", LineNum: 9},
	}

	stepEnd := base.Add(11 * time.Second)
	spans := p.Parse(lines, base, stepEnd)

	require.Len(t, spans, 2)
	assert.Equal(t, "Setup Node.js", spans[0].Name)
	assert.Equal(t, "Install deps", spans[1].Name)
	assert.Equal(t, "Setup Node.js", spans[0].Attributes["log.group"])
}

func TestGroupByPrefix(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := make([]ParsedSpan, 5)
	for i := range spans {
		name := fmt.Sprintf("Compiling crate-%d v0.%d.0", i, i)
		spans[i] = ParsedSpan{
			Name:      name,
			StartTime: base.Add(time.Duration(i) * 2 * time.Second),
			EndTime:   base.Add(time.Duration(i+1) * 2 * time.Second),
			Attributes: map[string]string{
				"log.line_count":  "1",
				"log.line_number": fmt.Sprintf("%d", i+1),
			},
		}
	}

	result := groupByPrefix(spans)
	require.Len(t, result, 1, "5 'Compiling' spans should merge into 1 parent")
	assert.Contains(t, result[0].Name, "Compiling")
	assert.Contains(t, result[0].Name, "x5")
	require.Len(t, result[0].Children, 5)
	// Children should have the prefix stripped
	assert.Equal(t, "crate-0 v0.0.0", result[0].Children[0].Name)
	assert.Equal(t, "crate-4 v0.4.0", result[0].Children[4].Name)
	// Parent time range covers all children
	assert.Equal(t, base, result[0].StartTime)
	assert.Equal(t, base.Add(10*time.Second), result[0].EndTime)
}

func TestGroupByPrefixMixed(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := []ParsedSpan{
		{Name: "Setup environment", StartTime: base, EndTime: base.Add(2 * time.Second),
			Attributes: map[string]string{"log.line_number": "1"}},
		{Name: "Compiling foo v1.0", StartTime: base.Add(2 * time.Second), EndTime: base.Add(3 * time.Second),
			Attributes: map[string]string{"log.line_number": "2"}},
		{Name: "Compiling bar v2.0", StartTime: base.Add(3 * time.Second), EndTime: base.Add(4 * time.Second),
			Attributes: map[string]string{"log.line_number": "3"}},
		{Name: "Compiling baz v3.0", StartTime: base.Add(4 * time.Second), EndTime: base.Add(5 * time.Second),
			Attributes: map[string]string{"log.line_number": "4"}},
		{Name: "Linking binary", StartTime: base.Add(5 * time.Second), EndTime: base.Add(7 * time.Second),
			Attributes: map[string]string{"log.line_number": "5"}},
	}

	result := groupByPrefix(spans)
	require.Len(t, result, 3) // Setup, Compiling(x3), Linking
	assert.Equal(t, "Setup environment", result[0].Name)
	assert.Contains(t, result[1].Name, "Compiling")
	assert.Contains(t, result[1].Name, "x3")
	require.Len(t, result[1].Children, 3)
	assert.Equal(t, "Linking binary", result[2].Name)
}

func TestGroupByPrefixTooFew(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	spans := []ParsedSpan{
		{Name: "Compiling foo", StartTime: base, EndTime: base.Add(2 * time.Second),
			Attributes: map[string]string{"log.line_number": "1"}},
		{Name: "Compiling bar", StartTime: base.Add(2 * time.Second), EndTime: base.Add(4 * time.Second),
			Attributes: map[string]string{"log.line_number": "2"}},
	}

	result := groupByPrefix(spans)
	// Only 2 spans with same prefix — should NOT group (minimum is 3)
	require.Len(t, result, 2)
	assert.Equal(t, "Compiling foo", result[0].Name)
	assert.Equal(t, "Compiling bar", result[1].Name)
}

func TestStripANSI(t *testing.T) {
	assert.Equal(t, "Compiling ruff v0.15.8", stripANSI("\x1b[1m\x1b[92m   Compiling\x1b[0m ruff v0.15.8"))
	assert.Equal(t, "Downloading crates", stripANSI("\x1b[32m   Downloading\x1b[0m crates"))
	assert.Equal(t, "no escapes here", stripANSI("no escapes here"))
	assert.Equal(t, "", stripANSI(""))
}

func TestParseLogLinesStripsANSI(t *testing.T) {
	raw := []byte("2024-01-15T10:30:45.1234567Z \x1b[1m\x1b[92m   Compiling\x1b[0m ruff v0.15.8\n")
	lines := ParseLogLines(raw)
	require.Len(t, lines, 1)
	assert.Equal(t, "Compiling ruff v0.15.8", lines[0].Content)
}

func TestLeadingWord(t *testing.T) {
	assert.Equal(t, "Compiling", leadingWord("Compiling foo v1.0"))
	assert.Equal(t, "Downloading", leadingWord("Downloading package"))
	assert.Equal(t, "", leadingWord(""))
	assert.Equal(t, "", leadingWord("[5 / 10] stuff"))  // starts with non-letter
	assert.Equal(t, "", leadingWord("singleword"))       // no space = no prefix
}
