package logparse

import (
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
