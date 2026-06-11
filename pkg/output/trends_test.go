package output

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

func TestLinkNameTruncatesOnRuneBoundary(t *testing.T) {
	// 70 runes of 3-byte CJK characters (210 bytes)
	name := strings.Repeat("ビルドとテスト", 10)
	out := linkName(name, nil, 48)

	if !utf8.ValidString(out) {
		t.Errorf("truncated name is not valid UTF-8: %q", out)
	}
	if got := utf8.RuneCountInString(out); got != 48 {
		t.Errorf("expected 48 runes (45 + %q), got %d: %q", "...", got, out)
	}
	if !strings.HasSuffix(out, "...") {
		t.Errorf("expected truncation ellipsis, got %q", out)
	}
}

func TestLinkNameShortNameUnchanged(t *testing.T) {
	if out := linkName("build", nil, 48); out != "build" {
		t.Errorf("short name should be unchanged, got %q", out)
	}
	// A 12-rune CJK name (36 bytes) fits in 35 visible columns worth of runes
	// and must not be truncated just because its byte length exceeds the max.
	name := "ビルドビルドビルドビルド"
	if out := linkName(name, nil, 35); out != name {
		t.Errorf("multi-byte name within rune budget should be unchanged, got %q", out)
	}
}

func TestGenerateASCIIChartTimeAxis(t *testing.T) {
	day := func(d int) time.Time {
		return time.Date(2026, 1, 1+d, 0, 0, 0, 0, time.UTC)
	}
	// Runs on Jan 1, Jan 2, and Jan 30: the Jan 2 peak is 1/29 of the way
	// through the time range and must render near the left edge, not at the
	// horizontal center of the chart.
	points := []analyzer.DataPoint{
		{Timestamp: day(0), Value: 0},
		{Timestamp: day(1), Value: 100},
		{Timestamp: day(29), Value: 0},
	}

	width := 30
	chart := generateASCIIChart(points, width, 10, "seconds")
	lines := strings.Split(chart, "\n")

	// Find the row that plots the peak value as "●".
	first, last := -1, -1
	for _, line := range lines {
		runes := []rune(line)
		if len(runes) <= 10 {
			continue
		}
		area := runes[10:] // skip the "%8s │" y-axis prefix
		f, l := -1, -1
		for i, r := range area {
			if r == '●' {
				if f < 0 {
					f = i
				}
				l = i
			}
		}
		// The peak row is the one whose band does not include column 0
		// (column 0 maps to the Jan 1 low value).
		if f > 0 {
			first, last = f, l
			break
		}
	}

	if first < 0 {
		t.Fatalf("peak band not found in chart:\n%s", chart)
	}
	if first > 3 {
		t.Errorf("peak band should start near the left edge (Jan 2), started at column %d:\n%s", first, chart)
	}
	if last >= width/2 {
		t.Errorf("peak band should end before the chart midpoint, ended at column %d:\n%s", last, chart)
	}
}

func TestRenderChangepointPositionIsOneBased(t *testing.T) {
	cp := &analyzer.Changepoint{
		Date:        time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC),
		Index:       3, // 0-based index of the first post-shift observation
		TotalPoints: 10,
	}

	var buf bytes.Buffer
	renderRegressions(&buf, []analyzer.JobRegression{{
		Name:            "build",
		OldAvgDuration:  10,
		NewAvgDuration:  20,
		PercentIncrease: 100,
		Changepoint:     cp,
	}})
	if out := buf.String(); !strings.Contains(out, "observation 4 of 10") {
		t.Errorf("regression changepoint should render 1-based ordinal, got:\n%s", out)
	}

	buf.Reset()
	renderImprovements(&buf, []analyzer.JobImprovement{{
		Name:            "build",
		OldAvgDuration:  20,
		NewAvgDuration:  10,
		PercentDecrease: 50,
		Changepoint:     cp,
	}})
	if out := buf.String(); !strings.Contains(out, "observation 4 of 10") {
		t.Errorf("improvement changepoint should render 1-based ordinal, got:\n%s", out)
	}
}
