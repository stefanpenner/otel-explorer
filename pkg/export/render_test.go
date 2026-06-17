package export

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// sampleRunReport builds a small but representative run-analysis Report.
func sampleRunReport() *Report {
	return &Report{
		SchemaVersion: SchemaVersion, Kind: KindRunAnalysis,
		Meta: Meta{Tool: "ote", GeneratedAt: "2026-06-17T00:00:00Z", Repo: "o/r"},
		Run: &RunReport{
			Summary: RunSummary{TotalRuns: 1, SuccessfulRuns: 1, TotalJobs: 2, FailedJobs: 1,
				TotalSteps: 1, SuccessRatePct: 100, JobSuccessRatePct: 50, MaxConcurrency: 2, WallClockMs: 3000},
			Runs: []Run{{
				Repo: "o/r", Identifier: "42", Type: "pr", DisplayName: "PR #42",
				TotalJobs: 2, FailedJobs: 1, JobSuccessRatePct: 50,
				Jobs: []Job{
					{Name: "build", Status: "completed", Conclusion: "success", DurationMs: 3000, Required: true, URL: "u1"},
					{Name: "test", Status: "completed", Conclusion: "failure", DurationMs: 1000, URL: "u2"},
				},
				Steps: []Step{{Job: "build", Name: "checkout", DurationMs: 500, URL: "s1"}},
			}},
		},
	}
}

func sampleTrendReport() *Report {
	return &Report{
		SchemaVersion: SchemaVersion, Kind: KindTrends,
		Meta: Meta{Tool: "ote", GeneratedAt: "2026-06-17T00:00:00Z", Repo: "o/r"},
		Trends: &TrendReport{
			Days: 7,
			Summary: TrendSummary{TotalRuns: 100, AvgDurationSec: 300, MedianDurationSec: 280,
				P95DurationSec: 600, AvgSuccessRatePct: 92.5, TrendDirection: "stable"},
			FlakyJobs: []FlakyJob{{Name: "test", FlakeRatePct: 10, SameSHAFlakes: 2, TransitionScore: 0.3}},
			Typical: []TypicalWorkflow{{Name: "CI", Jobs: []TypicalJob{
				{Name: "build", Duration: Quantiles{P50: 120, P95: 200}, SuccessRatePct: 99},
			}}},
		},
	}
}

func TestRenderJSON_RoundTrips(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderJSON(&buf, sampleRunReport()))

	var back Report
	require.NoError(t, json.Unmarshal(buf.Bytes(), &back))
	assert.Equal(t, KindRunAnalysis, back.Kind)
	assert.Equal(t, SchemaVersion, back.SchemaVersion)
	require.NotNil(t, back.Run)
	require.Len(t, back.Run.Runs, 1)
	assert.Equal(t, "build", back.Run.Runs[0].Jobs[0].Name)
	// Snake_case keys present for jq consumers.
	assert.Contains(t, buf.String(), `"job_success_rate_pct"`)
	assert.Contains(t, buf.String(), `"duration_ms"`)
}

func TestRenderXLSX_OpensWithExpectedSheetsAndCells(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderXLSX(&buf, sampleRunReport()))

	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer f.Close()

	sheets := f.GetSheetList()
	assert.Contains(t, sheets, "Summary")
	assert.Contains(t, sheets, "Jobs")
	assert.Contains(t, sheets, "Steps")
	assert.NotContains(t, sheets, "Sheet1", "default sheet should be renamed, not left empty")

	// Header + a data cell on the Jobs sheet.
	h, _ := f.GetCellValue("Jobs", "C1")
	assert.Equal(t, "Job", h)
	v, _ := f.GetCellValue("Jobs", "C2")
	assert.Equal(t, "build", v)
}

func TestRenderXLSX_Trends(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderXLSX(&buf, sampleTrendReport()))
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer f.Close()
	sheets := f.GetSheetList()
	assert.Contains(t, sheets, "Summary")
	assert.Contains(t, sheets, "Typical Run")
	assert.Contains(t, sheets, "Flaky Jobs")
}

func TestRenderDOCX_IsValidZipWithContent(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderDOCX(&buf, sampleRunReport()))

	// A .docx is a zip; word/document.xml must contain our text.
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	var doc string
	for _, file := range zr.File {
		if file.Name == "word/document.xml" {
			rc, err := file.Open()
			require.NoError(t, err)
			b, _ := io.ReadAll(rc)
			rc.Close()
			doc = string(b)
		}
	}
	require.NotEmpty(t, doc, "word/document.xml not found")
	assert.Contains(t, doc, "CI Run Analysis")
	assert.Contains(t, doc, "build")
	assert.Contains(t, doc, "Slowest jobs")
}

func TestRenderHTML_WellFormedAndEscaped(t *testing.T) {
	rep := sampleRunReport()
	rep.Run.Runs[0].Jobs[0].Name = "build <script>"
	var buf bytes.Buffer
	require.NoError(t, RenderHTML(&buf, rep))
	out := buf.String()

	assert.True(t, strings.HasPrefix(out, "<!DOCTYPE html>"))
	assert.Contains(t, out, "<title>CI Run Analysis — o/r</title>")
	assert.Contains(t, out, "Slowest jobs")
	// html/template must escape injected content.
	assert.NotContains(t, out, "<script>")
	assert.Contains(t, out, "&lt;script&gt;")
}
