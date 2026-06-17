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

func f64(v float64) *float64 { return &v }

// sampleRunReport builds a small but representative run-analysis Report.
func sampleRunReport() *Report {
	return &Report{
		SchemaVersion: SchemaVersion, Kind: KindRunAnalysis,
		Meta: Meta{Tool: "ote", GeneratedAt: "2026-06-17T00:00:00Z", Repo: "o/r"},
		Run: &RunReport{
			Summary: RunSummary{TotalRuns: 1, SuccessfulRuns: 1, TotalJobs: 2, FailedJobs: 1,
				TotalSteps: 1, SuccessRatePct: f64(100), JobSuccessRatePct: f64(50), MaxConcurrency: 2, WallClockMs: 3000},
			Runs: []Run{{
				Repo: "o/r", Identifier: "42", Type: "pr", DisplayName: "PR #42",
				TotalJobs: 2, FailedJobs: 1, JobSuccessRatePct: f64(50),
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

	// Consumer need: durations must be NUMBERS, so Excel/Sheets can sort, sum,
	// and chart them — not text stored as a (shared/inline) string. Column F is
	// "Duration (sec)". Plain numbers carry no type attribute (CellTypeUnset),
	// which is exactly what Excel reads as numeric; strings would not.
	durType, _ := f.GetCellType("Jobs", "F2")
	assert.NotContains(t, []excelize.CellType{excelize.CellTypeSharedString, excelize.CellTypeInlineString}, durType, "duration must not be stored as text")
	dur, _ := f.GetCellValue("Jobs", "F2")
	assert.Equal(t, "3", dur, "3000ms → 3 sec")

	// Unknown job success rate renders as the em dash, not a misleading 0%.
	rep := sampleRunReport()
	rep.Run.Summary.JobSuccessRatePct = nil
	var buf2 bytes.Buffer
	require.NoError(t, RenderXLSX(&buf2, rep))
	f2, err := excelize.OpenReader(&buf2)
	require.NoError(t, err)
	defer f2.Close()
	// Summary sheet is Metric/Value rows; find the "Job success rate" row value.
	found := ""
	rows, _ := f2.GetRows("Summary")
	for _, r := range rows {
		if len(r) >= 2 && r[0] == "Job success rate" {
			found = r[1]
		}
	}
	assert.Equal(t, "—", found, "unknown rate shows em dash, not 0%")
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

func TestRenderTrends_AllFormatsCarryKeyContent(t *testing.T) {
	rep := sampleTrendReport()

	var jbuf bytes.Buffer
	require.NoError(t, RenderJSON(&jbuf, rep))
	assert.Contains(t, jbuf.String(), `"flaky_jobs"`)
	assert.Contains(t, jbuf.String(), `"typical_workflows"`)

	var hbuf bytes.Buffer
	require.NoError(t, RenderHTML(&hbuf, rep))
	assert.Contains(t, hbuf.String(), "CI Trends")
	assert.Contains(t, hbuf.String(), "Flakiest jobs")
	assert.Contains(t, hbuf.String(), "Typical run · CI")

	var dbuf bytes.Buffer
	require.NoError(t, RenderDOCX(&dbuf, rep))
	doc := docXML(t, &dbuf)
	assert.Contains(t, doc, "CI Trends")
	assert.Contains(t, doc, "Flakiest jobs")
	assert.Contains(t, doc, "test") // the flaky job name
}

// docXML extracts word/document.xml from a .docx byte stream.
func docXML(t *testing.T, buf *bytes.Buffer) string {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	for _, file := range zr.File {
		if file.Name == "word/document.xml" {
			rc, err := file.Open()
			require.NoError(t, err)
			b, _ := io.ReadAll(rc)
			rc.Close()
			return string(b)
		}
	}
	t.Fatal("word/document.xml not found")
	return ""
}

func TestRenderDOCX_IsValidZipWithContent(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderDOCX(&buf, sampleRunReport()))

	// A .docx is a zip; word/document.xml must contain our text.
	doc := docXML(t, &buf)
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
