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
			DailyDuration: []DailyPoint{
				{Date: "2026-06-15", Value: 290, Count: 12},
				{Date: "2026-06-16", Value: 305, Count: 10},
				{Date: "2026-06-17", Value: 280, Count: 14},
			},
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
	assert.Equal(t, "Overview", sheets[0], "dashboard Overview is the first sheet")
	assert.Contains(t, sheets, "Jobs")
	assert.Contains(t, sheets, "Steps")
	assert.NotContains(t, sheets, "Sheet1", "default sheet should be renamed, not left empty")

	// Overview carries KPI cards + a key-findings block.
	overview := cellGrid(t, f, "Overview")
	assert.Contains(t, overview, "Job success", "KPI card label present")
	assert.Contains(t, overview, "Key findings")

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

	// Data bars decorate the duration column (world-class visual).
	cf, _ := f.GetConditionalFormats("Jobs")
	assert.NotEmpty(t, cf, "Jobs sheet has conditional formatting (data bars)")
	// Failed job rows are red-filled (not default style id 0).
	failStyle, _ := f.GetCellStyle("Jobs", "E3") // row 3 = the failing "test" job, Conclusion col
	assert.NotZero(t, failStyle, "failed conclusion cell is styled (red fill)")

	// Unknown job success rate renders as the em dash KPI, not a misleading 0%.
	rep := sampleRunReport()
	rep.Run.Summary.JobSuccessRatePct = nil
	var buf2 bytes.Buffer
	require.NoError(t, RenderXLSX(&buf2, rep))
	f2, err := excelize.OpenReader(&buf2)
	require.NoError(t, err)
	defer f2.Close()
	assert.Contains(t, cellGrid(t, f2, "Overview"), "—", "unknown rate shows em dash, not 0%")
}

func TestRenderXLSX_Trends(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderXLSX(&buf, sampleTrendReport()))
	f, err := excelize.OpenReader(&buf)
	require.NoError(t, err)
	defer f.Close()
	sheets := f.GetSheetList()
	assert.Equal(t, "Overview", sheets[0])
	assert.Contains(t, sheets, "Typical Run")
	assert.Contains(t, sheets, "Flaky Jobs")
	assert.Contains(t, sheets, "Daily", "daily series sheet with trend chart")

	// Flake % column carries a color-scale conditional format. (The Daily line
	// chart is validated by RenderXLSX returning no error — AddChart failures
	// propagate.)
	cf, _ := f.GetConditionalFormats("Flaky Jobs")
	assert.NotEmpty(t, cf, "Flaky Jobs has a color-scale conditional format")
}

// cellGrid flattens a sheet's cells into one string for content assertions.
func cellGrid(t *testing.T, f *excelize.File, sheet string) string {
	t.Helper()
	rows, err := f.GetRows(sheet)
	require.NoError(t, err)
	var sb strings.Builder
	for _, r := range rows {
		for _, c := range r {
			sb.WriteString(c)
			sb.WriteByte('\n')
		}
	}
	return sb.String()
}

func TestRenderTrends_AllFormatsCarryKeyContent(t *testing.T) {
	rep := sampleTrendReport()

	var jbuf bytes.Buffer
	require.NoError(t, RenderJSON(&jbuf, rep))
	assert.Contains(t, jbuf.String(), `"flaky_jobs"`)
	assert.Contains(t, jbuf.String(), `"typical_workflows"`)

	var hbuf bytes.Buffer
	require.NoError(t, RenderHTML(&hbuf, rep))
	html := hbuf.String()
	assert.Contains(t, html, "CI Trends")
	assert.Contains(t, html, "Flakiest jobs")
	assert.Contains(t, html, "Typical run · CI")
	// World-class: KPI cards, key findings, and an inline SVG trend chart.
	assert.Contains(t, html, `class="kpi`)
	assert.Contains(t, html, "Key findings")
	assert.Contains(t, html, "<svg")
	assert.Contains(t, html, `class="area"`)

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

	// A .docx is a zip; word/document.xml must contain our structure.
	doc := docXML(t, &buf)
	assert.Contains(t, doc, "CI Run Analysis")
	assert.Contains(t, doc, "Executive summary")
	assert.Contains(t, doc, "Recommendations") // sample has a failing job → a rec
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
	// World-class structure: KPI cards, key-findings callout.
	assert.Contains(t, out, `class="kpi`)
	assert.Contains(t, out, "Key findings")
	assert.Contains(t, out, `class="finding`)
	// Failed job rows are color-coded.
	assert.Contains(t, out, `<tr class="bad">`)
	// html/template must escape injected content.
	assert.NotContains(t, out, "<script>")
	assert.Contains(t, out, "&lt;script&gt;")
}
