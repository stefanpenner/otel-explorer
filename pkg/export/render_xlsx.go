package export

import (
	"fmt"
	"io"

	"github.com/xuri/excelize/v2"
)

// RenderXLSX writes the report as a multi-sheet Excel workbook: a sheet per
// logical table (Summary, Jobs, Steps for run analysis; Summary, Typical Run,
// Flaky Jobs, ... for trends). Header rows are bold and frozen with an
// auto-filter so the workbook is immediately explorable.
func RenderXLSX(w io.Writer, rep *Report) error {
	f := excelize.NewFile()
	defer f.Close()
	first := true
	sheet := func(name string, headers []string, rows [][]any) error {
		if err := writeSheet(f, name, headers, rows, &first); err != nil {
			return fmt.Errorf("sheet %q: %w", name, err)
		}
		return nil
	}

	switch rep.Kind {
	case KindRunAnalysis:
		if err := renderRunSheets(rep.Run, sheet); err != nil {
			return err
		}
	case KindTrends:
		if err := renderTrendSheets(rep.Trends, sheet); err != nil {
			return err
		}
	}

	// The first sheet renames the workbook's default "Sheet1" in place, so no
	// empty default sheet is left behind to clean up.
	_, err := f.WriteTo(w)
	return err
}

func renderRunSheets(r *RunReport, sheet func(string, []string, [][]any) error) error {
	if r == nil {
		return nil
	}
	summary := [][]any{
		{"Total runs", r.Summary.TotalRuns},
		{"Successful runs", r.Summary.SuccessfulRuns},
		{"Failed runs", r.Summary.FailedRuns},
		{"Total jobs", r.Summary.TotalJobs},
		{"Failed jobs", r.Summary.FailedJobs},
		{"Total steps", r.Summary.TotalSteps},
		{"Success rate %", round1(r.Summary.SuccessRatePct)},
		{"Job success rate %", round1(r.Summary.JobSuccessRatePct)},
		{"Max concurrency", r.Summary.MaxConcurrency},
		{"Wall clock (sec)", round1(float64(r.Summary.WallClockMs) / 1000)},
	}
	if err := sheet("Summary", []string{"Metric", "Value"}, summary); err != nil {
		return err
	}

	var jobs [][]any
	var steps [][]any
	for _, run := range r.Runs {
		for _, j := range run.Jobs {
			jobs = append(jobs, []any{
				run.Identifier, run.DisplayName, j.Name, j.Status, j.Conclusion,
				round1(float64(j.DurationMs) / 1000), j.Required, j.URL,
			})
		}
		for _, s := range run.Steps {
			steps = append(steps, []any{run.Identifier, s.Job, s.Name, round1(float64(s.DurationMs) / 1000), s.URL})
		}
	}
	if err := sheet("Jobs", []string{"Run", "Source", "Job", "Status", "Conclusion", "Duration (sec)", "Required", "URL"}, jobs); err != nil {
		return err
	}
	if len(steps) > 0 {
		if err := sheet("Steps", []string{"Run", "Job", "Step", "Duration (sec)", "URL"}, steps); err != nil {
			return err
		}
	}
	return nil
}

func renderTrendSheets(t *TrendReport, sheet func(string, []string, [][]any) error) error {
	if t == nil {
		return nil
	}
	summary := [][]any{
		{"Days", t.Days},
		{"Total runs", t.Summary.TotalRuns},
		{"Avg duration (sec)", round1(t.Summary.AvgDurationSec)},
		{"Median duration (sec)", round1(t.Summary.MedianDurationSec)},
		{"P95 duration (sec)", round1(t.Summary.P95DurationSec)},
		{"Avg success rate %", round1(t.Summary.AvgSuccessRatePct)},
		{"Trend", t.Summary.TrendDirection},
		{"Percent change", round1(t.Summary.PercentChange)},
		{"Rerun runs", t.Summary.RerunRuns},
		{"Retry burn (sec)", round1(float64(t.Summary.RerunComputeMs) / 1000)},
		{"Avg queue (sec)", round1(t.QueueStats.AvgQueueSec)},
		{"Queue ratio %", round1(t.QueueStats.QueueRatioPct)},
	}
	if err := sheet("Summary", []string{"Metric", "Value"}, summary); err != nil {
		return err
	}

	var typical [][]any
	for _, w := range t.Typical {
		for _, j := range w.Jobs {
			typical = append(typical, []any{
				w.Name, j.Name, j.Samples, round1(j.PresenceRatePct), round1(j.SuccessRatePct),
				round1(j.Duration.P5), round1(j.Duration.P25), round1(j.Duration.P50),
				round1(j.Duration.P75), round1(j.Duration.P95), j.TrendDirection, j.P50URL,
			})
		}
	}
	if len(typical) > 0 {
		if err := sheet("Typical Run", []string{
			"Workflow", "Job", "Samples", "Presence %", "Success %",
			"p5", "p25", "p50", "p75", "p95", "Trend", "p50 URL",
		}, typical); err != nil {
			return err
		}
	}

	var flaky [][]any
	for _, fj := range t.FlakyJobs {
		flaky = append(flaky, []any{
			fj.Name, fj.TotalRuns, fj.SuccessCount, fj.FailureCount,
			round1(fj.FlakeRatePct), fj.SameSHAFlakes, round2(fj.TransitionScore), fj.SampleURL,
		})
	}
	if len(flaky) > 0 {
		if err := sheet("Flaky Jobs", []string{
			"Job", "Runs", "Pass", "Fail", "Flake %", "Same-SHA", "Transition", "Sample URL",
		}, flaky); err != nil {
			return err
		}
	}

	if rows := changeRows(t.Regressions); len(rows) > 0 {
		if err := sheet("Regressions", changeHeaders(), rows); err != nil {
			return err
		}
	}
	if rows := changeRows(t.Improvements); len(rows) > 0 {
		if err := sheet("Improvements", changeHeaders(), rows); err != nil {
			return err
		}
	}

	if len(t.Hourly) > 0 {
		var hourly [][]any
		for _, h := range t.Hourly {
			hourly = append(hourly, []any{h.Hour, h.RunCount, round1(h.QueueP50Sec), round1(h.DurationP50Sec)})
		}
		if err := sheet("Hourly", []string{"UTC Hour", "Runs", "Queue p50 (sec)", "Duration p50 (sec)"}, hourly); err != nil {
			return err
		}
	}

	if len(t.DailyDuration) > 0 {
		var daily [][]any
		success := map[string]float64{}
		for _, p := range t.DailySuccess {
			success[p.Date] = p.Value
		}
		for _, p := range t.DailyDuration {
			daily = append(daily, []any{p.Date, round1(p.Value), p.Count, round1(success[p.Date])})
		}
		if err := sheet("Daily", []string{"Date", "Avg Duration (sec)", "Runs", "Success %"}, daily); err != nil {
			return err
		}
	}
	return nil
}

func changeHeaders() []string {
	return []string{"Job", "Old avg (sec)", "New avg (sec)", "Change %", "Delta (sec)", "Diff URL"}
}

func changeRows(changes []JobChange) [][]any {
	var rows [][]any
	for _, c := range changes {
		rows = append(rows, []any{c.Name, round1(c.OldAvgSec), round1(c.NewAvgSec), round1(c.PercentChange), round1(c.AbsoluteSec), c.DiffURL})
	}
	return rows
}

// writeSheet creates a sheet, writes a bold/frozen header plus data rows, and
// applies an auto-filter over the full range. The first sheet reuses the
// workbook's default "Sheet1" so it isn't left empty.
func writeSheet(f *excelize.File, name string, headers []string, rows [][]any, first *bool) error {
	if *first {
		if err := f.SetSheetName("Sheet1", name); err != nil {
			return err
		}
		*first = false
	} else if _, err := f.NewSheet(name); err != nil {
		return err
	}

	headerStyle, err := f.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true}})
	if err != nil {
		return err
	}
	headerRow := make([]any, len(headers))
	for i, h := range headers {
		headerRow[i] = h
	}
	if err := f.SetSheetRow(name, "A1", &headerRow); err != nil {
		return err
	}
	for i, row := range rows {
		cell, _ := excelize.CoordinatesToCellName(1, i+2)
		r := row
		if err := f.SetSheetRow(name, cell, &r); err != nil {
			return err
		}
	}

	lastCol, _ := excelize.ColumnNumberToName(max(len(headers), 1))
	if err := f.SetCellStyle(name, "A1", lastCol+"1", headerStyle); err != nil {
		return err
	}
	if err := f.SetColWidth(name, "A", lastCol, 16); err != nil {
		return err
	}
	// Freeze the header row.
	if err := f.SetPanes(name, &excelize.Panes{
		Freeze: true, YSplit: 1, TopLeftCell: "A2", ActivePane: "bottomLeft",
	}); err != nil {
		return err
	}
	if len(rows) > 0 {
		ref := fmt.Sprintf("A1:%s%d", lastCol, len(rows)+1)
		if err := f.AutoFilter(name, ref, []excelize.AutoFilterOptions{}); err != nil {
			return err
		}
	}
	return nil
}
