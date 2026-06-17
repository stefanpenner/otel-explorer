package export

import (
	"fmt"
	"io"

	"github.com/xuri/excelize/v2"
)

// RenderXLSX writes a dashboard-style workbook: an Overview sheet with KPI
// cards and key findings, then one Excel table per dataset with banded rows,
// auto-filters, number formats, and conditional formatting (data bars on
// durations, color scales on flake rate, red fills on failures). Trends also
// get a native line chart of the daily duration series.
func RenderXLSX(w io.Writer, rep *Report) error {
	f := excelize.NewFile()
	defer f.Close()

	st, err := newXLSXStyles(f)
	if err != nil {
		return err
	}
	xb := &xlsxBuilder{f: f, st: st, sheetN: 0}

	if err := xb.overview(rep); err != nil {
		return err
	}
	switch rep.Kind {
	case KindRunAnalysis:
		if err := xb.runSheets(rep.Run); err != nil {
			return err
		}
	case KindTrends:
		if err := xb.trendSheets(rep.Trends); err != nil {
			return err
		}
	}

	_, err = f.WriteTo(w)
	return err
}

type xlsxBuilder struct {
	f      *excelize.File
	st     *xlsxStyles
	sheetN int
}

// sheet creates a new sheet (renaming the default Sheet1 for the first one)
// and returns its name.
func (b *xlsxBuilder) sheet(name string) (string, error) {
	if b.sheetN == 0 {
		if err := b.f.SetSheetName("Sheet1", name); err != nil {
			return "", err
		}
	} else if _, err := b.f.NewSheet(name); err != nil {
		return "", err
	}
	b.sheetN++
	return name, nil
}

// --- Overview: KPI cards + key findings ---

func (b *xlsxBuilder) overview(rep *Report) error {
	sheet, err := b.sheet("Overview")
	if err != nil {
		return err
	}
	f := b.f
	_ = f.SetSheetView(sheet, 0, &excelize.ViewOptions{ShowGridLines: boolPtr(false)})
	_ = f.SetColWidth(sheet, "A", "L", 11.5)

	title := "CI Run Analysis"
	if rep.Kind == KindTrends {
		title = sprintf("CI Trends · last %d days", rep.Trends.Days)
	}
	_ = f.SetCellValue(sheet, "A1", title)
	_ = f.SetCellStyle(sheet, "A1", "A1", b.st.title)
	_ = f.SetCellValue(sheet, "A2", sprintf("%s · generated %s", rep.Meta.Repo, rep.Meta.GeneratedAt))
	_ = f.SetCellStyle(sheet, "A2", "A2", b.st.subtitle)

	var kpis []kpi
	if rep.Kind == KindTrends {
		kpis = trendKPIs(rep.Trends)
	} else {
		kpis = runKPIs(rep.Run)
	}
	// Cards: 3-column blocks (2 cols of content + 1 spacer) starting at row 4.
	row := 4
	for i, k := range kpis {
		col := 1 + (i%4)*3
		if i > 0 && i%4 == 0 {
			row += 4
		}
		if err := b.card(sheet, col, row, k); err != nil {
			return err
		}
	}
	row += 5

	// Key findings.
	hl := reportHighlights(rep)
	if len(hl) > 0 {
		cell, _ := excelize.CoordinatesToCellName(1, row)
		_ = f.SetCellValue(sheet, cell, "Key findings")
		_ = f.SetCellStyle(sheet, cell, cell, b.st.h2)
		row++
		for _, in := range hl {
			tagCell, _ := excelize.CoordinatesToCellName(1, row)
			txtCell, _ := excelize.CoordinatesToCellName(2, row)
			txtEnd, _ := excelize.CoordinatesToCellName(11, row)
			_ = f.MergeCell(sheet, txtCell, txtEnd)
			_ = f.SetCellValue(sheet, tagCell, severityTag(in.Severity))
			_ = f.SetCellStyle(sheet, tagCell, tagCell, b.st.tag[toneFor(in.Severity)])
			text := in.Title
			if in.Detail != "" {
				text += " — " + in.Detail
			}
			if in.Recommendation != "" {
				text += "  → " + in.Recommendation
			}
			_ = f.SetCellValue(sheet, txtCell, text)
			_ = f.SetCellStyle(sheet, txtCell, txtEnd, b.st.finding)
			row++
		}
	}
	return nil
}

// card writes a 2-column KPI card whose top-left is (col,row): a label row and
// a merged value block beneath, tinted by tone.
func (b *xlsxBuilder) card(sheet string, col, row int, k kpi) error {
	f := b.f
	c2 := col + 1
	lblTL, _ := excelize.CoordinatesToCellName(col, row)
	lblBR, _ := excelize.CoordinatesToCellName(c2, row)
	valTL, _ := excelize.CoordinatesToCellName(col, row+1)
	valBR, _ := excelize.CoordinatesToCellName(c2, row+1)
	subTL, _ := excelize.CoordinatesToCellName(col, row+2)
	subBR, _ := excelize.CoordinatesToCellName(c2, row+2)
	if err := f.MergeCell(sheet, lblTL, lblBR); err != nil {
		return err
	}
	_ = f.MergeCell(sheet, valTL, valBR)
	_ = f.MergeCell(sheet, subTL, subBR)
	_ = f.SetRowHeight(sheet, row+1, 26)

	_ = f.SetCellValue(sheet, lblTL, k.Label)
	_ = f.SetCellValue(sheet, valTL, k.Value)
	_ = f.SetCellValue(sheet, subTL, k.Sub)
	_ = f.SetCellStyle(sheet, lblTL, lblBR, b.st.cardLabel)
	_ = f.SetCellStyle(sheet, valTL, valBR, b.st.cardValue[toneKey(k.Tone)])
	_ = f.SetCellStyle(sheet, subTL, subBR, b.st.cardSub)
	return nil
}

// --- Run analysis sheets ---

func (b *xlsxBuilder) runSheets(r *RunReport) error {
	if r == nil {
		return nil
	}
	var jobs [][]any
	var jobConclusions []string
	var steps [][]any
	for _, run := range r.Runs {
		for _, j := range run.Jobs {
			jobs = append(jobs, []any{run.Identifier, run.DisplayName, j.Name, j.Status, j.Conclusion,
				round1(float64(j.DurationMs) / 1000), j.Required, j.URL})
			jobConclusions = append(jobConclusions, j.Conclusion)
		}
		for _, s := range run.Steps {
			steps = append(steps, []any{run.Identifier, s.Job, s.Name, round1(float64(s.DurationMs) / 1000), s.URL})
		}
	}

	jh := []string{"Run", "Source", "Job", "Status", "Conclusion", "Duration (sec)", "Required", "URL"}
	if err := b.table("Jobs", jh, jobs, tableOpts{
		durationCol: 6, // data bars on Duration
		failCol:     5, // red fill where Conclusion is failure
		failRows:    jobConclusions,
	}); err != nil {
		return err
	}
	if len(steps) > 0 {
		if err := b.table("Steps", []string{"Run", "Job", "Step", "Duration (sec)", "URL"}, steps, tableOpts{durationCol: 4}); err != nil {
			return err
		}
	}
	return nil
}

// --- Trend sheets ---

func (b *xlsxBuilder) trendSheets(t *TrendReport) error {
	if t == nil {
		return nil
	}
	var typical [][]any
	for _, w := range t.Typical {
		for _, j := range w.Jobs {
			typical = append(typical, []any{w.Name, j.Name, j.Samples, round1(j.PresenceRatePct), round1(j.SuccessRatePct),
				round1(j.Duration.P5), round1(j.Duration.P25), round1(j.Duration.P50), round1(j.Duration.P75), round1(j.Duration.P95), j.TrendDirection})
		}
	}
	if len(typical) > 0 {
		if err := b.table("Typical Run", []string{"Workflow", "Job", "Samples", "Presence %", "Success %", "p5", "p25", "p50", "p75", "p95", "Trend"}, typical, tableOpts{durationCol: 8}); err != nil {
			return err
		}
	}

	var flaky [][]any
	for _, fj := range t.FlakyJobs {
		flaky = append(flaky, []any{fj.Name, fj.TotalRuns, fj.SuccessCount, fj.FailureCount,
			round1(fj.FlakeRatePct), fj.SameSHAFlakes, round2(fj.TransitionScore), fj.SampleURL})
	}
	if len(flaky) > 0 {
		if err := b.table("Flaky Jobs", []string{"Job", "Runs", "Pass", "Fail", "Flake %", "Same-SHA", "Transition", "Sample URL"}, flaky, tableOpts{colorScaleCol: 5}); err != nil {
			return err
		}
	}

	if rows := changeRows(t.Regressions); len(rows) > 0 {
		if err := b.table("Regressions", changeHeaders(), rows, tableOpts{}); err != nil {
			return err
		}
	}
	if rows := changeRows(t.Improvements); len(rows) > 0 {
		if err := b.table("Improvements", changeHeaders(), rows, tableOpts{}); err != nil {
			return err
		}
	}
	if len(t.Hourly) > 0 {
		var hourly [][]any
		for _, h := range t.Hourly {
			hourly = append(hourly, []any{h.Hour, h.RunCount, round1(h.QueueP50Sec), round1(h.DurationP50Sec)})
		}
		if err := b.table("Hourly", []string{"UTC Hour", "Runs", "Queue p50 (sec)", "Duration p50 (sec)"}, hourly, tableOpts{}); err != nil {
			return err
		}
	}
	if len(t.DailyDuration) > 0 {
		if err := b.dailySheet(t); err != nil {
			return err
		}
	}
	return nil
}

func (b *xlsxBuilder) dailySheet(t *TrendReport) error {
	success := map[string]float64{}
	for _, p := range t.DailySuccess {
		success[p.Date] = p.Value
	}
	var daily [][]any
	for _, p := range t.DailyDuration {
		daily = append(daily, []any{p.Date, round1(p.Value), p.Count, round1(success[p.Date])})
	}
	name, err := b.sheetTable("Daily", []string{"Date", "Avg Duration (sec)", "Runs", "Success %"}, daily, tableOpts{})
	if err != nil {
		return err
	}
	// Native line chart of avg duration by day.
	n := len(daily)
	if n >= 2 {
		if err := b.f.AddChart(name, "F2", &excelize.Chart{
			Type: excelize.Line,
			Series: []excelize.ChartSeries{{
				Name:       sprintf("'%s'!$B$1", name),
				Categories: sprintf("'%s'!$A$2:$A$%d", name, n+1),
				Values:     sprintf("'%s'!$B$2:$B$%d", name, n+1),
				Line:       excelize.ChartLine{Width: 2},
			}},
			Title:     []excelize.RichTextRun{{Text: "Run duration by day"}},
			Legend:    excelize.ChartLegend{Position: "none"},
			Dimension: excelize.ChartDimension{Width: 560, Height: 280},
		}); err != nil {
			return err
		}
	}
	return nil
}

// --- generic table writer ---

type tableOpts struct {
	durationCol   int      // 1-based column to decorate with data bars (0 = none)
	colorScaleCol int      // 1-based column for a red→green color scale (0 = none)
	failCol       int      // 1-based column whose failure value triggers a red row fill
	failRows      []string // per-data-row conclusion, parallel to rows (for failCol)
}

func (b *xlsxBuilder) table(name string, headers []string, rows [][]any, opts tableOpts) error {
	_, err := b.sheetTable(name, headers, rows, opts)
	return err
}

// sheetTable creates a sheet, writes a styled header + rows, registers an Excel
// table, applies conditional formatting per opts, and freezes the header.
// Returns the sheet name.
func (b *xlsxBuilder) sheetTable(name string, headers []string, rows [][]any, opts tableOpts) (string, error) {
	sheet, err := b.sheet(name)
	if err != nil {
		return "", err
	}
	f := b.f

	header := make([]any, len(headers))
	for i, h := range headers {
		header[i] = h
	}
	if err := f.SetSheetRow(sheet, "A1", &header); err != nil {
		return "", err
	}
	for i, row := range rows {
		cell, _ := excelize.CoordinatesToCellName(1, i+2)
		r := row
		if err := f.SetSheetRow(sheet, cell, &r); err != nil {
			return "", err
		}
	}

	lastCol, _ := excelize.ColumnNumberToName(max(len(headers), 1))
	lastRow := len(rows) + 1
	ref := fmt.Sprintf("A1:%s%d", lastCol, lastRow)

	// Excel table → banded rows + auto-filter + a clean style.
	if len(rows) > 0 {
		showRowStripes := true
		if err := f.AddTable(sheet, &excelize.Table{
			Range: ref, Name: tableName(name), StyleName: "TableStyleMedium2",
			ShowRowStripes: &showRowStripes,
		}); err != nil {
			return "", err
		}
	} else {
		_ = f.SetCellStyle(sheet, "A1", lastCol+"1", b.st.tableHeader)
	}
	_ = f.SetColWidth(sheet, "A", lastCol, 15)
	_ = f.SetPanes(sheet, &excelize.Panes{Freeze: true, YSplit: 1, TopLeftCell: "A2", ActivePane: "bottomLeft"})

	if len(rows) == 0 {
		return sheet, nil
	}

	// Data bars on a duration column.
	if opts.durationCol > 0 {
		col, _ := excelize.ColumnNumberToName(opts.durationCol)
		rng := fmt.Sprintf("%s2:%s%d", col, col, lastRow)
		_ = f.SetConditionalFormat(sheet, rng, []excelize.ConditionalFormatOptions{{
			Type: "data_bar", Criteria: "=", MinType: "min", MaxType: "max",
			BarColor: "#638EC6", BarSolid: true,
		}})
	}
	// Color scale (e.g. flake %): green → yellow → red.
	if opts.colorScaleCol > 0 {
		col, _ := excelize.ColumnNumberToName(opts.colorScaleCol)
		rng := fmt.Sprintf("%s2:%s%d", col, col, lastRow)
		_ = f.SetConditionalFormat(sheet, rng, []excelize.ConditionalFormatOptions{{
			Type: "3_color_scale", Criteria: "=",
			MinType: "min", MidType: "percentile", MaxType: "max",
			MinColor: "#63BE7B", MidColor: "#FFEB84", MaxColor: "#F8696B", MidValue: "50",
		}})
	}
	// Red fill on failed rows (per the conclusion column).
	if opts.failCol > 0 && len(opts.failRows) == len(rows) {
		for i, concl := range opts.failRows {
			if concl == "failure" || concl == "timed_out" {
				col, _ := excelize.ColumnNumberToName(opts.failCol)
				cell := fmt.Sprintf("%s%d", col, i+2)
				_ = f.SetCellStyle(sheet, cell, cell, b.st.failCell)
			}
		}
	}
	return sheet, nil
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

func severityTag(s Severity) string {
	switch s {
	case SeverityBad:
		return "!"
	case SeverityWarn:
		return "~"
	case SeverityGood:
		return "✓"
	default:
		return "i"
	}
}

func toneKey(tone string) string {
	if tone == "" {
		return "neutral"
	}
	return tone
}

// tableName sanitizes a sheet name into a valid Excel table name (no spaces).
func tableName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == ' ' {
			out = append(out, '_')
		} else {
			out = append(out, r)
		}
	}
	return "tbl_" + string(out)
}

func boolPtr(b bool) *bool { return &b }
