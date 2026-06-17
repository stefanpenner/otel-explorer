package export

import (
	"fmt"
	"io"

	docx "github.com/fumiama/go-docx"
)

const docxTableWidth = 9000 // twips (~6.25 inches usable width)

// RenderDOCX writes the report as a Word document with headings, a narrative
// summary, and tables — formatted for humans to read (and import natively into
// Google Docs).
func RenderDOCX(w io.Writer, rep *Report) error {
	d := docx.New().WithDefaultTheme()

	switch rep.Kind {
	case KindRunAnalysis:
		renderRunDocx(d, rep)
	case KindTrends:
		renderTrendDocx(d, rep)
	}

	_, err := d.WriteTo(w)
	return err
}

func renderRunDocx(d *docx.Docx, rep *Report) {
	r := rep.Run
	heading(d, fmt.Sprintf("CI Run Analysis — %s", rep.Meta.Repo), 1)
	para(d, fmt.Sprintf("Generated %s. %d run(s), %d jobs (%d failed), job success rate %.1f%%, wall clock %s.",
		rep.Meta.GeneratedAt, r.Summary.TotalRuns, r.Summary.TotalJobs, r.Summary.FailedJobs,
		r.Summary.JobSuccessRatePct, humanSec(float64(r.Summary.WallClockMs)/1000)))

	heading(d, "Summary", 2)
	addTable(d, []string{"Metric", "Value"}, [][]string{
		{"Total runs", itoa(r.Summary.TotalRuns)},
		{"Successful / failed", fmt.Sprintf("%d / %d", r.Summary.SuccessfulRuns, r.Summary.FailedRuns)},
		{"Total jobs / failed", fmt.Sprintf("%d / %d", r.Summary.TotalJobs, r.Summary.FailedJobs)},
		{"Job success rate", fmt.Sprintf("%.1f%%", r.Summary.JobSuccessRatePct)},
		{"Max concurrency", itoa(r.Summary.MaxConcurrency)},
		{"Wall clock", humanSec(float64(r.Summary.WallClockMs) / 1000)},
	})

	heading(d, "Slowest jobs", 2)
	rows := [][]string{}
	for _, j := range topJobs(r, 15) {
		rows = append(rows, []string{j.run, j.Name, j.Conclusion, humanSec(float64(j.DurationMs) / 1000)})
	}
	addTable(d, []string{"Run", "Job", "Conclusion", "Duration"}, rows)
}

func renderTrendDocx(d *docx.Docx, rep *Report) {
	t := rep.Trends
	heading(d, fmt.Sprintf("CI Trends — %s (last %d days)", rep.Meta.Repo, t.Days), 1)
	desc := t.Summary.TrendDescription
	if desc == "" {
		desc = t.Summary.TrendDirection
	}
	para(d, fmt.Sprintf("Generated %s. %d runs, avg success %.1f%%, median duration %s. Trend: %s.",
		rep.Meta.GeneratedAt, t.Summary.TotalRuns, t.Summary.AvgSuccessRatePct,
		humanSec(t.Summary.MedianDurationSec), desc))

	heading(d, "Summary", 2)
	addTable(d, []string{"Metric", "Value"}, [][]string{
		{"Total runs", itoa(t.Summary.TotalRuns)},
		{"Avg / median / p95 duration", fmt.Sprintf("%s / %s / %s", humanSec(t.Summary.AvgDurationSec), humanSec(t.Summary.MedianDurationSec), humanSec(t.Summary.P95DurationSec))},
		{"Avg success rate", fmt.Sprintf("%.1f%%", t.Summary.AvgSuccessRatePct)},
		{"Trend", fmt.Sprintf("%s (%.1f%%)", t.Summary.TrendDirection, t.Summary.PercentChange)},
		{"Retry burn", fmt.Sprintf("%s across %d reruns", humanSec(float64(t.Summary.RerunComputeMs)/1000), t.Summary.RerunRuns)},
		{"Queue ratio", fmt.Sprintf("%.1f%%", t.QueueStats.QueueRatioPct)},
	})

	if len(t.FlakyJobs) > 0 {
		heading(d, "Flakiest jobs", 2)
		rows := [][]string{}
		for i, f := range t.FlakyJobs {
			if i >= 15 {
				break
			}
			rows = append(rows, []string{f.Name, fmt.Sprintf("%.1f%%", f.FlakeRatePct), itoa(f.SameSHAFlakes), fmt.Sprintf("%.2f", f.TransitionScore)})
		}
		addTable(d, []string{"Job", "Flake %", "Same-SHA", "Transition"}, rows)
	}

	if len(t.Regressions) > 0 {
		heading(d, "Top regressions", 2)
		rows := [][]string{}
		for i, c := range t.Regressions {
			if i >= 10 {
				break
			}
			rows = append(rows, []string{c.Name, humanSec(c.OldAvgSec), humanSec(c.NewAvgSec), fmt.Sprintf("+%.1f%%", c.PercentChange)})
		}
		addTable(d, []string{"Job", "Was", "Now", "Change"}, rows)
	}

	if len(t.Typical) > 0 {
		heading(d, "Typical run", 2)
		for _, wf := range t.Typical {
			heading(d, wf.Name, 3)
			rows := [][]string{}
			for _, j := range wf.Jobs {
				rows = append(rows, []string{j.Name, humanSec(j.Duration.P50), humanSec(j.Duration.P95), fmt.Sprintf("%.0f%%", j.SuccessRatePct)})
			}
			addTable(d, []string{"Job", "p50", "p95", "Success"}, rows)
		}
	}
}

// --- docx helpers ---

func heading(d *docx.Docx, text string, level int) {
	d.AddParagraph().Style(fmt.Sprintf("Heading%d", level)).AddText(text)
}

func para(d *docx.Docx, text string) {
	d.AddParagraph().AddText(text)
}

// addTable writes a header row (bold) followed by data rows. A nil/empty rows
// slice still emits the header so the section isn't blank.
func addTable(d *docx.Docx, headers []string, rows [][]string) {
	cols := len(headers)
	if cols == 0 {
		return
	}
	tbl := d.AddTable(len(rows)+1, cols, docxTableWidth, nil)
	for c, h := range headers {
		tbl.TableRows[0].TableCells[c].AddParagraph().AddText(h).Bold()
	}
	for r, row := range rows {
		for c := 0; c < cols; c++ {
			val := ""
			if c < len(row) {
				val = row[c]
			}
			tbl.TableRows[r+1].TableCells[c].AddParagraph().AddText(val)
		}
	}
}

type jobRow struct {
	Job
	run string
}

// topJobs returns the n longest jobs across all runs, longest first.
func topJobs(r *RunReport, n int) []jobRow {
	var all []jobRow
	for _, run := range r.Runs {
		label := run.DisplayName
		if label == "" {
			label = run.Identifier
		}
		for _, j := range run.Jobs {
			all = append(all, jobRow{Job: j, run: label})
		}
	}
	for i := 1; i < len(all); i++ {
		for k := i; k > 0 && all[k].DurationMs > all[k-1].DurationMs; k-- {
			all[k], all[k-1] = all[k-1], all[k]
		}
	}
	if len(all) > n {
		all = all[:n]
	}
	return all
}
