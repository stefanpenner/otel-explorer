package export

import (
	"fmt"
	"io"

	docx "github.com/fumiama/go-docx"
)

const docxTableWidth = 9000 // twips (~6.25 inches usable width)

// tone → hex color for DOCX runs (mirrors the HTML/XLSX palette).
var docxToneColor = map[string]string{
	"good": "1A7F37", "warn": "9A6700", "bad": "CF222E", "neutral": "1B1F24",
}

// RenderDOCX writes a structured report document: title, an Executive Summary
// with ranked key findings, a Recommendations section, and the supporting data
// tables — formatted for humans and Google Docs import.
func RenderDOCX(w io.Writer, rep *Report) error {
	d := docx.New().WithDefaultTheme()

	if rep.Kind == KindTrends {
		heading(d, sprintf("CI Trends — %s", rep.Meta.Repo), 1)
	} else {
		heading(d, sprintf("CI Run Analysis — %s", rep.Meta.Repo), 1)
	}
	meta(d, sprintf("Generated %s", rep.Meta.GeneratedAt))
	para(d, leadSentence(rep))

	writeExecutiveSummary(d, reportHighlights(rep))

	if rep.Kind == KindTrends {
		writeTrendBody(d, rep.Trends)
	} else {
		writeRunBody(d, rep.Run)
	}
	_, err := d.WriteTo(w)
	return err
}

func leadSentence(rep *Report) string {
	if rep.Kind == KindTrends {
		t := rep.Trends
		return sprintf("Over the last %d days, %d runs averaged %.0f%% success with a median duration of %s (p95 %s). Trend: %s.",
			t.Days, t.Summary.TotalRuns, t.Summary.AvgSuccessRatePct, humanSec(t.Summary.MedianDurationSec), humanSec(t.Summary.P95DurationSec), t.Summary.TrendDirection)
	}
	r := rep.Run
	return sprintf("Analyzed %d run(s) with %d jobs (%d failed); job success %s, wall clock %s.",
		r.Summary.TotalRuns, r.Summary.TotalJobs, r.Summary.FailedJobs, pctText(r.Summary.JobSuccessRatePct), humanSec(float64(r.Summary.WallClockMs)/1000))
}

func writeExecutiveSummary(d *docx.Docx, hl []Insight) {
	if len(hl) == 0 {
		return
	}
	heading(d, "Executive summary", 2)
	for _, in := range hl {
		p := d.AddParagraph()
		p.AddText("• ").Color(docxToneColor[toneFor(in.Severity)])
		p.AddText(in.Title).Bold().Color(docxToneColor[toneFor(in.Severity)])
		if in.Detail != "" {
			p.AddText("  " + in.Detail).Color("5B6772")
		}
	}

	// Recommendations tied to the findings.
	var recs []Insight
	for _, in := range hl {
		if in.Recommendation != "" {
			recs = append(recs, in)
		}
	}
	if len(recs) > 0 {
		heading(d, "Recommendations", 2)
		for _, in := range recs {
			p := d.AddParagraph()
			p.AddText("• ").Color("1B1F24")
			p.AddText(in.Recommendation)
			if in.URL != "" {
				p.AddText("  (" + in.URL + ")").Italic().Color("5B6772")
			}
		}
	}
}

func writeRunBody(d *docx.Docx, r *RunReport) {
	heading(d, "Summary", 2)
	addTable(d, []string{"Metric", "Value"}, [][]string{
		{"Total runs", itoa(r.Summary.TotalRuns)},
		{"Successful / failed", sprintf("%d / %d", r.Summary.SuccessfulRuns, r.Summary.FailedRuns)},
		{"Total jobs / failed", sprintf("%d / %d", r.Summary.TotalJobs, r.Summary.FailedJobs)},
		{"Job success rate", pctText(r.Summary.JobSuccessRatePct)},
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

func writeTrendBody(d *docx.Docx, t *TrendReport) {
	heading(d, "Summary", 2)
	addTable(d, []string{"Metric", "Value"}, [][]string{
		{"Total runs", itoa(t.Summary.TotalRuns)},
		{"Avg / median / p95 duration", sprintf("%s / %s / %s", humanSec(t.Summary.AvgDurationSec), humanSec(t.Summary.MedianDurationSec), humanSec(t.Summary.P95DurationSec))},
		{"Avg success rate", sprintf("%.1f%%", t.Summary.AvgSuccessRatePct)},
		{"Trend", sprintf("%s (%+.1f%%)", t.Summary.TrendDirection, t.Summary.PercentChange)},
		{"Retry burn", sprintf("%s across %d reruns", humanSec(float64(t.Summary.RerunComputeMs)/1000), t.Summary.RerunRuns)},
		{"Queue ratio", sprintf("%.1f%%", t.QueueStats.QueueRatioPct)},
	})

	if len(t.FlakyJobs) > 0 {
		heading(d, "Flakiest jobs", 2)
		rows := [][]string{}
		for i, f := range t.FlakyJobs {
			if i >= 15 {
				break
			}
			rows = append(rows, []string{f.Name, sprintf("%.1f%%", f.FlakeRatePct), itoa(f.SameSHAFlakes), sprintf("%.2f", f.TransitionScore)})
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
			rows = append(rows, []string{c.Name, humanSec(c.OldAvgSec), humanSec(c.NewAvgSec), sprintf("+%.1f%%", c.PercentChange)})
		}
		addTable(d, []string{"Job", "Was", "Now", "Change"}, rows)
	}

	if len(t.Typical) > 0 {
		heading(d, "Typical run", 2)
		for _, wf := range t.Typical {
			heading(d, wf.Name, 3)
			rows := [][]string{}
			for _, j := range wf.Jobs {
				rows = append(rows, []string{j.Name, humanSec(j.Duration.P50), humanSec(j.Duration.P95), sprintf("%.0f%%", j.SuccessRatePct)})
			}
			addTable(d, []string{"Job", "p50", "p95", "Success"}, rows)
		}
	}
}

// --- docx helpers ---

func heading(d *docx.Docx, text string, level int) {
	d.AddParagraph().Style(fmt.Sprintf("Heading%d", level)).AddText(text)
}

func meta(d *docx.Docx, text string) {
	d.AddParagraph().AddText(text).Italic().Color("5B6772")
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
