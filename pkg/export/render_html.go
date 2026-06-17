package export

import (
	"html/template"
	"io"
)

// RenderHTML writes a self-contained HTML report: opens in any browser,
// imports cleanly into Google Docs, and embeds light CSS plus inline bar
// charts (pure div widths, no JS).
func RenderHTML(w io.Writer, rep *Report) error {
	return htmlTemplate.Execute(w, newHTMLView(rep))
}

// htmlView is the flattened, presentation-ready model the template renders.
type htmlView struct {
	Title       string
	Repo        string
	GeneratedAt string
	Lead        string
	Sections    []htmlSection
}

type htmlSection struct {
	Title string
	Level int        // 2 or 3
	Table *htmlTable // optional
	Note  string     // optional paragraph
}

type htmlTable struct {
	Headers []string
	Rows    []htmlRow
}

type htmlRow struct {
	Cells []string
	// BarPct, when > 0, renders a horizontal bar in the last column scaled to
	// this percent of the row's max.
	BarPct float64
}

func newHTMLView(rep *Report) htmlView {
	switch rep.Kind {
	case KindTrends:
		return trendHTMLView(rep)
	default:
		return runHTMLView(rep)
	}
}

func runHTMLView(rep *Report) htmlView {
	r := rep.Run
	v := htmlView{
		Title:       "CI Run Analysis",
		Repo:        rep.Meta.Repo,
		GeneratedAt: rep.Meta.GeneratedAt,
		Lead: sprintf("%d run(s), %d jobs (%d failed) · job success %.1f%% · wall clock %s",
			r.Summary.TotalRuns, r.Summary.TotalJobs, r.Summary.FailedJobs,
			r.Summary.JobSuccessRatePct, humanSec(float64(r.Summary.WallClockMs)/1000)),
	}

	v.Sections = append(v.Sections, htmlSection{Title: "Summary", Level: 2, Table: &htmlTable{
		Headers: []string{"Metric", "Value"},
		Rows: rows2(
			[2]string{"Total runs", itoa(r.Summary.TotalRuns)},
			[2]string{"Successful / failed", sprintf("%d / %d", r.Summary.SuccessfulRuns, r.Summary.FailedRuns)},
			[2]string{"Total jobs / failed", sprintf("%d / %d", r.Summary.TotalJobs, r.Summary.FailedJobs)},
			[2]string{"Job success rate", sprintf("%.1f%%", r.Summary.JobSuccessRatePct)},
			[2]string{"Max concurrency", itoa(r.Summary.MaxConcurrency)},
			[2]string{"Wall clock", humanSec(float64(r.Summary.WallClockMs) / 1000)},
		),
	}})

	jobs := topJobs(r, 25)
	var maxDur float64
	for _, j := range jobs {
		if d := float64(j.DurationMs); d > maxDur {
			maxDur = d
		}
	}
	tbl := &htmlTable{Headers: []string{"Run", "Job", "Conclusion", "Duration"}}
	for _, j := range jobs {
		tbl.Rows = append(tbl.Rows, htmlRow{
			Cells:  []string{j.run, j.Name, j.Conclusion, humanSec(float64(j.DurationMs) / 1000)},
			BarPct: pctOf(float64(j.DurationMs), maxDur),
		})
	}
	v.Sections = append(v.Sections, htmlSection{Title: "Slowest jobs", Level: 2, Table: tbl})
	return v
}

func trendHTMLView(rep *Report) htmlView {
	t := rep.Trends
	desc := t.Summary.TrendDescription
	if desc == "" {
		desc = t.Summary.TrendDirection
	}
	v := htmlView{
		Title:       sprintf("CI Trends (last %d days)", t.Days),
		Repo:        rep.Meta.Repo,
		GeneratedAt: rep.Meta.GeneratedAt,
		Lead: sprintf("%d runs · avg success %.1f%% · median %s · trend: %s",
			t.Summary.TotalRuns, t.Summary.AvgSuccessRatePct, humanSec(t.Summary.MedianDurationSec), desc),
	}

	v.Sections = append(v.Sections, htmlSection{Title: "Summary", Level: 2, Table: &htmlTable{
		Headers: []string{"Metric", "Value"},
		Rows: rows2(
			[2]string{"Total runs", itoa(t.Summary.TotalRuns)},
			[2]string{"Avg / median / p95 duration", sprintf("%s / %s / %s", humanSec(t.Summary.AvgDurationSec), humanSec(t.Summary.MedianDurationSec), humanSec(t.Summary.P95DurationSec))},
			[2]string{"Avg success rate", sprintf("%.1f%%", t.Summary.AvgSuccessRatePct)},
			[2]string{"Trend", sprintf("%s (%.1f%%)", t.Summary.TrendDirection, t.Summary.PercentChange)},
			[2]string{"Retry burn", sprintf("%s across %d reruns", humanSec(float64(t.Summary.RerunComputeMs)/1000), t.Summary.RerunRuns)},
			[2]string{"Queue ratio", sprintf("%.1f%%", t.QueueStats.QueueRatioPct)},
		),
	}})

	if len(t.FlakyJobs) > 0 {
		tbl := &htmlTable{Headers: []string{"Job", "Flake %", "Same-SHA", "Transition"}}
		for i, f := range t.FlakyJobs {
			if i >= 25 {
				break
			}
			tbl.Rows = append(tbl.Rows, htmlRow{
				Cells:  []string{f.Name, sprintf("%.1f%%", f.FlakeRatePct), itoa(f.SameSHAFlakes), sprintf("%.2f", f.TransitionScore)},
				BarPct: f.FlakeRatePct,
			})
		}
		v.Sections = append(v.Sections, htmlSection{Title: "Flakiest jobs", Level: 2, Table: tbl})
	}

	if len(t.Regressions) > 0 {
		tbl := &htmlTable{Headers: []string{"Job", "Was", "Now", "Change"}}
		for i, c := range t.Regressions {
			if i >= 15 {
				break
			}
			tbl.Rows = append(tbl.Rows, htmlRow{Cells: []string{c.Name, humanSec(c.OldAvgSec), humanSec(c.NewAvgSec), sprintf("+%.1f%%", c.PercentChange)}})
		}
		v.Sections = append(v.Sections, htmlSection{Title: "Top regressions", Level: 2, Table: tbl})
	}

	for _, wf := range t.Typical {
		tbl := &htmlTable{Headers: []string{"Job", "p50", "p95", "Success"}}
		var maxP95 float64
		for _, j := range wf.Jobs {
			if j.Duration.P95 > maxP95 {
				maxP95 = j.Duration.P95
			}
		}
		for _, j := range wf.Jobs {
			tbl.Rows = append(tbl.Rows, htmlRow{
				Cells:  []string{j.Name, humanSec(j.Duration.P50), humanSec(j.Duration.P95), sprintf("%.0f%%", j.SuccessRatePct)},
				BarPct: pctOf(j.Duration.P50, maxP95),
			})
		}
		v.Sections = append(v.Sections, htmlSection{Title: "Typical run · " + wf.Name, Level: 3, Table: tbl})
	}
	return v
}

func rows2(pairs ...[2]string) []htmlRow {
	out := make([]htmlRow, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, htmlRow{Cells: []string{p[0], p[1]}})
	}
	return out
}

func pctOf(v, max float64) float64 {
	if max <= 0 {
		return 0
	}
	return v / max * 100
}

var htmlTemplate = template.Must(template.New("report").Parse(`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>{{.Title}} — {{.Repo}}</title>
<style>
  body{font:14px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#1a1a1a;max-width:980px;margin:2rem auto;padding:0 1rem}
  h1{font-size:1.6rem;margin:0 0 .2rem} h2{font-size:1.2rem;margin:1.8rem 0 .6rem;border-bottom:1px solid #eee;padding-bottom:.2rem} h3{font-size:1rem;margin:1.2rem 0 .4rem}
  .meta{color:#666;font-size:.85rem;margin-bottom:.4rem} .lead{font-size:1rem;margin:.4rem 0 1rem}
  table{border-collapse:collapse;width:100%;margin:.3rem 0 1rem;font-size:.9rem}
  th,td{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #eee} th{background:#fafafa;font-weight:600}
  td.bar{position:relative;min-width:120px} .barfill{display:inline-block;height:.7em;background:#4c8bf5;border-radius:2px;vertical-align:middle}
</style></head><body>
<h1>{{.Title}}</h1>
<div class="meta">{{.Repo}} · generated {{.GeneratedAt}}</div>
<div class="lead">{{.Lead}}</div>
{{range .Sections}}
{{if eq .Level 3}}<h3>{{.Title}}</h3>{{else}}<h2>{{.Title}}</h2>{{end}}
{{if .Note}}<p>{{.Note}}</p>{{end}}
{{with .Table}}<table><thead><tr>{{range .Headers}}<th>{{.}}</th>{{end}}{{if gt (len .Rows) 0}}{{if (index .Rows 0).BarPct}}<th></th>{{end}}{{end}}</tr></thead><tbody>
{{range .Rows}}<tr>{{range .Cells}}<td>{{.}}</td>{{end}}{{if .BarPct}}<td class="bar"><span class="barfill" style="width:{{printf "%.0f" .BarPct}}%"></span></td>{{end}}</tr>
{{end}}</tbody></table>{{end}}
{{end}}
</body></html>
`))
