package export

import (
	"fmt"
	"html/template"
	"io"
	"strings"
)

// RenderHTML writes a self-contained, polished HTML report: KPI cards, an
// executive "key findings" callout, inline SVG charts, and color-coded tables.
// No JavaScript, no external assets — opens in any browser and imports cleanly
// into Google Docs.
func RenderHTML(w io.Writer, rep *Report) error {
	return htmlTemplate.Execute(w, newHTMLView(rep))
}

type htmlView struct {
	Title       string
	Repo        string
	GeneratedAt string
	Lead        string
	KPIs        []kpi
	Findings    []finding
	Charts      []template.HTML
	Sections    []htmlSection
}

type finding struct {
	Tone           string
	Title          string
	Detail         string
	Recommendation string
	URL            string
}

type htmlSection struct {
	Title string
	Level int
	Table *htmlTable
}

type htmlTable struct {
	Headers []string
	Rows    []htmlRow
}

type htmlRow struct {
	Cells  []string
	Tone   string  // row accent: bad | warn | "" — color-codes status
	BarPct float64 // optional trailing bar column
}

func newHTMLView(rep *Report) htmlView {
	v := htmlView{Repo: rep.Meta.Repo, GeneratedAt: rep.Meta.GeneratedAt, Findings: findings(reportHighlights(rep))}
	if rep.Kind == KindTrends {
		fillTrendHTML(&v, rep.Trends)
	} else {
		fillRunHTML(&v, rep.Run)
	}
	return v
}

func findings(ins []Insight) []finding {
	out := make([]finding, 0, len(ins))
	for _, in := range ins {
		out = append(out, finding{
			Tone: toneFor(in.Severity), Title: in.Title, Detail: in.Detail,
			Recommendation: in.Recommendation, URL: in.URL,
		})
	}
	return out
}

func fillRunHTML(v *htmlView, r *RunReport) {
	v.Title = "CI Run Analysis"
	v.Lead = sprintf("%d run(s) · %d jobs · wall clock %s", r.Summary.TotalRuns, r.Summary.TotalJobs, humanSec(float64(r.Summary.WallClockMs)/1000))
	v.KPIs = runKPIs(r)

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
			Tone:   conclusionTone(j.Conclusion),
			BarPct: pctOf(float64(j.DurationMs), maxDur),
		})
	}
	v.Sections = append(v.Sections, htmlSection{Title: "Slowest jobs", Level: 2, Table: tbl})
}

func fillTrendHTML(v *htmlView, t *TrendReport) {
	v.Title = sprintf("CI Trends · last %d days", t.Days)
	v.Lead = sprintf("%d runs · trend %s (%+.0f%%)", t.Summary.TotalRuns, t.Summary.TrendDirection, t.Summary.PercentChange)

	v.KPIs = trendKPIs(t)

	if len(t.DailyDuration) > 1 {
		v.Charts = append(v.Charts, svgArea("Run duration (median, sec) by day", t.DailyDuration, "sec"))
	}
	if len(t.DailySuccess) > 1 {
		v.Charts = append(v.Charts, svgArea("Success rate (%) by day", t.DailySuccess, "%"))
	}

	if len(t.FlakyJobs) > 0 {
		tbl := &htmlTable{Headers: []string{"Job", "Flake %", "Same-SHA", "Transition"}}
		for i, f := range t.FlakyJobs {
			if i >= 25 {
				break
			}
			tbl.Rows = append(tbl.Rows, htmlRow{
				Cells:  []string{f.Name, sprintf("%.1f%%", f.FlakeRatePct), itoa(f.SameSHAFlakes), sprintf("%.2f", f.TransitionScore)},
				Tone:   flakeTone(f.FlakeRatePct),
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
			tbl.Rows = append(tbl.Rows, htmlRow{Cells: []string{c.Name, humanSec(c.OldAvgSec), humanSec(c.NewAvgSec), sprintf("+%.1f%%", c.PercentChange)}, Tone: "warn"})
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
}

func pctOf(v, max float64) float64 {
	if max <= 0 {
		return 0
	}
	return v / max * 100
}

// svgArea renders a compact area+line chart for a daily series as inline SVG.
// Generated entirely from numeric data we control, so it is safe trusted HTML.
func svgArea(title string, pts []DailyPoint, unit string) template.HTML {
	const w, h = 680.0, 170.0
	const padL, padR, padT, padB = 8.0, 8.0, 26.0, 22.0
	innerW, innerH := w-padL-padR, h-padT-padB

	var maxV float64
	for _, p := range pts {
		if p.Value > maxV {
			maxV = p.Value
		}
	}
	if maxV <= 0 {
		maxV = 1
	}
	n := len(pts)
	x := func(i int) float64 { return padL + innerW*float64(i)/float64(n-1) }
	y := func(v float64) float64 { return padT + innerH*(1-v/maxV) }

	var line, area strings.Builder
	for i, p := range pts {
		cmd := "L"
		if i == 0 {
			cmd = "M"
		}
		fmt.Fprintf(&line, "%s%.1f %.1f ", cmd, x(i), y(p.Value))
	}
	fmt.Fprintf(&area, "M%.1f %.1f ", x(0), padT+innerH)
	for i, p := range pts {
		fmt.Fprintf(&area, "L%.1f %.1f ", x(i), y(p.Value))
	}
	fmt.Fprintf(&area, "L%.1f %.1f Z", x(n-1), padT+innerH)

	last := pts[n-1]
	var b strings.Builder
	fmt.Fprintf(&b, `<figure class="chart"><figcaption>%s</figcaption>`, template.HTMLEscapeString(title))
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" preserveAspectRatio="none" role="img" aria-label="%s">`, w, h, template.HTMLEscapeString(title))
	// baseline + max gridline
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="grid"/>`, padL, padT, padL+innerW, padT)
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="grid"/>`, padL, padT+innerH, padL+innerW, padT+innerH)
	fmt.Fprintf(&b, `<path d="%s" class="area"/>`, area.String())
	fmt.Fprintf(&b, `<path d="%s" class="line"/>`, strings.TrimSpace(line.String()))
	fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="2.5" class="dot"/>`, x(n-1), y(last.Value))
	// labels: max (top-left), last value (near dot), first/last dates
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="yl">%s%s</text>`, padL+2, padT-6, trimNum(maxV), unitSuffix(unit))
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="xl">%s</text>`, padL, h-6, template.HTMLEscapeString(pts[0].Date))
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="xl xr">%s</text>`, padL+innerW, h-6, template.HTMLEscapeString(last.Date))
	b.WriteString(`</svg></figure>`)
	return template.HTML(b.String())
}

func unitSuffix(u string) string {
	if u == "%" {
		return "%"
	}
	return ""
}

func trimNum(v float64) string {
	return strings.TrimSuffix(strings.TrimSuffix(sprintf("%.1f", v), "0"), ".")
}

var htmlTemplate = template.Must(template.New("report").Parse(`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} — {{.Repo}}</title>
<style>
  :root{--bg:#fff;--fg:#1b1f24;--muted:#5b6772;--line:#e7ebef;--card:#f7f9fb;
        --good:#1a7f37;--good-bg:#dafbe1;--warn:#9a6700;--warn-bg:#fff8c5;--bad:#cf222e;--bad-bg:#ffebe9;--accent:#4c8bf5;}
  *{box-sizing:border-box}
  body{font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;color:var(--fg);background:var(--bg);margin:0;padding:2rem 1rem;}
  .wrap{max-width:1000px;margin:0 auto}
  h1{font-size:1.7rem;margin:0 0 .15rem} h2{font-size:1.2rem;margin:2rem 0 .6rem;padding-bottom:.3rem;border-bottom:2px solid var(--line)} h3{font-size:1.02rem;margin:1.3rem 0 .4rem;color:var(--muted)}
  .meta{color:var(--muted);font-size:.85rem} .lead{font-size:1.05rem;margin:.5rem 0 1.3rem}
  .kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:.75rem;margin:1rem 0 1.6rem}
  .kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:.8rem .9rem}
  .kpi .v{font-size:1.7rem;font-weight:700;line-height:1.1} .kpi .l{font-size:.78rem;color:var(--muted);text-transform:uppercase;letter-spacing:.03em} .kpi .s{font-size:.78rem;color:var(--muted);margin-top:.15rem}
  .kpi.good .v{color:var(--good)} .kpi.warn .v{color:var(--warn)} .kpi.bad .v{color:var(--bad)}
  .findings{display:grid;gap:.55rem;margin:.4rem 0 1.4rem}
  .finding{border-left:4px solid var(--line);background:var(--card);border-radius:0 8px 8px 0;padding:.6rem .85rem}
  .finding.good{border-color:var(--good);background:var(--good-bg)} .finding.warn{border-color:var(--warn);background:var(--warn-bg)} .finding.bad{border-color:var(--bad);background:var(--bad-bg)} .finding.neutral{border-color:var(--accent)}
  .finding .ft{font-weight:650} .finding .fd{color:var(--muted);font-size:.9rem;margin-top:.1rem} .finding .fr{font-size:.88rem;margin-top:.25rem} .finding .fr::before{content:"→ "}
  figure.chart{margin:1rem 0;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:.7rem .8rem}
  figure.chart figcaption{font-size:.8rem;color:var(--muted);text-transform:uppercase;letter-spacing:.03em;margin-bottom:.3rem}
  figure.chart svg{width:100%;height:170px;display:block}
  .area{fill:var(--accent);opacity:.13} .line{fill:none;stroke:var(--accent);stroke-width:2;stroke-linejoin:round;stroke-linecap:round} .dot{fill:var(--accent)} .grid{stroke:var(--line);stroke-width:1}
  text.yl,text.xl{fill:var(--muted);font-size:11px} text.xr{text-anchor:end}
  table{border-collapse:collapse;width:100%;margin:.3rem 0 1rem;font-size:.9rem}
  th,td{text-align:left;padding:.4rem .6rem;border-bottom:1px solid var(--line)} th{font-weight:650;color:var(--muted);font-size:.8rem;text-transform:uppercase;letter-spacing:.02em}
  tr.bad td:first-child{box-shadow:inset 3px 0 var(--bad)} tr.warn td:first-child{box-shadow:inset 3px 0 var(--warn)}
  td.bar{min-width:120px} .barfill{display:inline-block;height:.7em;background:var(--accent);border-radius:3px;vertical-align:middle;min-width:1px}
  footer{margin-top:2rem;color:var(--muted);font-size:.78rem;border-top:1px solid var(--line);padding-top:.6rem}
</style></head><body><div class="wrap">
<h1>{{.Title}}</h1>
<div class="meta">{{.Repo}} · generated {{.GeneratedAt}}</div>
<div class="lead">{{.Lead}}</div>

{{if .KPIs}}<div class="kpis">{{range .KPIs}}<div class="kpi {{.Tone}}"><div class="l">{{.Label}}</div><div class="v">{{.Value}}</div>{{if .Sub}}<div class="s">{{.Sub}}</div>{{end}}</div>{{end}}</div>{{end}}

{{if .Findings}}<h2>Key findings</h2><div class="findings">{{range .Findings}}<div class="finding {{.Tone}}"><div class="ft">{{.Title}}</div>{{if .Detail}}<div class="fd">{{.Detail}}</div>{{end}}{{if .Recommendation}}<div class="fr">{{.Recommendation}}{{if .URL}} (<a href="{{.URL}}">details</a>){{end}}</div>{{else if .URL}}<div class="fr"><a href="{{.URL}}">details</a></div>{{end}}</div>{{end}}</div>{{end}}

{{range .Charts}}{{.}}{{end}}

{{range .Sections}}
{{if eq .Level 3}}<h3>{{.Title}}</h3>{{else}}<h2>{{.Title}}</h2>{{end}}
{{with .Table}}<table><thead><tr>{{range .Headers}}<th>{{.}}</th>{{end}}{{if gt (len .Rows) 0}}{{if (index .Rows 0).BarPct}}<th></th>{{end}}{{end}}</tr></thead><tbody>
{{range .Rows}}<tr{{if .Tone}} class="{{.Tone}}"{{end}}>{{range .Cells}}<td>{{.}}</td>{{end}}{{if .BarPct}}<td class="bar"><span class="barfill" style="width:{{printf "%.0f" .BarPct}}%"></span></td>{{end}}</tr>
{{end}}</tbody></table>{{end}}
{{end}}
<footer>Generated by ote</footer>
</div></body></html>
`))
