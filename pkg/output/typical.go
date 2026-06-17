package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

const (
	typicalTrackWidth   = 44
	typicalNameWidth    = 30
	maxTypicalWorkflows = 8
	maxTypicalJobs      = 15
)

// renderTypicalRun draws the statistically typical pipeline per workflow:
// one Gantt-style row per job placed at its median start offset, with the
// bar shaded from median duration (█) through p75 (▓) to p95 (░) to show
// the right tail.
func renderTypicalRun(w io.Writer, typical *analyzer.TypicalRun) {
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  Median timelines across %d runs", typical.SampledRuns)
	if typical.TotalCommits > 0 {
		fmt.Fprintf(w, " (%d of %d commits)", typical.CommitsCovered, typical.TotalCommits)
	}
	fmt.Fprintln(w, ". Skipped/cancelled jobs excluded.")

	shown := typical.Workflows
	if len(shown) > maxTypicalWorkflows {
		shown = shown[:maxTypicalWorkflows]
	}

	for _, wf := range shown {
		fmt.Fprintln(w)
		header := fmt.Sprintf("▸ %s", wf.Name)
		coverage := fmt.Sprintf("%d/%d runs sampled", wf.SampledRuns, wf.TotalRuns)
		if wf.SampledRuns == wf.TotalRuns {
			coverage = fmt.Sprintf("all %d runs", wf.TotalRuns)
		}
		stats := fmt.Sprintf("%s — run p50 %s  p95 %s",
			coverage,
			utils.HumanizeTime(wf.RunDuration.P50),
			utils.HumanizeTime(wf.RunDuration.P95))
		fmt.Fprintf(w, "  %s  %s\n", valueStyle.Render(header), dimStyle.Render(stats))
		renderTypicalWorkflow(w, wf)
	}

	if hidden := len(typical.Workflows) - len(shown); hidden > 0 {
		fmt.Fprintf(w, "\n  %s\n", dimStyle.Render(fmt.Sprintf("... and %d more workflows (use --workflow=<file> to focus)", hidden)))
	}
	fmt.Fprintf(w, "\n  %s\n", dimStyle.Render("▒ queue  █ median  ▓ to p75  ░ to p95 — bars start at each job's median offset from run start"))
}

func renderTypicalWorkflow(w io.Writer, wf analyzer.TypicalWorkflow) {
	// Scale the track to the widest p95 envelope: the run itself or the
	// latest-ending job.
	maxSec := wf.RunDuration.P95
	for _, job := range wf.Jobs {
		if end := job.StartOffset.P50 + job.Duration.P95; end > maxSec {
			maxSec = end
		}
	}
	if maxSec <= 0 {
		maxSec = 1
	}
	scaleX := func(sec float64) int {
		x := int(sec / maxSec * float64(typicalTrackWidth))
		if x < 0 {
			x = 0
		}
		if x > typicalTrackWidth {
			x = typicalTrackWidth
		}
		return x
	}

	// Axis header: 0 ... maxSec
	endLabel := utils.HumanizeTime(maxSec)
	dashes := typicalTrackWidth - 1 - len(endLabel)
	if dashes < 0 {
		dashes = 0
	}
	axis := "0" + strings.Repeat("─", dashes) + endLabel
	fmt.Fprintf(w, "  %-*s %s\n", typicalNameWidth, "", dimStyle.Render(axis))

	jobs := wf.Jobs
	if len(jobs) > maxTypicalJobs {
		jobs = jobs[:maxTypicalJobs]
	}

	for _, job := range jobs {
		start := scaleX(job.StartOffset.P50)
		p50End := scaleX(job.StartOffset.P50 + job.Duration.P50)
		p75End := scaleX(job.StartOffset.P50 + job.Duration.P75)
		p95End := scaleX(job.StartOffset.P50 + job.Duration.P95)
		if p50End <= start {
			p50End = start + 1 // always show at least one cell of bar
		}
		if p50End > typicalTrackWidth {
			p50End = typicalTrackWidth
		}
		if p75End < p50End {
			p75End = p50End
		}
		if p95End < p75End {
			p95End = p75End
		}

		// Queue prefix: the median wait before the job started, drawn dim so
		// "waiting for a runner" reads differently from "running".
		queueStart := start
		if job.QueueTime.P50 > 0 {
			queueStart = scaleX(job.StartOffset.P50 - job.QueueTime.P50)
			if queueStart > start {
				queueStart = start
			}
		}

		barStyle := successStyle
		if job.SuccessRate < 90 {
			barStyle = failureStyle
		} else if job.SuccessRate < 99.5 {
			barStyle = warningStyle
		}
		// Style segments independently: a nested style reset would cancel the
		// outer bar color for the rest of the row.
		track := strings.Repeat(" ", queueStart) +
			dimStyle.Render(strings.Repeat("▒", start-queueStart)) +
			barStyle.Render(strings.Repeat("█", p50End-start)+
				strings.Repeat("▓", p75End-p50End)+
				strings.Repeat("░", p95End-p75End)) +
			strings.Repeat(" ", typicalTrackWidth-p95End)

		trendIcon := dimStyle.Render("→")
		switch job.TrendDirection {
		case "improving":
			trendIcon = successStyle.Render("✓")
		case "degrading":
			trendIcon = failureStyle.Render("⚠")
		}

		name := job.Name
		if runes := []rune(name); len(runes) > typicalNameWidth {
			name = string(runes[:typicalNameWidth-3]) + "..."
		}

		// Percentile values link to the real run nearest that percentile, so
		// aggregate statistics stay grounded in inspectable runs.
		p50Val := utils.HumanizeTime(job.Duration.P50)
		if job.P50URL != "" {
			p50Val = utils.MakeClickableLink(job.P50URL, p50Val)
		}
		p95Val := utils.HumanizeTime(job.Duration.P95)
		if job.P95URL != "" {
			p95Val = utils.MakeClickableLink(job.P95URL, p95Val)
		}
		detail := fmt.Sprintf("p50 %s  p95 %s", p50Val, p95Val)
		if job.PresenceRate < 99.5 {
			detail += fmt.Sprintf("  in %.0f%% of runs", job.PresenceRate)
		}
		if job.SuccessRate < 99.5 {
			detail += fmt.Sprintf("  %.0f%% pass", job.SuccessRate)
		}

		fmt.Fprintf(w, "  %-*s %s %s %s\n", typicalNameWidth, name, track, trendIcon, dimStyle.Render(detail))
	}

	if hidden := len(wf.Jobs) - len(jobs); hidden > 0 {
		fmt.Fprintf(w, "  %s\n", dimStyle.Render(fmt.Sprintf("... and %d more jobs", hidden)))
	}
}
