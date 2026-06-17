package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

const maxRunDeltas = 10

// RenderRunVsTypical prints how this run's jobs deviated from the
// repository's typical baseline (built from the local store). Answers "why
// was this run slow?" with per-job deltas instead of a feeling.
func RenderRunVsTypical(w io.Writer, deltas []analyzer.RunDelta, baselineRuns int) {
	if len(deltas) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s %s\n", titleStyle.Render("vs Typical"),
		dimStyle.Render(fmt.Sprintf("(baseline: %d stored runs; deviations ≥25%% from p50)", baselineRuns)))
	fmt.Fprintln(w)

	shown := deltas
	if len(shown) > maxRunDeltas {
		shown = shown[:maxRunDeltas]
	}
	for _, d := range shown {
		sign := "+"
		style := failureStyle
		if d.DeltaSec < 0 {
			sign = "-"
			style = successStyle
		}
		flag := ""
		if d.AboveP95 {
			flag = "  " + failureStyle.Render("▲ above p95")
		}
		name := d.JobName
		if runes := []rune(name); len(runes) > 34 {
			name = string(runes[:31]) + "..."
		}
		fmt.Fprintf(w, "  %-34s %10s  vs p50 %-8s %s%s\n",
			name,
			utils.HumanizeTime(d.ActualSec),
			utils.HumanizeTime(d.TypicalP50),
			style.Render(fmt.Sprintf("%s%.0f%%", sign, absFloat(d.DeltaPct))),
			flag)
	}
	if hidden := len(deltas) - len(shown); hidden > 0 {
		fmt.Fprintf(w, "  %s\n", dimStyle.Render(fmt.Sprintf("... and %d more deviations", hidden)))
	}
	fmt.Fprintln(w, strings.Repeat(" ", 0))
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
