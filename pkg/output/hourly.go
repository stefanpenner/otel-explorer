package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

var sparkGlyphs = []rune("▁▂▃▄▅▆▇█")

// sparkline renders values as one glyph per element, scaled to the max.
func sparkline(values [24]float64) string {
	max := 0.0
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	var sb strings.Builder
	for _, v := range values {
		if v <= 0 || max <= 0 {
			sb.WriteRune(' ')
			continue
		}
		idx := int(v / max * float64(len(sparkGlyphs)-1))
		sb.WriteRune(sparkGlyphs[idx])
	}
	return sb.String()
}

// renderHourlyPatterns draws run volume and queue-time p50 by UTC hour.
// Queue peaks that track volume peaks indicate runner contention; the
// remedy is capacity, not code.
func renderHourlyPatterns(w io.Writer, hp *analyzer.HourlyPatterns) {
	var volume, queue [24]float64
	for h, b := range hp.Hours {
		volume[h] = float64(b.RunCount)
		queue[h] = b.QueueP50
	}

	fmt.Fprintln(w)
	fmt.Fprintf(w, "         0   4   8   12  16  20      (UTC)\n")
	fmt.Fprintf(w, "  Runs   %s\n", sparkline(volume))
	fmt.Fprintf(w, "  Queue  %s\n", sparkline(queue))

	peakV := hp.Hours[hp.PeakVolumeHour]
	peakQ := hp.Hours[hp.PeakQueueHour]
	fmt.Fprintf(w, "\n  %s\n", dimStyle.Render(fmt.Sprintf(
		"Busiest hour %02d:00 (%d runs). Worst queue %02d:00 (p50 %s).",
		hp.PeakVolumeHour, peakV.RunCount, hp.PeakQueueHour, utils.HumanizeTime(peakQ.QueueP50))))
	if peakQ.QueueP50 > 0 && hp.PeakQueueHour == hp.PeakVolumeHour {
		fmt.Fprintf(w, "  %s\n", dimStyle.Render("Queue peaks with volume — runner contention; capacity, not code, is the fix."))
	}
}
