package output

import (
	"strings"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

func TestRenderHourlyPatternsNoQueueData(t *testing.T) {
	// PeakQueueHour is -1 when no hour has enough queue observations;
	// rendering must not panic and must omit the queue callout.
	hp := &analyzer.HourlyPatterns{PeakQueueHour: -1, PeakVolumeHour: 3}
	hp.Hours[3].RunCount = 12

	var sb strings.Builder
	renderHourlyPatterns(&sb, hp)

	out := sb.String()
	if !strings.Contains(out, "Busiest hour 03:00") {
		t.Errorf("missing busiest-hour line in %q", out)
	}
	if strings.Contains(out, "Worst queue") {
		t.Errorf("queue callout should be omitted without queue data: %q", out)
	}
}
