package analyzer

import (
	"math"
	"testing"
)

func baselineTypical() *TypicalRun {
	return &TypicalRun{
		SampledRuns: 100,
		Workflows: []TypicalWorkflow{{
			Name: "CI", SampledRuns: 100, TotalRuns: 100,
			Jobs: []TypicalJob{
				{Name: "build", Samples: 100, Duration: Quantiles{P50: 300, P95: 400}},
				{Name: "test", Samples: 100, Duration: Quantiles{P50: 600, P95: 900}},
			},
		}, {
			Name: "Docs", SampledRuns: 10, TotalRuns: 10,
			Jobs: []TypicalJob{
				// Same job name in another workflow, fewer samples: the
				// busier baseline must win the name lookup.
				{Name: "build", Samples: 10, Duration: Quantiles{P50: 60, P95: 90}},
			},
		}},
	}
}

func TestCompareJobsToTypical(t *testing.T) {
	t.Parallel()
	observed := []JobObservation{
		{Name: "build", DurationSec: 450}, // +50% over p50, above p95 (400)
		{Name: "test", DurationSec: 590},  // ~p50: not notable
		{Name: "brand-new-job", DurationSec: 120}, // no baseline
	}

	deltas := CompareJobsToTypical(observed, baselineTypical())

	if len(deltas) != 1 {
		t.Fatalf("len(deltas) = %d, want 1 (only notable deviations)", len(deltas))
	}
	d := deltas[0]
	if d.JobName != "build" || d.WorkflowName != "CI" {
		t.Errorf("delta identity = %s/%s, want CI/build (busier baseline wins)", d.WorkflowName, d.JobName)
	}
	if math.Abs(d.ActualSec-450) > 0.01 || math.Abs(d.TypicalP50-300) > 0.01 {
		t.Errorf("actual/typical = %v/%v, want 450/300", d.ActualSec, d.TypicalP50)
	}
	if math.Abs(d.DeltaPct-50) > 0.01 {
		t.Errorf("DeltaPct = %v, want 50", d.DeltaPct)
	}
	if !d.AboveP95 {
		t.Error("450s should be flagged above the 400s p95")
	}
}

func TestCompareJobsToTypicalFasterIsAlsoNotable(t *testing.T) {
	t.Parallel()
	observed := []JobObservation{{Name: "test", DurationSec: 200}} // -67%
	deltas := CompareJobsToTypical(observed, baselineTypical())
	if len(deltas) != 1 {
		t.Fatalf("len(deltas) = %d, want 1", len(deltas))
	}
	if deltas[0].DeltaPct > -50 {
		t.Errorf("DeltaPct = %v, want about -67", deltas[0].DeltaPct)
	}
	if deltas[0].AboveP95 {
		t.Error("a fast run is not above p95")
	}
}

func TestCompareJobsToTypicalNilBaseline(t *testing.T) {
	t.Parallel()
	if got := CompareJobsToTypical([]JobObservation{{Name: "x", DurationSec: 1}}, nil); got != nil {
		t.Errorf("nil baseline should yield nil, got %+v", got)
	}
}
