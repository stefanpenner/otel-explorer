package analyzer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// obsFromDurations builds chronological observations, one per index, each with
// its own commit SHA so the localization window maps to distinct commits.
func obsFromDurations(durs []float64) []jobObservation {
	base := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	obs := make([]jobObservation, len(durs))
	for i, d := range durs {
		obs[i] = jobObservation{
			DurationSec:  d,
			RunCreatedAt: base.Add(time.Duration(i) * time.Hour),
			HeadSHA:      fmt.Sprintf("sha%02d", i),
			JobURL:       fmt.Sprintf("https://job/%d", i),
		}
	}
	return obs
}

func TestDetectChangepoint_SharpStepIsTightAndSignificant(t *testing.T) {
	// A clean doubling halfway through: ~10s → ~20s.
	durs := []float64{10, 10, 11, 10, 9, 20, 21, 19, 20, 20}
	cp := detectChangepoint(obsFromDurations(durs), 3, 1)

	require.NotNil(t, cp, "a clean step must be detected")
	assert.Equal(t, 5, cp.Index, "split at the true boundary (index 5)")
	assert.Equal(t, "sha04", cp.BeforeSHA)
	assert.Equal(t, "sha05", cp.AfterSHA)
	assert.Less(t, cp.PValue, 0.05, "shift is significant")
	assert.Contains(t, []string{"medium", "high", "very high"}, cp.Confidence)

	// Localization is tight: the window is a small number of commits and the
	// best split sits inside it.
	assert.LessOrEqual(t, cp.RangeCommits, 3)
	assert.GreaterOrEqual(t, cp.RangeCommits, 1)
}

func TestDetectChangepoint_PerfectStepNarrowsToOneCommit(t *testing.T) {
	// No noise at all → the interval collapses to the adjacent boundary.
	durs := []float64{10, 10, 10, 10, 20, 20, 20, 20}
	cp := detectChangepoint(obsFromDurations(durs), 3, 1)

	require.NotNil(t, cp)
	assert.Equal(t, 4, cp.Index)
	assert.Equal(t, 1, cp.RangeCommits, "perfect step → exactly one candidate commit")
	assert.Equal(t, "sha03", cp.RangeStartSHA, "last known-good")
	assert.Equal(t, "sha04", cp.RangeEndSHA, "first confirmed-shifted")
}

func TestDetectChangepoint_NoRealChangeReturnsNil(t *testing.T) {
	// Two halves drawn from the same distribution: no changepoint.
	durs := []float64{10, 12, 10, 12, 10, 12, 10, 12, 10, 12}
	cp := detectChangepoint(obsFromDurations(durs), 3, 1)
	assert.Nil(t, cp, "noise without a regime shift must not be reported")
}

func TestDetectChangepoint_SingleOutlierIsNotAChangepoint(t *testing.T) {
	// One spike among steady runs is not a regime change.
	durs := []float64{10, 10, 10, 10, 10, 10, 10, 1000}
	cp := detectChangepoint(obsFromDurations(durs), 3, 1)
	assert.Nil(t, cp, "a lone outlier must not be flagged as a shift")
}

func TestDetectChangepoint_WideWindowWhenLocationIsUncertain(t *testing.T) {
	// A real but gradual ramp (10s climbing to 20s over several commits):
	// significant overall, but no single commit owns the shift, so the window
	// must span more than one commit rather than fingering one falsely.
	durs := []float64{10, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 20, 20, 20, 20}
	cp := detectChangepoint(obsFromDurations(durs), 3, 1)
	require.NotNil(t, cp)
	assert.Less(t, cp.PValue, 0.05)
	assert.Greater(t, cp.RangeCommits, 1, "uncertain boundary → wider commit window")
	assert.NotEqual(t, cp.RangeStartSHA, cp.RangeEndSHA)
}

func TestDetectChangepoint_DirectionFilter(t *testing.T) {
	// A clean speed-up: 20s → 10s (got faster).
	durs := []float64{20, 20, 21, 19, 20, 10, 11, 9, 10, 10}

	// Asking for an increase (regression) must NOT match a decrease — this is
	// what stops a changepoint from contradicting the change it explains.
	assert.Nil(t, detectChangepoint(obsFromDurations(durs), 3, 1),
		"a downward shift must not be reported as a regression changepoint")

	// Asking for a decrease (improvement) finds it.
	cp := detectChangepoint(obsFromDurations(durs), 3, -1)
	require.NotNil(t, cp)
	assert.Equal(t, 5, cp.Index)
	assert.Greater(t, cp.BeforeAvg, cp.AfterAvg, "before slower than after (a speed-up)")
}

func TestMannWhitneyP(t *testing.T) {
	// Cleanly separated samples → tiny p.
	a := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	b := []float64{20, 21, 22, 23, 24, 25, 26, 27}
	assert.Less(t, mannWhitneyP(a, b), 0.001)

	// Identical samples → no evidence of difference.
	same := []float64{5, 6, 7, 8}
	assert.Equal(t, 1.0, mannWhitneyP(same, append([]float64{}, same...)))

	// All values tied → variance 0 → p = 1 (guard against divide-by-zero).
	assert.Equal(t, 1.0, mannWhitneyP([]float64{3, 3, 3}, []float64{3, 3, 3}))

	// Overlapping-but-shifted → between the extremes.
	p := mannWhitneyP([]float64{1, 2, 3, 4}, []float64{3, 4, 5, 6})
	assert.Greater(t, p, 0.0)
	assert.Less(t, p, 1.0)
}

func TestLocateChangepoint_InsufficientData(t *testing.T) {
	_, _, _, _, ok := locateChangepoint([]float64{1, 2, 3}, 3, 0)
	assert.False(t, ok, "need at least 2*minSideSize points")
}
