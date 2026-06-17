package analyzer

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
)

// TestFacetAccuracyAgainstDump measures how close the production-sampled
// --facet=all comparison table is to full-scan ground truth. It is gated on a
// dump captured with `ote trends <repo> --no-sample --dump-runs=FILE`; set
// OTE_FACET_DUMP to that file to run it. Without the env var it skips, so it
// never breaks CI (no network, no fixture committed) yet stays runnable on
// demand the same way cmd/sample-eval consumes dumps.
//
// Ground truth = computeFacets over every completed run's jobs. The sampled
// table = computeFacets over the same runs after stripping job detail from the
// runs the production sampler (SelectSampleIndices) did not pick — exactly what
// AnalyzeTrends produces, since unsampled runs never get a jobs fetch.
func TestFacetAccuracyAgainstDump(t *testing.T) {
	path := os.Getenv("OTE_FACET_DUMP")
	if path == "" {
		t.Skip("set OTE_FACET_DUMP=<dump.json> (from `ote trends --no-sample --dump-runs=`) to run")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	var dump RunDump
	if err := json.Unmarshal(data, &dump); err != nil {
		t.Fatalf("parse dump: %v", err)
	}

	// Keep completed runs only — the same filter AnalyzeTrends applies.
	var completed []RunData
	for _, r := range dump.Runs {
		if r.Status == "completed" {
			completed = append(completed, r)
		}
	}
	if len(completed) == 0 {
		t.Fatalf("no completed runs in dump %s", path)
	}

	defaultBranch := os.Getenv("OTE_FACET_DEFAULT_BRANCH")
	if defaultBranch == "" {
		defaultBranch = "main"
	}
	dims := []FacetDimension{FacetBranch, FacetEvent, FacetRunner}

	// --- ground truth: every run's jobs were fetched (no extrapolation) ---
	for i := range completed {
		completed[i].JobsFetched = true
	}
	truth := computeFacets(completed, dims, defaultBranch)

	// --- production sample: strip jobs from unsampled runs ---
	wfRuns := make([]githubapi.WorkflowRun, len(completed))
	for i, r := range completed {
		wfRuns[i] = githubapi.WorkflowRun{
			ID:        r.ID,
			Name:      r.WorkflowName,
			Status:    r.Status,
			CreatedAt: r.CreatedAt.Format(time.RFC3339),
			UpdatedAt: r.UpdatedAt.Format(time.RFC3339),
		}
	}
	major, minor := JobSampleTargets(0.10)
	sampleIdx := SelectSampleIndices(wfRuns, major, minor)
	sampled := make([]RunData, len(completed))
	keep := make(map[int]bool, len(sampleIdx))
	for _, i := range sampleIdx {
		keep[i] = true
	}
	for i, r := range completed {
		sampled[i] = r
		if !keep[i] {
			// Unsampled runs have no job detail and were never fetched.
			sampled[i].Jobs = nil
			sampled[i].JobsFetched = false
		}
	}
	sampledFacets := computeFacets(sampled, dims, defaultBranch)
	sampledByKey := indexFacet(sampledFacets)

	// Score buckets that are material in truth: enough jobs to have a stable
	// rate, and big enough to actually appear in the capped 25-row table.
	const minObs = 30
	var succAbsErr, durRelErr, medRelErr, countRelErr []float64
	missing := 0
	var scored int
	var lines []string
	for _, tr := range truth.Rows {
		if tr.Count < minObs {
			continue
		}
		key := strings.Join(tr.Keys, " / ")
		sr, ok := sampledByKey[strings.Join(tr.Keys, "\x00")]
		if !ok {
			missing++
			lines = append(lines, fmt.Sprintf("  MISSING  %-52s truth n=%d succ=%.1f%%", key, tr.Count, tr.SuccessRatePct))
			continue
		}
		scored++
		se := math.Abs(sr.SuccessRatePct - tr.SuccessRatePct)
		succAbsErr = append(succAbsErr, se)
		if tr.Count > 0 {
			countRelErr = append(countRelErr, math.Abs(float64(sr.Count-tr.Count))/float64(tr.Count)*100)
		}
		var de, me float64
		if tr.AvgDurationSec > 0 {
			de = math.Abs(sr.AvgDurationSec-tr.AvgDurationSec) / tr.AvgDurationSec * 100
			durRelErr = append(durRelErr, de)
		}
		if tr.MedianDurationSec > 0 {
			me = math.Abs(sr.MedianDurationSec-tr.MedianDurationSec) / tr.MedianDurationSec * 100
			medRelErr = append(medRelErr, me)
		}
		lines = append(lines, fmt.Sprintf("  %-52s truthN=%-5d sampN=%-5d succ Δ%4.1fpp (%.1f→%.1f) avgdur Δ%5.1f%% meddur Δ%5.1f%%",
			key, tr.Count, sr.Count, se, tr.SuccessRatePct, sr.SuccessRatePct, de, me))
	}

	t.Logf("\n=== %s/%s facet=all accuracy: %d/%d completed runs sampled, %d buckets scored (≥%d truth jobs), %d missing ===",
		dump.Owner, dump.Repo, len(sampleIdx), len(completed), scored, minObs, missing)
	sort.Strings(lines)
	for _, l := range lines {
		t.Log(l)
	}
	t.Logf("job-count rel error (%%):       p50=%.1f p90=%.1f max=%.1f", q(countRelErr, 50), q(countRelErr, 90), q(countRelErr, 100))
	t.Logf("success-rate abs error (pp):   p50=%.1f p90=%.1f max=%.1f", q(succAbsErr, 50), q(succAbsErr, 90), q(succAbsErr, 100))
	t.Logf("avg-duration rel error (%%):    p50=%.1f p90=%.1f max=%.1f", q(durRelErr, 50), q(durRelErr, 90), q(durRelErr, 100))
	t.Logf("median-duration rel error (%%): p50=%.1f p90=%.1f max=%.1f", q(medRelErr, 50), q(medRelErr, 90), q(medRelErr, 100))

	// Accuracy bars for material buckets. These are claims about reality, not
	// the textbook margin; loosen/tighten as the sampler evolves.
	if missing > 0 {
		t.Errorf("%d material truth buckets (≥%d jobs) absent from the sampled facet table", missing, minObs)
	}
	if p := q(succAbsErr, 90); p > 10 {
		t.Errorf("success-rate p90 abs error %.1fpp exceeds 10pp", p)
	}
	// Population job counts are extrapolated per workflow; they should track
	// reality closely (this is the bias the extrapolation corrects).
	if p := q(countRelErr, 90); p > 20 {
		t.Errorf("job-count p90 rel error %.1f%% exceeds 20%%", p)
	}
	if p := q(medRelErr, 90); p > 35 {
		t.Errorf("median-duration p90 rel error %.1f%% exceeds 35%%", p)
	}
}

func indexFacet(c *FacetComparison) map[string]FacetRow {
	m := make(map[string]FacetRow, len(c.Rows))
	for _, r := range c.Rows {
		m[strings.Join(r.Keys, "\x00")] = r
	}
	return m
}

func q(xs []float64, p int) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	idx := int(math.Ceil(float64(len(s))*float64(p)/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
