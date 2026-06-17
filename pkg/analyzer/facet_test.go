package analyzer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rowByKeys(t *testing.T, c *FacetComparison, keys ...string) FacetRow {
	t.Helper()
	for _, r := range c.Rows {
		if len(r.Keys) != len(keys) {
			continue
		}
		match := true
		for i := range keys {
			if r.Keys[i] != keys[i] {
				match = false
				break
			}
		}
		if match {
			return r
		}
	}
	t.Fatalf("facet row %v not found in %+v", keys, c.Rows)
	return FacetRow{}
}

func TestComputeFacetsBranchBucketsUpstreamVsFeature(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	mk := func(branch, conclusion string, durMs int64) RunData {
		return RunData{Branch: branch, Conclusion: conclusion, Duration: durMs, CreatedAt: base}
	}
	runs := []RunData{
		mk("main", "success", 600_000),
		mk("v20.x", "success", 620_000),  // release → upstream
		mk("fix/a", "failure", 300_000),  // feature
		mk("dependabot/x", "failure", 0), // feature, no duration
		mk("topic/b", "success", 360_000),
	}

	c := computeFacets(runs, []FacetDimension{FacetBranch}, "main")
	require.NotNil(t, c)
	assert.Equal(t, []FacetDimension{FacetBranch}, c.Dimensions)
	assert.Equal(t, "run", c.Level)
	require.Len(t, c.Rows, 2)

	up := rowByKeys(t, c, "upstream")
	assert.Equal(t, 2, up.Count)
	assert.InDelta(t, 100.0, up.SuccessRatePct, 0.01)

	feat := rowByKeys(t, c, "feature")
	assert.Equal(t, 3, feat.Count)
	assert.InDelta(t, 100.0/3.0, feat.SuccessRatePct, 0.5) // 1 of 3 success
}

func TestComputeFacetsByEvent(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	runs := []RunData{
		{Event: "push", Conclusion: "success", Duration: 100_000, CreatedAt: base},
		{Event: "push", Conclusion: "success", Duration: 200_000, CreatedAt: base},
		{Event: "pull_request", Conclusion: "failure", Duration: 50_000, CreatedAt: base},
		{Event: "", Conclusion: "success", Duration: 10_000, CreatedAt: base},
	}
	c := computeFacets(runs, []FacetDimension{FacetEvent}, "")
	require.NotNil(t, c)
	assert.Equal(t, "run", c.Level)
	assert.Equal(t, 2, rowByKeys(t, c, "push").Count)
	assert.Equal(t, 1, rowByKeys(t, c, "pull_request").Count)
	assert.Equal(t, 1, rowByKeys(t, c, "unknown").Count) // empty event bucketed
}

func TestComputeFacetsByRunnerIsJobLevel(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	runs := []RunData{{
		Branch: "main", CreatedAt: base,
		Jobs: []JobData{
			{Conclusion: "success", Duration: 60_000, QueueTime: 5_000, Labels: []string{"ubuntu-24.04"}},
			{Conclusion: "failure", Duration: 90_000, QueueTime: 9_000, Labels: []string{"ubuntu-24.04"}},
			{Conclusion: "success", Duration: 120_000, QueueTime: 1_000, Labels: []string{"ubuntu-24.04-arm"}},
			{Conclusion: "success", Duration: 30_000, QueueTime: 2_000, RunnerName: "Hosted 9"}, // no labels
		},
	}}
	c := computeFacets(runs, []FacetDimension{FacetRunner}, "")
	require.NotNil(t, c)
	assert.Equal(t, []FacetDimension{FacetRunner}, c.Dimensions)
	assert.Equal(t, "job", c.Level)

	x64 := rowByKeys(t, c, "ubuntu-24.04")
	assert.Equal(t, 2, x64.Count)
	assert.InDelta(t, 50.0, x64.SuccessRatePct, 0.01) // 1 of 2 decisive
	assert.InDelta(t, 75.0, x64.AvgDurationSec, 0.01) // (60+90)/2
	assert.InDelta(t, 7.0, x64.AvgQueueSec, 0.01)     // (5+9)/2

	assert.Equal(t, 1, rowByKeys(t, c, "ubuntu-24.04-arm").Count)
	// job with no labels falls back to its runner name as the key
	assert.Equal(t, 1, rowByKeys(t, c, "Hosted 9").Count)
}

func TestComputeFacetsCrossBranchEventIsRunLevel(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	runs := []RunData{
		{Branch: "main", Event: "push", Conclusion: "success", Duration: 100_000, CreatedAt: base},
		{Branch: "main", Event: "push", Conclusion: "failure", Duration: 100_000, CreatedAt: base},
		{Branch: "main", Event: "schedule", Conclusion: "success", Duration: 50_000, CreatedAt: base},
		{Branch: "fix/x", Event: "pull_request", Conclusion: "success", Duration: 30_000, CreatedAt: base},
	}
	c := computeFacets(runs, []FacetDimension{FacetBranch, FacetEvent}, "main")
	require.NotNil(t, c)
	assert.Equal(t, []FacetDimension{FacetBranch, FacetEvent}, c.Dimensions)
	assert.Equal(t, "run", c.Level)
	require.Len(t, c.Rows, 3)

	assert.Equal(t, 2, rowByKeys(t, c, "upstream", "push").Count)
	assert.InDelta(t, 50.0, rowByKeys(t, c, "upstream", "push").SuccessRatePct, 0.01)
	assert.Equal(t, 1, rowByKeys(t, c, "upstream", "schedule").Count)
	assert.Equal(t, 1, rowByKeys(t, c, "feature", "pull_request").Count)
}

func TestComputeFacetsCrossWithRunnerIsJobLevel(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	runs := []RunData{
		{Branch: "main", Event: "push", CreatedAt: base, Jobs: []JobData{
			{Conclusion: "success", Duration: 60_000, Labels: []string{"ubuntu-24.04"}},
			{Conclusion: "failure", Duration: 60_000, Labels: []string{"ubuntu-24.04"}},
		}},
		{Branch: "fix/y", Event: "pull_request", CreatedAt: base, Jobs: []JobData{
			{Conclusion: "success", Duration: 120_000, Labels: []string{"ubuntu-24.04"}},
		}},
	}
	c := computeFacets(runs, []FacetDimension{FacetBranch, FacetRunner}, "main")
	require.NotNil(t, c)
	assert.Equal(t, "job", c.Level)
	// Same runner label split across branch buckets via each job's parent run.
	assert.Equal(t, 2, rowByKeys(t, c, "upstream", "ubuntu-24.04").Count)
	assert.Equal(t, 1, rowByKeys(t, c, "feature", "ubuntu-24.04").Count)
}

// TestFacetJobCountsExtrapolateForSampling pins the fix for the headline
// accuracy bug: when only some runs of a workflow have job detail (sampling),
// the job-level facet Count must estimate the *population* count, not the raw
// sample count. Two workflows sampled at different rates would otherwise mis-
// rank: a busy workflow sampled sparsely looks smaller than reality.
func TestFacetJobCountsExtrapolateForSampling(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	var runs []RunData
	// Workflow "big": 10 completed runs, only 2 sampled (have jobs). Each
	// sampled run has one ubuntu-slim job → population ≈ 10 jobs.
	for i := 0; i < 10; i++ {
		r := RunData{WorkflowName: "big", Branch: "main", Event: "push", CreatedAt: base}
		if i < 2 {
			r.JobsFetched = true
			r.Jobs = []JobData{{Conclusion: "success", Duration: 60_000, Labels: []string{"ubuntu-slim"}}}
		}
		runs = append(runs, r)
	}
	// Workflow "small": 2 completed runs, both sampled. One ubuntu-slim job
	// each → population = 2 jobs, all observed.
	for i := 0; i < 2; i++ {
		runs = append(runs, RunData{WorkflowName: "small", Branch: "main", Event: "push", CreatedAt: base,
			JobsFetched: true,
			Jobs:        []JobData{{Conclusion: "success", Duration: 60_000, Labels: []string{"ubuntu-slim"}}}})
	}

	c := computeFacets(runs, []FacetDimension{FacetRunner}, "main")
	require.NotNil(t, c)
	row := rowByKeys(t, c, "ubuntu-slim")
	// Raw sample = 4 jobs; extrapolated population = big(2×10/2=10) + small(2) = 12.
	assert.Equal(t, 12, row.Count, "job count should extrapolate to population, not report raw sample size")
	// Per-job stats stay sample-based (validated accurate by cmd/sample-eval).
	assert.InDelta(t, 100.0, row.SuccessRatePct, 0.01)
	assert.InDelta(t, 60.0, row.AvgDurationSec, 0.01)
}

func TestComputeFacetsTruncatesBusiestFirst(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	var runs []RunData
	for i := 0; i < 30; i++ {
		runs = append(runs, RunData{Event: fmt.Sprintf("evt%02d", i), Conclusion: "success", Duration: 1000, CreatedAt: base})
	}
	c := computeFacets(runs, []FacetDimension{FacetEvent}, "")
	require.NotNil(t, c)
	assert.Len(t, c.Rows, maxFacetRows)
	assert.Equal(t, 30-maxFacetRows, c.Truncated)
}

func TestComputeFacetsEmptyOrUnknownIsNil(t *testing.T) {
	t.Parallel()
	assert.Nil(t, computeFacets([]RunData{{Branch: "main"}}, nil, ""))
	assert.Nil(t, computeFacets([]RunData{{Branch: "main"}}, []FacetDimension{FacetDimension("nope")}, ""))
	assert.Nil(t, computeFacets([]RunData{{Branch: "main"}}, []FacetDimension{FacetBranch, FacetDimension("nope")}, ""))
}

func TestParseFacets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    []FacetDimension
		wantErr bool
	}{
		{in: "", want: nil},
		{in: "branch", want: []FacetDimension{FacetBranch}},
		{in: "branch,event", want: []FacetDimension{FacetBranch, FacetEvent}},
		{in: " branch , runner ", want: []FacetDimension{FacetBranch, FacetRunner}},
		{in: "branch,branch", want: []FacetDimension{FacetBranch}}, // de-duped
		{in: "all", want: []FacetDimension{FacetBranch, FacetEvent, FacetRunner}},
		{in: "bogus", wantErr: true},
		{in: "branch,bogus", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseFacets(tc.in)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestClassifyBranch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		branch        string
		defaultBranch string
		want          string
	}{
		{"default branch is upstream", "main", "main", "upstream"},
		{"master default", "master", "master", "upstream"},
		{"main is upstream even when default unknown", "main", "", "upstream"},
		{"master is upstream even when default unknown", "master", "", "upstream"},
		{"node-style release line", "v20.x", "main", "upstream"},
		{"dotted version", "v18.17.1", "main", "upstream"},
		{"release/ prefix", "release/1.2", "main", "upstream"},
		{"rails-style stable line", "7-1-stable", "main", "upstream"},
		{"feature branch", "fix/clamp-times", "main", "feature"},
		{"dependabot branch", "dependabot/npm/foo-1.2.3", "main", "feature"},
		{"user fork topic", "stefanpenner/add-faceting", "main", "feature"},
		{"empty branch is feature", "", "main", "feature"},
		{"main stays upstream even if default differs", "main", "develop", "upstream"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyBranch(tc.branch, tc.defaultBranch); got != tc.want {
				t.Errorf("classifyBranch(%q, %q) = %q, want %q", tc.branch, tc.defaultBranch, got, tc.want)
			}
		})
	}
}
