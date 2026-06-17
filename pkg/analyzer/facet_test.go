package analyzer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rowByKey(t *testing.T, c *FacetComparison, key string) FacetRow {
	t.Helper()
	for _, r := range c.Rows {
		if r.Key == key {
			return r
		}
	}
	t.Fatalf("facet row %q not found in %+v", key, c.Rows)
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

	c := computeFacets(runs, FacetBranch, "main")
	require.NotNil(t, c)
	assert.Equal(t, FacetBranch, c.Dimension)
	assert.Equal(t, "run", c.Level)
	require.Len(t, c.Rows, 2)

	up := rowByKey(t, c, "upstream")
	assert.Equal(t, 2, up.Count)
	assert.InDelta(t, 100.0, up.SuccessRatePct, 0.01)

	feat := rowByKey(t, c, "feature")
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
	c := computeFacets(runs, FacetEvent, "")
	require.NotNil(t, c)
	assert.Equal(t, "run", c.Level)
	assert.Equal(t, 2, rowByKey(t, c, "push").Count)
	assert.Equal(t, 1, rowByKey(t, c, "pull_request").Count)
	assert.Equal(t, 1, rowByKey(t, c, "unknown").Count) // empty event bucketed
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
	c := computeFacets(runs, FacetRunner, "")
	require.NotNil(t, c)
	assert.Equal(t, FacetRunner, c.Dimension)
	assert.Equal(t, "job", c.Level)

	x64 := rowByKey(t, c, "ubuntu-24.04")
	assert.Equal(t, 2, x64.Count)
	assert.InDelta(t, 50.0, x64.SuccessRatePct, 0.01) // 1 of 2 decisive
	assert.InDelta(t, 75.0, x64.AvgDurationSec, 0.01) // (60+90)/2
	assert.InDelta(t, 7.0, x64.AvgQueueSec, 0.01)     // (5+9)/2

	arm := rowByKey(t, c, "ubuntu-24.04-arm")
	assert.Equal(t, 1, arm.Count)
	// job with no labels falls back to its runner name as the key
	assert.Equal(t, 1, rowByKey(t, c, "Hosted 9").Count)
}

func TestComputeFacetsUnknownDimensionIsNil(t *testing.T) {
	t.Parallel()
	assert.Nil(t, computeFacets([]RunData{{Branch: "main"}}, FacetDimension("nope"), ""))
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
