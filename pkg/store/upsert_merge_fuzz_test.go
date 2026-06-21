package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

// FuzzStoreUpsertMerge exercises the re-sync merge contract that prevents
// duplicate jobs: UpsertRuns replaces a run's jobs wholesale when the new run
// carries job detail, but preserves the existing jobs when the new run has
// none (a run-listing-only refresh). The property:
//   - upsert(run, jobsA) then upsert(run, jobsB), jobsB non-empty  => exactly jobsB
//   - upsert(run, jobsA) then upsert(run, no jobs)                 => still jobsA
//
// In neither case may jobs duplicate or be partially merged. A regression here
// (append instead of replace, or wiping on an empty refresh) silently corrupts
// every incremental `ote sync`.
func FuzzStoreUpsertMerge(f *testing.F) {
	f.Add(int64(7), 3, 2, int64(1000), int64(2000))
	f.Add(int64(7), 4, 0, int64(1000), int64(0))
	f.Add(int64(7), 0, 3, int64(1000), int64(1000)) // overlapping ID ranges

	clamp := func(n, lo, hi int) int {
		if n < lo {
			return lo
		}
		if n > hi {
			return hi
		}
		return n
	}
	ts := time.UnixMilli(1_700_000_000_000).UTC()
	mkRun := func(id int64, baseJobID int64, n int) analyzer.RunData {
		r := analyzer.RunData{ID: id, WorkflowName: "CI", Status: "completed",
			CreatedAt: ts, StartedAt: ts, UpdatedAt: ts}
		for i := 0; i < n; i++ {
			r.Jobs = append(r.Jobs, analyzer.JobData{
				ID: baseJobID + int64(i), Name: "j", Status: "completed", Conclusion: "success",
				CreatedAt: ts, StartedAt: ts, CompletedAt: ts,
			})
		}
		return r
	}

	f.Fuzz(func(t *testing.T, runID int64, nA, nB int, baseA, baseB int64) {
		nA, nB = clamp(nA, 0, 30), clamp(nB, 0, 30)

		dir, err := os.MkdirTemp("", "store-merge")
		if err != nil {
			t.Skip()
		}
		defer os.RemoveAll(dir)
		st, err := Open(filepath.Join(dir, "s.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer st.Close()

		if err := st.UpsertRuns("o", "r", []analyzer.RunData{mkRun(runID, baseA, nA)}); err != nil {
			t.Fatalf("upsert A: %v", err)
		}
		if err := st.UpsertRuns("o", "r", []analyzer.RunData{mkRun(runID, baseB, nB)}); err != nil {
			t.Fatalf("upsert B: %v", err)
		}
		got, err := st.LoadRuns("o", "r", time.UnixMilli(1), time.UnixMilli(1<<50))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("want 1 run, got %d", len(got))
		}

		// Expected job-ID set: jobsB when B carried jobs, else jobsA (preserved).
		want := map[int64]bool{}
		if nB > 0 {
			for i := 0; i < nB; i++ {
				want[baseB+int64(i)] = true
			}
		} else {
			for i := 0; i < nA; i++ {
				want[baseA+int64(i)] = true
			}
		}
		gotIDs := map[int64]int{}
		for _, j := range got[0].Jobs {
			gotIDs[j.ID]++
		}
		if len(got[0].Jobs) != len(want) {
			t.Fatalf("job count: got %d want %d (nA=%d nB=%d baseA=%d baseB=%d) — duplication or loss",
				len(got[0].Jobs), len(want), nA, nB, baseA, baseB)
		}
		for id, c := range gotIDs {
			if c != 1 {
				t.Fatalf("job id %d appears %d times — duplicated", id, c)
			}
			if !want[id] {
				t.Fatalf("unexpected job id %d in result (nA=%d nB=%d)", id, nA, nB)
			}
		}
	})
}
