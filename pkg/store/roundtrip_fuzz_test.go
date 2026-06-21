package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

// FuzzStoreRoundTrip checks SQLite persistence fidelity: a run (with one job)
// written via UpsertRuns and read back via LoadRuns must reappear with its
// fields intact. The store serializes times to milliseconds, normalizes a zero
// run-attempt to 1, and JSON-encodes runner labels — this pins those exact
// semantics and would catch a dropped column, truncated value, or label-codec
// bug. Each exec uses a fresh temp DB (modernc :memory: opens a *separate* DB
// per pooled connection, so it can't be used for a write-then-read round-trip).
func FuzzStoreRoundTrip(f *testing.F) {
	f.Add(int64(42), "CI", "main", "push", "success", "completed", "deadbeef", int64(1500), int64(1_700_000_000_000), "build", "ubuntu-latest", "ubuntu-24.04,self-hosted")
	f.Add(int64(0), "", "", "", "", "", "", int64(0), int64(1), "", "", "")

	clampMs := func(ms int64) int64 {
		if ms < 1 {
			return 1
		}
		if ms > 1<<50 {
			return 1 << 50
		}
		return ms
	}

	f.Fuzz(func(t *testing.T,
		id int64, wf, branch, event, conclusion, status, sha string,
		durMs, createdMs int64, jobName, runner, labelsCSV string,
	) {
		// Domain constraint: every string the store persists originates from
		// GitHub's JSON API, which is always valid UTF-8. Labels in particular
		// round-trip through json.Marshal (encodeLabels), which lossily replaces
		// invalid UTF-8 with U+FFFD — a non-issue for real data. Skip inputs
		// outside the real domain so the fidelity property reflects reality.
		for _, s := range []string{wf, branch, event, conclusion, status, sha, jobName, runner, labelsCSV} {
			if !utf8.ValidString(s) {
				return
			}
		}
		cms := clampMs(createdMs)
		ts := time.UnixMilli(cms).UTC()
		var labels []string
		if labelsCSV != "" {
			labels = strings.Split(labelsCSV, ",")
		}
		run := analyzer.RunData{
			ID: id, WorkflowName: wf, HeadSHA: sha, Branch: branch, Event: event,
			Status: status, Conclusion: conclusion,
			CreatedAt: ts, StartedAt: ts, UpdatedAt: ts, Duration: durMs,
			Jobs: []analyzer.JobData{{
				ID: id ^ 0x5a5a, Name: jobName, Status: "completed", Conclusion: "success",
				CreatedAt: ts, StartedAt: ts, CompletedAt: ts, Duration: durMs,
				RunnerName: runner, Labels: labels,
			}},
		}

		dir, err := os.MkdirTemp("", "store-fuzz")
		if err != nil {
			t.Skip()
		}
		defer os.RemoveAll(dir)
		st, err := Open(filepath.Join(dir, "s.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer st.Close()

		if err := st.UpsertRuns("o", "r", []analyzer.RunData{run}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		got, err := st.LoadRuns("o", "r", time.UnixMilli(1), time.UnixMilli(1<<50))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("round-trip returned %d runs, want 1 (created=%d)", len(got), cms)
		}
		g := got[0]
		wantAttempt := int64(1) // store normalizes 0 -> 1
		if g.ID != run.ID || g.WorkflowName != wf || g.HeadSHA != sha || g.Branch != branch ||
			g.Event != event || g.Status != status || g.Conclusion != conclusion ||
			g.Duration != durMs || g.Attempt != wantAttempt {
			t.Fatalf("run field drift:\n in: %+v\nout: %+v", run, g)
		}
		if g.CreatedAt.UnixMilli() != cms || g.StartedAt.UnixMilli() != cms || g.UpdatedAt.UnixMilli() != cms {
			t.Fatalf("run time drift: want %d got created=%d started=%d updated=%d",
				cms, g.CreatedAt.UnixMilli(), g.StartedAt.UnixMilli(), g.UpdatedAt.UnixMilli())
		}
		if len(g.Jobs) != 1 {
			t.Fatalf("expected 1 job round-tripped, got %d", len(g.Jobs))
		}
		j := g.Jobs[0]
		if j.Name != jobName || j.RunnerName != runner || j.Duration != durMs {
			t.Fatalf("job field drift:\n in: %+v\nout: %+v", run.Jobs[0], j)
		}
		if strings.Join(j.Labels, ",") != strings.Join(labels, ",") {
			t.Fatalf("label drift: want %q got %q", labels, j.Labels)
		}
	})
}
