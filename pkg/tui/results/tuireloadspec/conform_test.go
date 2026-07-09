package tuireloadspec

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestSupersessionSequence drives the correct reload→stale-fetch-discard
// path via Trace() and checks post-states match the decision core.
//
// Sequence:
//  1. PressFetch1  — start fetch job 1 at gen 0
//  2. PressReload  — isLoading
//  3. ReloadDone   — reloadGen = 1; fetch still in flight (fetchGen=0)
//  4. FetchDiscard — gen mismatch → clear fetchJob, never accept
//
// Maps to model.go:365-372: msg.gen != m.reloadGen → discard.
func TestSupersessionSequence(t *testing.T) {
	s := Init()
	if s.IsLoading || s.ReloadGen != 0 || s.FetchJob != 0 || s.StaleAccepted {
		t.Fatalf("Init = %+v", s)
	}

	actions := []string{"PressFetch1", "PressReload", "ReloadDone", "FetchDiscard"}
	var entries []TraceEntry
	for _, a := range actions {
		if !contains(s.EnabledActions(), a) {
			t.Fatalf("action %q not enabled in state %+v (enabled=%v)", a, s, s.EnabledActions())
		}
		var e TraceEntry
		e, s = s.Trace(a)
		entries = append(entries, e)
	}

	// Final: idle, gen 1, no fetch, never accepted stale.
	if s.IsLoading {
		t.Errorf("IsLoading still true")
	}
	if s.ReloadGen != 1 {
		t.Errorf("ReloadGen = %d, want 1", s.ReloadGen)
	}
	if s.FetchJob != 0 {
		t.Errorf("FetchJob = %d, want 0 (discarded)", s.FetchJob)
	}
	if s.FetchGen != 0 {
		t.Errorf("FetchGen = %d, want 0 (stamped at press)", s.FetchGen)
	}
	if s.StaleAccepted {
		t.Errorf("StaleAccepted = true; supersession failed")
	}

	// Intermediate: after ReloadDone, fetch still in flight with stale gen.
	afterReload := entries[2].Post
	if afterReload.FetchJob != 1 || afterReload.FetchGen != 0 || afterReload.ReloadGen != 1 {
		t.Errorf("after ReloadDone: %+v (want FetchJob=1 FetchGen=0 ReloadGen=1)", afterReload)
	}
	if !entries[2].Post.CanFetchDiscard() {
		t.Error("FetchDiscard should be enabled after ReloadDone with stale fetch")
	}
	if entries[2].Post.CanFetchAccept() {
		t.Error("FetchAccept must NOT be enabled for stale fetch")
	}

	// Write NDJSON for optional conform CLI.
	dir := t.TempDir()
	ndjsonPath := filepath.Join(dir, "tuireload-trace.ndjson")
	f, err := os.Create(ndjsonPath)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote NDJSON trace: %s (%d steps)", ndjsonPath, len(entries))

	// Run conform if available (skip if not on PATH / no JVM tooling).
	conform, err := exec.LookPath("conform")
	if err != nil {
		t.Log("conform not on PATH — skipped external conform; ApplyAction sequence validated above")
		t.Log("manual: conform -spec specs/tui-reload/decision/Decision.tla -config specs/tui-reload/decision/MC.cfg <trace.ndjson>")
		return
	}
	spec, cfg, ok := decisionPaths(t)
	if !ok {
		return
	}
	cmd := exec.Command(conform, "-spec", spec, "-config", cfg, ndjsonPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("conform failed: %v\n%s", err, out)
	}
	t.Logf("conform ok:\n%s", out)
}

// TestFetchAcceptSameGen: fetch without reload is accepted.
func TestFetchAcceptSameGen(t *testing.T) {
	s := Init()
	s = s.ApplyAction("PressFetch2")
	if !s.CanFetchAccept() {
		t.Fatalf("expected FetchAccept enabled: %+v", s)
	}
	s = s.ApplyAction("FetchAccept")
	if s.FetchJob != 0 || s.StaleAccepted {
		t.Fatalf("after accept: %+v", s)
	}
}

// TestModelGoDiscardRule documents the dual with model.go:365-372.
// Decision-core FetchDiscard abstracts the gen half of:
//
//	if msg.jobID == 0 || msg.jobID != m.logFetchingJobID || msg.gen != m.reloadGen {
//	    return m, nil // discard
//	}
func TestModelGoDiscardRule(t *testing.T) {
	// Stale gen after reload → FetchDiscard enabled (gen half of the rule).
	s := Init()
	s = s.ApplyAction("PressFetch1")
	s = s.ApplyAction("PressReload")
	s = s.ApplyAction("ReloadDone")
	// fetchGen=0, reloadGen=1  ⇔  msg.gen != m.reloadGen
	if s.FetchGen == s.ReloadGen {
		t.Fatal("expected gen mismatch")
	}
	if !s.CanFetchDiscard() {
		t.Fatal("FetchDiscard is the decision-core form of model.go discard")
	}
	if s.CanFetchAccept() {
		t.Fatal("must not accept stale")
	}
	s = s.ApplyAction("FetchDiscard")
	if s.StaleAccepted || s.FetchJob != 0 {
		t.Fatalf("discard should clear fetch without accepting: %+v", s)
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// decisionPaths finds Decision.tla + MC.cfg relative to this source file
// or the module root (go test / bazel run layouts differ).
func decisionPaths(t *testing.T) (spec, cfg string, ok bool) {
	t.Helper()
	candidates := []string{}
	if _, file, _, ok := runtime.Caller(0); ok {
		// .../pkg/tui/results/tuireloadspec/conform_test.go → repo root
		root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
		candidates = append(candidates, root)
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, wd,
			filepath.Join(wd, "..", "..", "..", ".."),
		)
	}
	for _, root := range candidates {
		spec = filepath.Join(root, "specs", "tui-reload", "decision", "Decision.tla")
		cfg = filepath.Join(root, "specs", "tui-reload", "decision", "MC.cfg")
		if _, err1 := os.Stat(spec); err1 == nil {
			if _, err2 := os.Stat(cfg); err2 == nil {
				return spec, cfg, true
			}
		}
	}
	t.Log("Decision.tla not found from test paths — skipped conform CLI")
	return "", "", false
}
