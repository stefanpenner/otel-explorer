package ratelimitspec

// Handwritten dual / conform checks for the rate-limit wake-recheck
// decision core (FINDING 13). Generated code is never hand-edited;
// this file is the mechanics↔decision bridge.
//
// Code under test: pkg/githubapi/client.go rateLimiter.waitIfNeeded
// (loop sleep → recheck, client.go:361-379).
// Spec: specs/rate-limit/decision/Decision.tla
// Integration dual: pkg/githubapi/client_test.go
//   TestRateLimitSpec_WakeRechecksLimiter

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestDecision_WakeRecheckSequence walks the faithful FINDING 13 path:
// learn exhausted → sleep → recheck-resleep while WaitNeeded → tick past
// reset → wake-recheck-send. Never sets SentWhileExhausted.
func TestDecision_WakeRecheckSequence(t *testing.T) {
	s := Init()
	if s.Remaining != 2 || s.Sleeping {
		t.Fatalf("Init = %+v, want Remaining=2 Sleeping=false", s)
	}

	// LearnExhausted: updateFromHeaders remaining=0, reset=clock+1
	if !s.CanLearnExhausted() {
		t.Fatal("CanLearnExhausted should hold at Init")
	}
	var entry TraceEntry
	entry, s = s.Trace("LearnExhausted")
	if s.Remaining != 0 || s.ResetAt != 1 {
		t.Fatalf("after LearnExhausted: %+v", s)
	}
	_ = entry

	// StartSleep: waitIfNeeded sees WaitNeeded, sleepContext
	if !s.CanStartSleep() {
		t.Fatal("CanStartSleep should hold after LearnExhausted")
	}
	_, s = s.Trace("StartSleep")
	if !s.Sleeping {
		t.Fatal("expected sleeping after StartSleep")
	}

	// While WaitNeeded: only recheck-resleep (faithful), never bug-send
	if s.CanWakeBugSend() {
		t.Fatal("WakeBugSend must be disabled when Bug=FALSE")
	}
	if !s.CanWakeRecheckResleep() {
		t.Fatal("CanWakeRecheckResleep should hold while WaitNeeded")
	}
	if s.CanWakeRecheckSend() {
		t.Fatal("CanWakeRecheckSend must not hold while WaitNeeded")
	}
	_, s = s.Trace("WakeRecheckResleep")
	if !s.Sleeping || s.SentWhileExhausted {
		t.Fatalf("resleep must stay sleeping without fault: %+v", s)
	}

	// Tick past resetAt (clock 0 → 1); remaining may refill
	if !s.CanTick() {
		t.Fatal("CanTick should hold")
	}
	_, s = s.Trace("Tick")
	if s.Clock != 1 {
		t.Fatalf("clock = %d, want 1", s.Clock)
	}

	// Recheck: no wait needed → wake and send OK
	if !s.CanWakeRecheckSend() {
		t.Fatalf("CanWakeRecheckSend should hold after reset; state=%+v", s)
	}
	_, s = s.Trace("WakeRecheckSend")
	if s.Sleeping {
		t.Fatal("expected awake after WakeRecheckSend")
	}
	if s.SentWhileExhausted {
		t.Fatal("faithful path must not set SentWhileExhausted")
	}
	if s.Remaining != 2 {
		t.Fatalf("remaining after wake-send = %d, want MaxRem=2", s.Remaining)
	}
}

// TestDecision_NoSendWhileExhausted_BFS checks the load-bearing invariant
// over every reachable state of the generated module (Bug=FALSE).
func TestDecision_NoSendWhileExhausted_BFS(t *testing.T) {
	seen := map[State]bool{}
	queue := []State{Init()}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		if seen[s] {
			continue
		}
		seen[s] = true
		if s.SentWhileExhausted {
			t.Fatalf("NoSendWhileExhausted violated at %+v", s)
		}
		for _, a := range s.EnabledActions() {
			queue = append(queue, s.ApplyAction(a))
		}
	}
	if len(seen) < 2 {
		t.Fatalf("explored too few states: %d", len(seen))
	}
	t.Logf("explored %d states; NoSendWhileExhausted held", len(seen))
}

// TestDecision_WaitNeededMirrorsWaitDuration documents the guard that
// production waitDuration() implements (client.go:349-359):
//
//	remaining == 0 && !resetTime.IsZero() && time.Until(resetTime) > 0
//
// as WaitNeeded == remaining=0 /\ resetAt>0 /\ clock<resetAt.
// StartSleep / WakeRecheckResleep require it; WakeRecheckSend requires ~it.
func TestDecision_WaitNeededMirrorsWaitDuration(t *testing.T) {
	// Exhausted, reset in future → WaitNeeded path (StartSleep enabled)
	s := State{Remaining: 0, Sleeping: false, Clock: 0, ResetAt: 2}
	if !s.CanStartSleep() {
		t.Fatal("StartSleep should mirror waitDuration()>0")
	}
	s = s.StartSleep()
	if !s.CanWakeRecheckResleep() || s.CanWakeRecheckSend() {
		t.Fatalf("while WaitNeeded: resleep yes, send no; %+v", s)
	}

	// Reset reached → waitDuration()==0 → recheck send
	s.Clock = 2
	if s.CanWakeRecheckResleep() || !s.CanWakeRecheckSend() {
		t.Fatalf("after reset: resleep no, send yes; %+v", s)
	}
}

// TestDecision_ConformTrace emits NDJSON via Trace and runs conform when
// the binary and spec are available (skipped otherwise — pure Go checks
// above always run).
func TestDecision_ConformTrace(t *testing.T) {
	conform, err := exec.LookPath("conform")
	if err != nil {
		t.Skip("conform not on PATH")
	}
	specPath, cfgPath, ok := findDecisionSpec()
	if !ok {
		t.Skip("specs/rate-limit/decision not found from test cwd")
	}

	s := Init()
	var lines [][]byte
	for _, action := range []string{
		"LearnExhausted",
		"StartSleep",
		"WakeRecheckResleep",
		"Tick",
		"WakeRecheckSend",
	} {
		if !contains(s.EnabledActions(), action) {
			t.Fatalf("action %q not enabled at %+v (enabled=%v)", action, s, s.EnabledActions())
		}
		var e TraceEntry
		e, s = s.Trace(action)
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, b)
	}

	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ndjson")
	f, err := os.Create(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range lines {
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(conform,
		"-spec", specPath,
		"-config", cfgPath,
		tracePath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("conform failed: %v\n%s", err, out)
	}
	t.Logf("conform ok:\n%s", out)
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// findDecisionSpec walks up from this source file to the repo root and
// returns Decision.tla + MC.cfg paths.
func findDecisionSpec() (spec, cfg string, ok bool) {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	for i := 0; i < 8; i++ {
		cand := filepath.Join(dir, "specs", "rate-limit", "decision")
		spec = filepath.Join(cand, "Decision.tla")
		cfg = filepath.Join(cand, "MC.cfg")
		if st, err := os.Stat(spec); err == nil && !st.IsDir() {
			if st, err := os.Stat(cfg); err == nil && !st.IsDir() {
				return spec, cfg, true
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", "", false
}
