package githubapi

import (
	"testing"
)

// FuzzSplitJobLogByStep drives the per-step log splitter with arbitrary log
// bytes and arbitrary step time-range strings. Job logs are downloaded raw
// from GitHub (fully untrusted), and the step times come from a separate API
// response, so neither may panic the splitter. The fuzzer varies the log body
// plus two steps' RFC3339 timestamps (which flow through time.Parse and the
// best-match index search).
func FuzzSplitJobLogByStep(f *testing.F) {
	f.Add([]byte("2024-01-15T10:30:45.1234567Z hello\n2024-01-15T10:31:00.0000000Z world\n"),
		"2024-01-15T10:30:00Z", "2024-01-15T10:30:59Z",
		"2024-01-15T10:31:00Z", "2024-01-15T10:31:59Z")
	f.Add([]byte("short\n"), "", "", "", "")
	f.Add([]byte(""), "not-a-time", "also-bad", "2024-01-15T10:31:00Z", "2024-01-15T10:30:00Z")
	f.Add([]byte("2024-01-15T10:30:45.1234567Z line with no matching step\n"),
		"2025-01-01T00:00:00Z", "2025-01-01T00:00:01Z", "2025-01-02T00:00:00Z", "2025-01-02T00:00:01Z")

	f.Fuzz(func(t *testing.T, logData []byte, s1Start, s1End, s2Start, s2End string) {
		steps := []Step{
			{Number: 1, StartedAt: s1Start, CompletedAt: s1End},
			{Number: 2, StartedAt: s2Start, CompletedAt: s2End},
		}
		// Must not panic regardless of timestamps or log content.
		SplitJobLogByStep(logData, steps)
	})
}
