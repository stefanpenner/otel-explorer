package logparse

import (
	"testing"
	"time"
)

// FuzzParseLogPipeline drives the whole log-extraction pipeline with arbitrary
// bytes: ParseLogLines (timestamp slicing, ANSI stripping) → Registry.Detect →
// parser.Parse (Gradle/Bazel/SetupJob/Timestamp). Log text comes straight from
// the GitHub API, so malformed/adversarial content must never panic, only
// produce zero or more spans. Seeds hit the GHA timestamp prefix, ANSI codes,
// and shapes each format parser keys on.
func FuzzParseLogPipeline(f *testing.F) {
	seeds := []string{
		"",
		"\n",
		"no timestamp here",
		"2024-01-15T10:30:45.1234567Z hello world",
		"2024-01-15T10:30:45.1234567Z \x1b[31mcolored\x1b[0m text",
		// Gradle-ish
		"2024-01-15T10:30:45.1234567Z > Task :compileJava\n2024-01-15T10:30:46.1234567Z > Task :test",
		// Bazel-ish
		"2024-01-15T10:30:45.1234567Z INFO: Analyzed target //foo:bar\n2024-01-15T10:30:46.1234567Z INFO: Build completed",
		// setup-job-ish
		"2024-01-15T10:30:45.1234567Z ##[group]Run actions/checkout\n2024-01-15T10:30:46.1234567Z ##[endgroup]",
		// short / boundary lines around the 29-char timestamp width
		"2024-01-15T10:30:45.1234567Z",
		"2024-01-15T10:30:45.1234567",
		"短い行 with multibyte runes before the timestamp width boundary",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	reg := DefaultRegistry()
	base := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)

	f.Fuzz(func(t *testing.T, raw []byte) {
		lines := ParseLogLines(raw)
		// Exercise detection + parse with a sane and an inverted window.
		reg.Parse(lines, base, base.Add(time.Hour))
		reg.Parse(lines, base, base)                // zero-duration window (division guards)
		reg.Parse(lines, base.Add(time.Hour), base) // end before start
	})
}
