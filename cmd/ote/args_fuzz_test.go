package main

import (
	"strings"
	"testing"
)

// FuzzParseArgs drives the CLI argument parser with arbitrary whitespace-split
// tokens. parseArgs slices flag values (--days=, --facet=, --margin=, etc.) and
// reorders the subcommand out of the args slice; malformed flags must surface
// as errors, never panic (out-of-range slice, bad index after reordering).
func FuzzParseArgs(f *testing.F) {
	seeds := []string{
		"",
		"trends nodejs/node --days=7 --facet=all",
		"--clear-cache trends owner/repo",
		"--days=",
		"--days=notanumber",
		"--margin=",
		"--facet=",
		"convert file.json --output=json",
		"sync owner/repo --days=30",
		"--days=--facet=trends",
		"trends trends trends",
		"-",
		"--",
		"=",
		"--days=99999999999999999999999999",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		args := strings.Fields(raw)
		// Both terminal modes; must return (config, error) without panicking.
		_, _ = parseArgs(args, false)
		_, _ = parseArgs(args, true)
	})
}
