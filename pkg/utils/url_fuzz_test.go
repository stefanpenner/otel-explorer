package utils

import "testing"

// FuzzParseGitHubURL drives the GitHub-URL parser with arbitrary strings.
// ParseGitHubURL consumes CLI arguments directly, so for any input it must
// return a value or an error — never panic. Seeds cover the three accepted
// shapes (PR, commit, run) plus shorthand and malformed variants so the
// fuzzer reaches every branch.
func FuzzParseGitHubURL(f *testing.F) {
	seeds := []string{
		"",
		"github.com/owner/repo/pull/123",
		"https://github.com/owner/repo/pull/123",
		"https://github.com/owner/repo/pull/123/files",
		"owner/repo/commit/abc123",
		"https://github.com/owner/repo/commit/abc123",
		"owner/repo/actions/runs/12345",
		"https://github.com/owner/repo/actions/runs/12345",
		"not a url",
		"http://example.com/foo/bar",
		"https://github.com/only-one-segment",
		"https://github.com/a/b/pull/",
		"://broken",
		"github.com/a/b/c/d/e/f/g/h",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		result, err := ParseGitHubURL(raw)
		if err != nil {
			// On error, result must be the zero value (no partial leaks).
			if result != (ParsedGitHubURL{}) {
				t.Fatalf("ParseGitHubURL returned non-zero result with error: %+v", result)
			}
			return
		}
		// On success, owner/repo/type/identifier must be populated and host
		// must be github.com. These invariants hold for every accepted shape.
		if result.Owner == "" || result.Repo == "" || result.Type == "" || result.Identifier == "" {
			t.Fatalf("ParseGitHubURL accepted %q but left a required field empty: %+v", raw, result)
		}
		switch result.Type {
		case "pr", "commit", "run":
		default:
			t.Fatalf("ParseGitHubURL returned unknown type %q for %q", result.Type, raw)
		}
	})
}
