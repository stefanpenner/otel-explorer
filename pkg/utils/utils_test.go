package utils

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseGitHubURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		url        string
		expectType string
		expectID   string
		wantError  bool
	}{
		{name: "pr url", url: "https://github.com/owner/repo/pull/123", expectType: "pr", expectID: "123"},
		{name: "commit url", url: "https://github.com/owner/repo/commit/abc123def", expectType: "commit", expectID: "abc123def"},
		{name: "short pr url", url: "owner/repo/pull/123", expectType: "pr", expectID: "123"},
		{name: "short commit url", url: "owner/repo/commit/abc123def", expectType: "commit", expectID: "abc123def"},
		{name: "github.com pr url", url: "github.com/owner/repo/pull/123", expectType: "pr", expectID: "123"},
		{name: "run url", url: "https://github.com/owner/repo/actions/runs/12345", expectType: "run", expectID: "12345"},
		{name: "run url with job suffix", url: "https://github.com/owner/repo/actions/runs/12345/job/67890", expectType: "run", expectID: "12345"},
		{name: "short run url", url: "owner/repo/actions/runs/12345", expectType: "run", expectID: "12345"},
		{name: "invalid url", url: "https://github.com/owner/repo/issues/123", wantError: true},
		{name: "completely invalid", url: "not-a-url", wantError: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseGitHubURL(tc.url)
			if tc.wantError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectType, result.Type)
			assert.Equal(t, tc.expectID, result.Identifier)
		})
	}
}

func TestHumanizeTime(t *testing.T) {
	t.Parallel()

	cases := []struct {
		seconds  float64
		expected string
	}{
		{seconds: 0, expected: "0s"},
		{seconds: 0.5, expected: "500ms"},
		{seconds: 1, expected: "1s"},
		{seconds: 65, expected: "1m 5s"},
		{seconds: 3661, expected: "1h 1m 1s"},
		{seconds: 86400, expected: "24h"},
	}

	for _, tc := range cases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, HumanizeTime(tc.seconds))
		})
	}
}

// TestHumanizeTimeInteriorZeros verifies that a zero minutes component is not
// dropped when both a larger (hours) and smaller (seconds) component are
// present. "1h 3s" is ambiguous (reads like 1h0m3s or 1h3s); the minutes
// placeholder must be rendered so the value is unambiguous. Trailing zero
// components are still suppressed.
func TestHumanizeTimeInteriorZeros(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		seconds  float64
		expected string
	}{
		{name: "hours and seconds, zero minutes", seconds: 3603, expected: "1h 0m 3s"},
		{name: "two hours, zero minutes", seconds: 7203, expected: "2h 0m 3s"},
		{name: "trailing zeros suppressed (exact hour)", seconds: 3600, expected: "1h"},
		{name: "trailing zero seconds suppressed", seconds: 3660, expected: "1h 1m"},
		{name: "all three components present", seconds: 3661, expected: "1h 1m 1s"},
		{name: "minutes and seconds unaffected", seconds: 65, expected: "1m 5s"},
		{name: "negative hours and seconds, zero minutes", seconds: -3603, expected: "-1h 0m 3s"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, HumanizeTime(tc.seconds))
		})
	}
}

// TestRoundTrendDuration pins the display grid used by the trend regression /
// improvement tables: tenths of a second below a minute, whole seconds at or
// above. Percentages are computed from these same rounded values so that the
// Was/Now columns reproduce the displayed Change.
func TestRoundTrendDuration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		seconds float64
		want    float64
	}{
		{seconds: 6.0, want: 6.0},
		{seconds: 8.47, want: 8.5},
		{seconds: 17.44, want: 17.4},
		{seconds: 59.96, want: 60.0},
		{seconds: 62.4, want: 62.0},
		{seconds: 3253.4, want: 3253.0},
	}

	for _, tc := range cases {
		assert.InDelta(t, tc.want, RoundTrendDuration(tc.seconds), 1e-9,
			"RoundTrendDuration(%v)", tc.seconds)
	}
}

// TestFormatTrendDuration verifies sub-minute durations show one decimal so the
// percentage change is reproducible from the displayed value, while durations
// of a minute or more keep the compact HumanizeTime form.
func TestFormatTrendDuration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		seconds  float64
		expected string
	}{
		{seconds: 6.0, expected: "6.0s"},
		{seconds: 8.47, expected: "8.5s"},
		{seconds: 17.44, expected: "17.4s"},
		{seconds: 44.0, expected: "44.0s"},
		{seconds: 62.0, expected: "1m 2s"},
		{seconds: 3253.4, expected: "54m 13s"},
		{seconds: 59.96, expected: "1m"},
		{seconds: 0, expected: "0s"},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.expected, FormatTrendDuration(tc.seconds),
			"FormatTrendDuration(%v)", tc.seconds)
	}
}

func TestStepCategorizationAndIcons(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		category string
		icon     string
	}{
		{name: "Checkout code", category: "step_checkout", icon: "📥"},
		{name: "Setup Node.js", category: "step_setup", icon: "⚙️"},
		{name: "Build project", category: "step_build", icon: "🔨"},
		{name: "Run tests", category: "step_test", icon: "🧪"},
		{name: "Lint code", category: "step_lint", icon: "🔍"},
		{name: "Deploy to prod", category: "step_deploy", icon: "🚀"},
		{name: "Upload artifacts", category: "step_artifact", icon: "📤"},
		{name: "Security scan", category: "step_security", icon: "🔒"},
		{name: "Send notification", category: "step_notify", icon: "📢"},
		{name: "Custom step", category: "step_other", icon: "▶️"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.category, CategorizeStep(tc.name))
			assert.Equal(t, tc.icon, GetStepIcon(tc.name, "success"))
		})
	}
	assert.Equal(t, "❌", GetStepIcon("Any step", "failure"))
}

func TestGetStepIconConclusions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		conclusion string
		expected   string
	}{
		{"failure", "❌"},
		{"cancelled", "🚫"},
		{"skipped", "⏭️"},
	}

	for _, tc := range cases {
		t.Run(tc.conclusion, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetStepIcon("any step", tc.conclusion))
		})
	}
}

func TestGetJobGroup(t *testing.T) {
	t.Parallel()

	cases := []struct {
		jobName  string
		expected string
	}{
		{"build / linux", "build"},
		{"test / unit / fast", "test"},
		{"single-job", "single-job"},
		{"", ""},
	}

	for _, tc := range cases {
		t.Run(tc.jobName, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetJobGroup(tc.jobName))
		})
	}
}

func TestExpandGitHubURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input    string
		expected string
	}{
		{"https://github.com/owner/repo/pull/1", "https://github.com/owner/repo/pull/1"},
		{"http://github.com/owner/repo/pull/1", "http://github.com/owner/repo/pull/1"},
		{"github.com/owner/repo/pull/1", "https://github.com/owner/repo/pull/1"},
		{"owner/repo/pull/1", "https://github.com/owner/repo/pull/1"},
		{"nodejs/node/pull/60369", "https://github.com/nodejs/node/pull/60369"},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, ExpandGitHubURL(tc.input))
		})
	}
}

func TestMakeClickableLink(t *testing.T) {
	t.Parallel()

	t.Run("returns display text for non-github URL", func(t *testing.T) {
		result := MakeClickableLink("https://example.com", "click me")
		assert.Equal(t, "click me", result)
	})

	t.Run("returns URL when text is empty for non-github", func(t *testing.T) {
		result := MakeClickableLink("https://example.com", "")
		assert.Equal(t, "https://example.com", result)
	})

	t.Run("wraps github URL in OSC 8 hyperlink", func(t *testing.T) {
		result := MakeClickableLink("https://github.com/owner/repo", "repo link")
		assert.Contains(t, result, "\u001b]8;;https://github.com/owner/repo\u0007")
		assert.Contains(t, result, "repo link")
		assert.Contains(t, result, "\u001b]8;;\u0007")
	})
}

func TestParseTime(t *testing.T) {
	t.Parallel()

	t.Run("parses valid RFC3339 time", func(t *testing.T) {
		parsed, ok := ParseTime("2026-01-15T10:30:00Z")
		assert.True(t, ok)
		assert.Equal(t, 2026, parsed.Year())
		assert.Equal(t, time.January, parsed.Month())
		assert.Equal(t, 15, parsed.Day())
	})

	t.Run("returns false for empty string", func(t *testing.T) {
		_, ok := ParseTime("")
		assert.False(t, ok)
	})

	t.Run("returns false for invalid format", func(t *testing.T) {
		_, ok := ParseTime("not a date")
		assert.False(t, ok)
	})
}

func TestStripANSI(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain text unchanged", "hello world", "hello world"},
		{"strips SGR codes", "\u001b[32mgreen\u001b[0m", "green"},
		{"strips OSC hyperlink", "\u001b]8;;https://example.com\u0007link\u001b]8;;\u0007", "link"},
		{"preserves tabs and newlines", "line1\n\tline2", "line1\n\tline2"},
		{"strips control chars", "hello\x01world", "helloworld"},
		{"complex ANSI sequence", "\u001b[1;31mred bold\u001b[0m normal", "red bold normal"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, StripANSI(tc.input))
		})
	}
}

func TestGlobMatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"exact", "exact", true},
		{"exact", "other", false},
		{"prefix*", "prefix-something", true},
		{"prefix*", "other", false},
		{"*suffix", "something-suffix", true},
		{"*suffix", "other", false},
		{"*middle*", "has-middle-here", true},
		{"*middle*", "no match", false},
		{"5*", "503", true},
		{"5*", "200", false},
	}

	for _, tc := range cases {
		t.Run(tc.pattern+"_"+tc.value, func(t *testing.T) {
			assert.Equal(t, tc.want, GlobMatch(tc.pattern, tc.value))
		})
	}
}

func TestColorFormatters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		fn       func(string) string
		expected string
	}{
		{"GrayText", GrayText, "\u001b[90mtest\u001b[0m"},
		{"GreenText", GreenText, "\u001b[32mtest\u001b[0m"},
		{"RedText", RedText, "\u001b[31mtest\u001b[0m"},
		{"YellowText", YellowText, "\u001b[33mtest\u001b[0m"},
		{"BlueText", BlueText, "\u001b[34mtest\u001b[0m"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.fn("test"))
		})
	}
}

func TestHumanizeTimeBoundaries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		seconds  float64
		expected string
	}{
		{name: "rounds up to 1s not 1000ms", seconds: 0.9995, expected: "1s"},
		{name: "just below rounding boundary", seconds: 0.999, expected: "999ms"},
		{name: "tiny value", seconds: 0.0001, expected: "0s"},
		{name: "negative sub-second", seconds: -0.5, expected: "-500ms"},
		{name: "negative seconds", seconds: -2, expected: "-2s"},
		{name: "negative minutes", seconds: -65, expected: "-1m 5s"},
		{name: "negative rounds up to 1s", seconds: -0.9995, expected: "-1s"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, HumanizeTime(tc.seconds))
		})
	}
}

func TestParseGitHubURLTrailingSegments(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		url        string
		expectType string
		expectID   string
	}{
		{name: "pr files tab", url: "https://github.com/owner/repo/pull/123/files", expectType: "pr", expectID: "123"},
		{name: "pr commits tab", url: "https://github.com/owner/repo/pull/123/commits", expectType: "pr", expectID: "123"},
		{name: "pr checks tab", url: "https://github.com/owner/repo/pull/123/checks", expectType: "pr", expectID: "123"},
		{name: "commit with suffix", url: "https://github.com/owner/repo/commit/abc123/checks", expectType: "commit", expectID: "abc123"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseGitHubURL(tc.url)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectType, result.Type)
			assert.Equal(t, tc.expectID, result.Identifier)
		})
	}
}

// Note: deliberately not parallel — it mutates the package-level color gate.
func TestColorGating(t *testing.T) {
	defer SetColorEnabled(true)

	SetColorEnabled(false)
	assert.False(t, ColorEnabled())
	assert.Equal(t, "plain", GreenText("plain"))
	assert.Equal(t, "plain", RedText("plain"))
	assert.Equal(t, "plain", YellowText("plain"))
	assert.Equal(t, "plain", BlueText("plain"))
	assert.Equal(t, "plain", GrayText("plain"))
	assert.Equal(t, "repo link", MakeClickableLink("https://github.com/owner/repo", "repo link"))

	SetColorEnabled(true)
	assert.True(t, ColorEnabled())
	assert.Contains(t, GreenText("plain"), "[32m")
	assert.Contains(t, MakeClickableLink("https://github.com/owner/repo", "repo link"), "]8;;")
}

func TestStripANSIWriter(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	w := NewStripANSIWriter(&sb)
	input := GreenText("ok") + " " + MakeClickableLink("https://github.com/o/r", "link")
	n, err := w.Write([]byte(input))
	assert.NoError(t, err)
	assert.Equal(t, len(input), n)
	assert.Equal(t, "ok link", sb.String())
}
