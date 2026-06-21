package analyzer

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestTruncateString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		max   int
		want  string
	}{
		{name: "short passthrough", input: "ab", max: 5, want: "ab"},
		{name: "empty", input: "", max: 5, want: ""},
		{name: "ascii truncate", input: "hello world", max: 5, want: "hello"},
		{name: "multibyte no split", input: "ビルドテスト", max: 4, want: "ビルドテ"},
		{name: "exact fit", input: "abc", max: 3, want: "abc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateString(tc.input, tc.max)
			assert.Equal(t, tc.want, got)
			assert.True(t, utf8.ValidString(got), "result must be valid UTF-8")
		})
	}
}

func TestIsJobRequired_PrefixBoundary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		jobName          string
		workflowName     string
		requiredContexts []string
		want             bool
	}{
		{name: "exact job match", jobName: "test", workflowName: "wf", requiredContexts: []string{"test"}, want: true},
		{name: "exact workflow match", jobName: "test", workflowName: "wf", requiredContexts: []string{"wf"}, want: true},
		{name: "full name match", jobName: "test", workflowName: "wf", requiredContexts: []string{"wf / test"}, want: true},
		{name: "context matches job with matrix suffix", jobName: "test (ubuntu, 18)", workflowName: "wf", requiredContexts: []string{"test"}, want: true},
		{name: "context matches fullName with matrix suffix", jobName: "test (ubuntu, 18)", workflowName: "wf", requiredContexts: []string{"wf / test"}, want: true},
		{name: "prefix false positive: test vs testing", jobName: "testing platform", workflowName: "wf", requiredContexts: []string{"test"}, want: false},
		{name: "no match", jobName: "deploy", workflowName: "wf", requiredContexts: []string{"test"}, want: false},
		{name: "empty contexts = all required", jobName: "test", workflowName: "wf", requiredContexts: nil, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isJobRequired(tc.jobName, tc.workflowName, tc.requiredContexts)
			assert.Equal(t, tc.want, got)
		})
	}
}
