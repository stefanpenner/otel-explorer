package analyzer

import "testing"

func TestClassifyBranch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		branch        string
		defaultBranch string
		want          string
	}{
		{"default branch is upstream", "main", "main", "upstream"},
		{"master default", "master", "master", "upstream"},
		{"main is upstream even when default unknown", "main", "", "upstream"},
		{"master is upstream even when default unknown", "master", "", "upstream"},
		{"node-style release line", "v20.x", "main", "upstream"},
		{"dotted version", "v18.17.1", "main", "upstream"},
		{"release/ prefix", "release/1.2", "main", "upstream"},
		{"rails-style stable line", "7-1-stable", "main", "upstream"},
		{"feature branch", "fix/clamp-times", "main", "feature"},
		{"dependabot branch", "dependabot/npm/foo-1.2.3", "main", "feature"},
		{"user fork topic", "stefanpenner/add-faceting", "main", "feature"},
		{"empty branch is feature", "", "main", "feature"},
		{"main stays upstream even if default differs", "main", "develop", "upstream"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyBranch(tc.branch, tc.defaultBranch); got != tc.want {
				t.Errorf("classifyBranch(%q, %q) = %q, want %q", tc.branch, tc.defaultBranch, got, tc.want)
			}
		})
	}
}
