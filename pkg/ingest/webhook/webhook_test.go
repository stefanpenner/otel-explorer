package webhook

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseWebhook(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantURLs    []string
		wantErr     bool
		errContains string
	}{
		{
			name:     "workflow_run completed",
			input:    `{"action":"completed","workflow_run":{"head_sha":"abc123"},"repository":{"full_name":"owner/repo"}}`,
			wantURLs: []string{"owner/repo/commit/abc123"},
		},
		{
			name:     "workflow_job completed",
			input:    `{"action":"completed","workflow_job":{"head_sha":"def456"},"repository":{"full_name":"org/project"}}`,
			wantURLs: []string{"org/project/commit/def456"},
		},
		{
			name:     "workflow_run prefers over job",
			input:    `{"action":"completed","workflow_run":{"head_sha":"run-sha"},"workflow_job":{"head_sha":"job-sha"},"repository":{"full_name":"owner/repo"}}`,
			wantURLs: []string{"owner/repo/commit/run-sha"},
		},
		{
			name:        "missing repository",
			input:       `{"workflow_run":{"head_sha":"abc"}}`,
			wantErr:     true,
			errContains: "missing repository",
		},
		{
			name:        "missing head_sha",
			input:       `{"workflow_run":{},"repository":{"full_name":"a/b"}}`,
			wantErr:     true,
			errContains: "empty head_sha",
		},
		{
			name:        "empty full_name",
			input:       `{"workflow_run":{"head_sha":"abc"},"repository":{"full_name":""}}`,
			wantErr:     true,
			errContains: "empty repository full_name",
		},
		{
			name:        "no recognized event",
			input:       `{"action":"created","issue":{},"repository":{"full_name":"owner/repo"}}`,
			wantErr:     true,
			errContains: "no recognized event",
		},
		{
			name:        "empty JSON object",
			input:       `{}`,
			wantErr:     true,
			errContains: "missing repository",
		},
		{
			name:        "empty input",
			input:       ``,
			wantErr:     true,
			errContains: "empty webhook payload",
		},
		{
			name:        "invalid JSON",
			input:       `not json`,
			wantErr:     true,
			errContains: "failed to parse webhook JSON",
		},
		{
			name:        "null body",
			input:       `null`,
			wantErr:     true,
			errContains: "missing repository",
		},
		{
			name:        "array input",
			input:       `[1,2,3]`,
			wantErr:     true,
			errContains: "failed to parse webhook JSON",
		},
		{
			name:     "extra fields ignored",
			input:    `{"action":"completed","workflow_run":{"head_sha":"abc","name":"CI","extra":true},"repository":{"full_name":"owner/repo","id":12345},"sender":{"login":"user"}}`,
			wantURLs: []string{"owner/repo/commit/abc"},
		},
		{
			name:     "very long sha",
			input:    `{"workflow_run":{"head_sha":"` + strings.Repeat("a", 200) + `"},"repository":{"full_name":"owner/repo"}}`,
			wantURLs: []string{"owner/repo/commit/" + strings.Repeat("a", 200)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urls, err := ParseWebhook(strings.NewReader(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(urls) != len(tt.wantURLs) {
				t.Fatalf("got %d URLs, want %d", len(urls), len(tt.wantURLs))
			}
			for i, u := range urls {
				if u != tt.wantURLs[i] {
					t.Errorf("url[%d] = %q, want %q", i, u, tt.wantURLs[i])
				}
			}
		})
	}
}

func TestParseWebhook_OversizedPayload(t *testing.T) {
	bigPayload := strings.Repeat("a", maxWebhookBytes+1)
	_, err := ParseWebhook(strings.NewReader(bigPayload))
	if err == nil {
		t.Fatal("expected error for oversized payload")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Errorf("expected 'too large' error, got: %v", err)
	}
}

func FuzzParseWebhook(f *testing.F) {
	f.Add([]byte(`{"workflow_run":{"head_sha":"abc"},"repository":{"full_name":"o/r"}}`))
	f.Add([]byte(`{"workflow_job":{"head_sha":"abc"},"repository":{"full_name":"o/r"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(``))
	f.Add([]byte(`{"action":"completed"}`))
	f.Add([]byte{0xff, 0xfe})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic — may return error or valid result
		ParseWebhook(bytes.NewReader(data))
	})
}
