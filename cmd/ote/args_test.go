package main

import (
	"testing"
	"time"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		isTerminal bool
		want       config
		wantErr    bool
	}{
		{
			name:       "no args",
			args:       []string{},
			isTerminal: false,
			want:       config{},
		},
		{
			name:       "URLs only",
			args:       []string{"url1", "url2"},
			isTerminal: false,
			want:       config{urls: []string{"url1", "url2"}},
		},
		{
			name:       "tuiMode defaults to isTerminal true",
			args:       []string{"url"},
			isTerminal: true,
			want:       config{urls: []string{"url"}, tuiMode: true},
		},
		{
			name:       "tuiMode defaults to isTerminal false",
			args:       []string{"url"},
			isTerminal: false,
			want:       config{urls: []string{"url"}},
		},
		{
			name:       "--tui flag",
			args:       []string{"url", "--tui"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, tuiMode: true},
		},
		{
			name:       "--no-tui flag",
			args:       []string{"url", "--no-tui"},
			isTerminal: true,
			want:       config{urls: []string{"url"}},
		},
		{
			name:       "--notui alias",
			args:       []string{"url", "--notui"},
			isTerminal: true,
			want:       config{urls: []string{"url"}},
		},
		{
			name:       "bare --otel sets otelStdout",
			args:       []string{"url", "--otel"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, otelStdout: true},
		},
		{
			name:       "--otel=endpoint sets otelEndpoint",
			args:       []string{"url", "--otel=host:4318"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, otelEndpoint: "host:4318"},
		},
		{
			name:       "bare --otel-grpc defaults to localhost:4317",
			args:       []string{"url", "--otel-grpc"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, otelGRPCEndpoint: "localhost:4317"},
		},
		{
			name:       "--otel-grpc=endpoint sets custom endpoint",
			args:       []string{"url", "--otel-grpc=host:9999"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, otelGRPCEndpoint: "host:9999"},
		},
		{
			name:       "--output=json",
			args:       []string{"url", "--output=json"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, outputFormat: "json"},
		},
		{
			name:       "--output=xlsx with --out",
			args:       []string{"url", "--output=xlsx", "--out=report.xlsx"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, outputFormat: "xlsx", outFile: "report.xlsx"},
		},
		{
			name:       "--output=doc",
			args:       []string{"url", "--output=doc"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, outputFormat: "doc"},
		},
		{
			name:       "--output=html",
			args:       []string{"url", "--output=html"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, outputFormat: "html"},
		},
		{
			name:       "--output=slack",
			args:       []string{"url", "--output=slack"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, outputFormat: "slack"},
		},
		{
			name:       "--output=slack with --slack-webhook",
			args:       []string{"url", "--output=slack", "--slack-webhook=https://hooks.slack.com/x"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, outputFormat: "slack", slackWebhook: "https://hooks.slack.com/x"},
		},
		{
			name:       "invalid --output returns error",
			args:       []string{"url", "--output=pdf"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--window=2h",
			args:       []string{"url", "--window=2h"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, window: 2 * time.Hour},
		},
		{
			name:       "--window=bad returns error",
			args:       []string{"url", "--window=bad"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--clear-cache flag",
			args:       []string{"--clear-cache"},
			isTerminal: false,
			want:       config{clearCache: true},
		},
		{
			name:       "--help flag",
			args:       []string{"--help"},
			isTerminal: false,
			want:       config{showHelp: true},
		},
		{
			name:       "-h flag",
			args:       []string{"-h"},
			isTerminal: false,
			want:       config{showHelp: true},
		},
		{
			name:       "help word",
			args:       []string{"help"},
			isTerminal: false,
			want:       config{showHelp: true},
		},
		{
			name:       "--version flag",
			args:       []string{"--version"},
			isTerminal: false,
			want:       config{showVersion: true},
		},
		{
			name:       "-v flag",
			args:       []string{"-v"},
			isTerminal: false,
			want:       config{showVersion: true},
		},
		{
			name:       "--perfetto=file.json",
			args:       []string{"url", "--perfetto=trace.json"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, perfettoFile: "trace.json"},
		},
		{
			name:       "--open-in-perfetto flag",
			args:       []string{"url", "--open-in-perfetto"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, openInPerfetto: true},
		},
		{
			name:       "--open-in-otel flag",
			args:       []string{"url", "--open-in-otel"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, openInOTel: true},
		},
		{
			name:       "unknown flags pass through as URLs",
			args:       []string{"--unknown"},
			isTerminal: false,
			want:       config{urls: []string{"--unknown"}},
		},
		{
			name:       "multiple flags combined",
			args:       []string{"url", "--otel", "--otel-grpc", "--no-tui"},
			isTerminal: true,
			want: config{
				urls:             []string{"url"},
				otelStdout:       true,
				otelGRPCEndpoint: "localhost:4317",
			},
		},
		{
			name:       "--clear-cache with URL",
			args:       []string{"--clear-cache", "url"},
			isTerminal: false,
			want:       config{urls: []string{"url"}, clearCache: true},
		},
		{
			name:       "--output=stdout sets outputFormat and disables TUI",
			args:       []string{"url", "--output=stdout"},
			isTerminal: true,
			want:       config{urls: []string{"url"}, outputFormat: "stdout"},
		},
		{
			name:       "--output=markdown sets outputFormat and disables TUI",
			args:       []string{"url", "--output=markdown"},
			isTerminal: true,
			want:       config{urls: []string{"url"}, outputFormat: "markdown"},
		},
		{
			name:       "--output=invalid returns error",
			args:       []string{"url", "--output=invalid"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--output then --tui still disables TUI (order independent)",
			args:       []string{"url", "--output=markdown", "--tui"},
			isTerminal: true,
			want:       config{urls: []string{"url"}, outputFormat: "markdown"},
		},
		{
			name:       "--tui then --output disables TUI (order independent)",
			args:       []string{"url", "--tui", "--output=markdown"},
			isTerminal: true,
			want:       config{urls: []string{"url"}, outputFormat: "markdown"},
		},
		{
			name:       "--listen with --output returns error",
			args:       []string{"--listen", "--output=markdown"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--tempo with --trace-id is accepted",
			args:       []string{"--tempo=http://localhost:3200", "--trace-id=abc"},
			isTerminal: false,
			want:       config{},
		},
		{
			name:       "--tempo and --jaeger together returns error",
			args:       []string{"--tempo=http://a", "--jaeger=http://b", "--trace-id=abc"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--tempo without --trace-id returns error",
			args:       []string{"--tempo=http://localhost:3200"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--jaeger without --trace-id returns error",
			args:       []string{"--jaeger=http://localhost:16686"},
			isTerminal: false,
			wantErr:    true,
		},
		{
			name:       "--no-sample flag in trends mode",
			args:       []string{"trends", "owner/repo", "--no-sample"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", trendsNoSample: true},
		},
		{
			name:       "--confidence flag is deprecated but still accepted",
			args:       []string{"trends", "owner/repo", "--confidence=0.99"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo"},
		},
		{
			name:       "trends subcommand after global flags",
			args:       []string{"--clear-cache", "trends", "owner/repo"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", clearCache: true},
		},
		{
			name:       "trends subcommand after flag with days",
			args:       []string{"--clear-cache", "trends", "owner/repo", "--days=7"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", clearCache: true, trendsDays: 7},
		},
		{
			name:       "--margin flag",
			args:       []string{"trends", "owner/repo", "--margin=0.05"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", trendsMargin: 0.05},
		},
		{
			name:       "--margin=1.5 returns error",
			args:       []string{"trends", "owner/repo", "--margin=1.5"},
			isTerminal: false,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args, tt.isTerminal)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !slicesEqual(got.urls, tt.want.urls) {
				t.Errorf("urls = %v, want %v", got.urls, tt.want.urls)
			}
			if got.perfettoFile != tt.want.perfettoFile {
				t.Errorf("perfettoFile = %q, want %q", got.perfettoFile, tt.want.perfettoFile)
			}
			if got.openInPerfetto != tt.want.openInPerfetto {
				t.Errorf("openInPerfetto = %v, want %v", got.openInPerfetto, tt.want.openInPerfetto)
			}
			if got.openInOTel != tt.want.openInOTel {
				t.Errorf("openInOTel = %v, want %v", got.openInOTel, tt.want.openInOTel)
			}
			if got.otelEndpoint != tt.want.otelEndpoint {
				t.Errorf("otelEndpoint = %q, want %q", got.otelEndpoint, tt.want.otelEndpoint)
			}
			if got.otelStdout != tt.want.otelStdout {
				t.Errorf("otelStdout = %v, want %v", got.otelStdout, tt.want.otelStdout)
			}
			if got.otelGRPCEndpoint != tt.want.otelGRPCEndpoint {
				t.Errorf("otelGRPCEndpoint = %q, want %q", got.otelGRPCEndpoint, tt.want.otelGRPCEndpoint)
			}
			if got.tuiMode != tt.want.tuiMode {
				t.Errorf("tuiMode = %v, want %v", got.tuiMode, tt.want.tuiMode)
			}
			if got.clearCache != tt.want.clearCache {
				t.Errorf("clearCache = %v, want %v", got.clearCache, tt.want.clearCache)
			}
			if got.window != tt.want.window {
				t.Errorf("window = %v, want %v", got.window, tt.want.window)
			}
			if got.showHelp != tt.want.showHelp {
				t.Errorf("showHelp = %v, want %v", got.showHelp, tt.want.showHelp)
			}
			if got.showVersion != tt.want.showVersion {
				t.Errorf("showVersion = %v, want %v", got.showVersion, tt.want.showVersion)
			}
			if got.outputFormat != tt.want.outputFormat {
				t.Errorf("outputFormat = %q, want %q", got.outputFormat, tt.want.outputFormat)
			}
			if got.trendsNoSample != tt.want.trendsNoSample {
				t.Errorf("trendsNoSample = %v, want %v", got.trendsNoSample, tt.want.trendsNoSample)
			}
			if tt.want.trendsMargin != 0 && got.trendsMargin != tt.want.trendsMargin {
				t.Errorf("trendsMargin = %v, want %v", got.trendsMargin, tt.want.trendsMargin)
			}
		})
	}
}

func TestParseArgsDays(t *testing.T) {
	tests := []struct {
		name     string
		arg      string
		wantDays int
		wantErr  bool
	}{
		{name: "valid days", arg: "--days=7", wantDays: 7},
		{name: "trailing garbage rejected", arg: "--days=7days", wantErr: true},
		{name: "trailing letter rejected", arg: "--days=30x", wantErr: true},
		{name: "zero rejected", arg: "--days=0", wantErr: true},
		{name: "negative rejected", arg: "--days=-5", wantErr: true},
		{name: "non-numeric rejected", arg: "--days=abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs([]string{"trends", "owner/repo", tt.arg}, false)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %s, got nil (days=%d)", tt.arg, got.trendsDays)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.trendsDays != tt.wantDays {
				t.Errorf("trendsDays = %d, want %d", got.trendsDays, tt.wantDays)
			}
		})
	}
}

func TestParseTrendsRepo(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		{name: "owner/repo", input: "owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{name: "trailing slash", input: "owner/repo/", wantOwner: "owner", wantRepo: "repo"},
		{name: "full https URL", input: "https://github.com/owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{name: "full URL with trailing slash", input: "https://github.com/owner/repo/", wantOwner: "owner", wantRepo: "repo"},
		{name: "github.com prefix", input: "github.com/owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{name: "www URL", input: "https://www.github.com/owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{name: "owner only", input: "owner", wantErr: true},
		{name: "empty", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, err := parseTrendsRepo(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got owner=%q repo=%q", owner, repo)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if owner != tt.wantOwner || repo != tt.wantRepo {
				t.Errorf("got %q/%q, want %q/%q", owner, repo, tt.wantOwner, tt.wantRepo)
			}
		})
	}
}

func TestLooksLikeGitHubToken(t *testing.T) {
	tests := []struct {
		arg  string
		want bool
	}{
		{"ghp_abc123XYZ", true},
		{"gho_abc123", true},
		{"ghu_abc123", true},
		{"ghs_abc123", true},
		{"ghr_abc123", true},
		{"github_pat_11ABC", true},
		{"0123456789abcdef0123456789abcdef01234567", true}, // legacy 40-hex PAT
		{"owner/repo", false},
		{"https://github.com/owner/repo/pull/123", false},
		{"not-a-token", false},
		{"0123456789abcdef0123456789abcdef0123456", false},  // 39 chars
		{"0123456789abcdef0123456789abcdef0123456g", false}, // non-hex
	}

	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			if got := looksLikeGitHubToken(tt.arg); got != tt.want {
				t.Errorf("looksLikeGitHubToken(%q) = %v, want %v", tt.arg, got, tt.want)
			}
		})
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHasOtherWork(t *testing.T) {
	tests := []struct {
		name string
		cfg  config
		want bool
	}{
		{"nothing else", config{}, false},
		{"urls", config{urls: []string{"u"}}, true},
		{"trace file", config{traceFiles: []string{"t.json"}}, true},
		{"trends", config{trendsMode: true}, true},
		{"diff", config{diffMode: true}, true},
		{"sync", config{syncMode: true}, true},
		{"listen", config{listenAddr: ":4318"}, true},
		{"tempo backend", config{tempoURL: "http://t"}, true},
		{"jaeger backend", config{jaegerURL: "http://j"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasOtherWork(tt.cfg); got != tt.want {
				t.Errorf("hasOtherWork(%s) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestListenRejectsOtherInputs(t *testing.T) {
	cases := [][]string{
		{"--listen", "trace.json"},
		{"--listen", "https://github.com/o/r/pull/1"},
		{"--listen", "--tempo=http://t", "--trace-id=abc"},
	}
	for _, args := range cases {
		if _, err := parseArgs(args, false); err == nil {
			t.Errorf("parseArgs(%v) should reject --listen combined with other inputs", args)
		}
	}
	if _, err := parseArgs([]string{"--listen"}, false); err != nil {
		t.Errorf("plain --listen should parse: %v", err)
	}
}

func TestOutputFormatModeMismatch(t *testing.T) {
	if _, err := parseArgs([]string{"trends", "o/r", "--output=json"}, false); err == nil {
		t.Error("trends + --output should error (trends uses --format)")
	}
	if _, err := parseArgs([]string{"trace.json", "--format=json"}, false); err == nil {
		t.Error("--format outside trends mode should error (analysis uses --output)")
	}
	if _, err := parseArgs([]string{"trends", "o/r", "--format=json"}, false); err != nil {
		t.Errorf("trends + --format should parse: %v", err)
	}
	if _, err := parseArgs([]string{"trace.json", "--output=json"}, false); err != nil {
		t.Errorf("analysis + --output should parse: %v", err)
	}
}
