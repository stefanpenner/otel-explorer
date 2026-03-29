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
			name:       "--no-sample flag in trends mode",
			args:       []string{"trends", "owner/repo", "--no-sample"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", trendsNoSample: true},
		},
		{
			name:       "--confidence flag",
			args:       []string{"trends", "owner/repo", "--confidence=0.99"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", trendsConfidence: 0.99},
		},
		{
			name:       "--margin flag",
			args:       []string{"trends", "owner/repo", "--margin=0.05"},
			isTerminal: false,
			want:       config{trendsMode: true, trendsRepo: "owner/repo", trendsMargin: 0.05},
		},
		{
			name:       "--confidence=0 returns error",
			args:       []string{"trends", "owner/repo", "--confidence=0"},
			isTerminal: false,
			wantErr:    true,
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
			if got.outputFormat != tt.want.outputFormat {
				t.Errorf("outputFormat = %q, want %q", got.outputFormat, tt.want.outputFormat)
			}
			if got.trendsNoSample != tt.want.trendsNoSample {
				t.Errorf("trendsNoSample = %v, want %v", got.trendsNoSample, tt.want.trendsNoSample)
			}
			if tt.want.trendsConfidence != 0 && got.trendsConfidence != tt.want.trendsConfidence {
				t.Errorf("trendsConfidence = %v, want %v", got.trendsConfidence, tt.want.trendsConfidence)
			}
			if tt.want.trendsMargin != 0 && got.trendsMargin != tt.want.trendsMargin {
				t.Errorf("trendsMargin = %v, want %v", got.trendsMargin, tt.want.trendsMargin)
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
