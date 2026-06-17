package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/export"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostSlackWebhook(t *testing.T) {
	var gotBody []byte
	var gotContentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	rep := &export.Report{
		SchemaVersion: export.SchemaVersion, Kind: export.KindRunAnalysis,
		Meta: export.Meta{Tool: "ote", Repo: "o/r"},
		Run: &export.RunReport{
			Summary: export.RunSummary{TotalRuns: 1, TotalJobs: 2, FailedJobs: 1},
			Runs:    []export.Run{{Jobs: []export.Job{{Name: "test", Conclusion: "failure"}}}},
		},
	}

	require.NoError(t, postSlackWebhook(rep, server.URL))

	assert.Equal(t, "application/json", gotContentType)
	var msg map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &msg), "posted a valid Block Kit JSON body")
	assert.Contains(t, msg, "blocks")
	assert.Contains(t, msg["text"], "CI Run Analysis")
}

func TestPostSlackWebhook_NonOKStatusIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid_payload"))
	}))
	defer server.Close()

	rep := &export.Report{Kind: export.KindRunAnalysis, Run: &export.RunReport{}}
	err := postSlackWebhook(rep, server.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_payload")
}

func TestResolveSlackWebhook_EnvFallback(t *testing.T) {
	t.Setenv("SLACK_WEBHOOK_URL", "https://env.example/hook")
	assert.Equal(t, "https://flag.example/hook", resolveSlackWebhook("https://flag.example/hook"), "flag wins")
	assert.Equal(t, "https://env.example/hook", resolveSlackWebhook(""), "env is the fallback")
}
