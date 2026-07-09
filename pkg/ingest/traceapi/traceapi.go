// Package traceapi fetches traces from trace backends
// (Tempo, Jaeger) via their HTTP APIs and returns ReadOnlySpans.
//
// Supported backends:
//   - Grafana Tempo: GET /api/traces/{traceID} (OTLP JSON)
//   - Jaeger:        GET /api/traces/{traceID} (Jaeger JSON, auto-detected)
package traceapi

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
)

func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(s) > 0
}

// defaultMaxTraceBytes caps trace backend (Tempo/Jaeger) responses so a
// hostile or compromised backend cannot OOM the process. 256MB matches a
// large but realistic trace; the receiver/webhook paths use the same pattern.
const defaultMaxTraceBytes = 256 * 1024 * 1024

// Client fetches traces from a trace backend HTTP API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	maxBytes   int64
}

// New creates a Client for the given backend base URL.
// The baseURL should not include a trailing slash or path
// (e.g. "http://localhost:3200" for Tempo).
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		maxBytes: defaultMaxTraceBytes,
	}
}

// FetchTrace retrieves a trace by its ID and returns parsed ReadOnlySpans.
// Auto-detects the response format (OTLP JSON or Jaeger JSON).
func (c *Client) FetchTrace(traceID string) ([]sdktrace.ReadOnlySpan, error) {
	if !isHexString(traceID) {
		return nil, fmt.Errorf("invalid trace ID %q: must be non-empty hex string", traceID)
	}
	url := fmt.Sprintf("%s/api/traces/%s", c.baseURL, traceID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch trace %s: %w", traceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, c.maxBytes+1))
		return nil, fmt.Errorf("fetch trace %s: HTTP %d: %s", traceID, resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read trace %s response: %w", traceID, err)
	}
	if int64(len(body)) > c.maxBytes {
		return nil, fmt.Errorf("fetch trace %s: response exceeds %d bytes", traceID, c.maxBytes)
	}

	// Tempo's /api/traces returns OTLP under a top-level "batches" key (tempopb.Trace)
	// rather than "resourceSpans"; the contents are otherwise identical OTLP/JSON.
	if bytes.Contains(body, []byte(`"batches"`)) && !bytes.Contains(body, []byte(`"resourceSpans"`)) {
		body = bytes.Replace(body, []byte(`"batches"`), []byte(`"resourceSpans"`), 1)
	}

	// Auto-detect format: Jaeger responses have "data" key, OTLP has "resourceSpans"
	if bytes.Contains(body, []byte(`"resourceSpans"`)) {
		spans, err := otlpfile.ParseProto(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("parse trace %s (OTLP): %w", traceID, err)
		}
		return spans, nil
	}

	spans, err := otlpfile.ParseJaeger(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse trace %s (Jaeger): %w", traceID, err)
	}
	return spans, nil
}
