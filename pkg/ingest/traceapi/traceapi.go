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

// Client fetches traces from a trace backend HTTP API.
type Client struct {
	baseURL    string
	httpClient *http.Client
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
	}
}

// FetchTrace retrieves a trace by its ID and returns parsed ReadOnlySpans.
// Auto-detects the response format (OTLP JSON or Jaeger JSON).
func (c *Client) FetchTrace(traceID string) ([]sdktrace.ReadOnlySpan, error) {
	url := fmt.Sprintf("%s/api/traces/%s", c.baseURL, traceID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/protobuf")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch trace %s: %w", traceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("fetch trace %s: HTTP %d: %s", traceID, resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read trace %s response: %w", traceID, err)
	}

	contentType := resp.Header.Get("Content-Type")

	// Protobuf response (Tempo, OTLP-native backends)
	if strings.Contains(contentType, "application/protobuf") || strings.Contains(contentType, "application/x-protobuf") {
		spans, err := otlpfile.ParseRawProtobuf(body)
		if err != nil {
			return nil, fmt.Errorf("parse trace %s (protobuf): %w", traceID, err)
		}
		return spans, nil
	}

	// JSON: auto-detect format
	// Tempo wraps the same structure under "batches" instead of "resourceSpans"
	if bytes.Contains(body, []byte(`"batches"`)) {
		body = bytes.Replace(body, []byte(`"batches"`), []byte(`"resourceSpans"`), 1)
	}
	if bytes.Contains(body, []byte(`"resourceSpans"`)) {
		spans, err := otlpfile.ParseProto(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("parse trace %s (OTLP JSON): %w", traceID, err)
		}
		return spans, nil
	}

	spans, err := otlpfile.ParseJaeger(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse trace %s (Jaeger): %w", traceID, err)
	}
	return spans, nil
}
