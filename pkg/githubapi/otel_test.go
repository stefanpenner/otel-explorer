package githubapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestClientInstrumentation(t *testing.T) {
	// Setup tracetest
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "ok"}`))
	}))
	defer server.Close()

	client := NewClient(NewContext("test-token"), WithCacheDir(""))

	ctx := context.Background()

	// Call a method that uses the tracer directly
	_, _ = client.FetchRepository(ctx, server.URL)

	// Verify spans
	spans := exporter.GetSpans()
	assert.NotEmpty(t, spans, "Should have at least one span from tracer.Start")

	foundFetch := false
	foundHTTP := false
	for _, span := range spans {
		if span.Name == "FetchRepository" {
			foundFetch = true
		}
		// otelhttp default span name is the method or "HTTP GET" depending on version/config
		if span.Attributes != nil {
			for _, attr := range span.Attributes {
				if attr.Key == "http.method" {
					foundHTTP = true
				}
			}
		}
		if span.Name == "HTTP GET" || span.Name == "GET" {
			foundHTTP = true
		}
	}
	assert.True(t, foundFetch, "Should have a FetchRepository span")
	assert.True(t, foundHTTP, "Should have an HTTP span from otelhttp")
}
