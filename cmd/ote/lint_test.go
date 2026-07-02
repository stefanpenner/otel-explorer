package main

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestBuildLintDataKindAndIntAttrs(t *testing.T) {
	stub := tracetest.SpanStub{
		Name:      "GET",
		SpanKind:  trace.SpanKindClient,
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Second),
		Attributes: []attribute.KeyValue{
			attribute.String("http.request.method", "GET"),
			attribute.Int64("http.status_code", 500), // deprecated + int-typed
		},
	}
	data := buildLintData([]sdktrace.ReadOnlySpan{stub.Snapshot()})
	if len(data) != 1 {
		t.Fatalf("got %d SpanData, want 1", len(data))
	}
	if data[0].SpanKind != "CLIENT" {
		t.Errorf("SpanKind = %q, want CLIENT (lint's kind-gated checks compare uppercase)", data[0].SpanKind)
	}
	if data[0].Attrs["http.status_code"] != "500" {
		t.Errorf("int attr = %q, want \"500\" (AsString drops int values, hiding deprecation warnings)", data[0].Attrs["http.status_code"])
	}
}
