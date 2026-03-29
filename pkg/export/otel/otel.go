package otel

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type Exporter struct {
	exporter sdktrace.SpanExporter
}

func NewExporter(ctx context.Context, endpoint string) (*Exporter, error) {
	// Using OTLP/HTTP as it's more compatible with otel-desktop-viewer by default
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create OTLP exporter")
	}

	return &Exporter{exporter: exporter}, nil
}

func NewStdoutExporter(w io.Writer) (*Exporter, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(w))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create stdout trace exporter")
	}
	return &Exporter{exporter: exporter}, nil
}

func NewGRPCExporter(ctx context.Context, endpoint string) (*Exporter, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create OTLP gRPC exporter")
	}
	return &Exporter{exporter: exporter}, nil
}

func (e *Exporter) Export(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return e.exporter.ExportSpans(ctx, spans)
}

func (e *Exporter) Finish(ctx context.Context) error {
	return e.exporter.Shutdown(ctx)
}

// GetResource returns a standard resource for OTel analyzer
func GetResource(ctx context.Context) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("ote"),
		),
	)
}
