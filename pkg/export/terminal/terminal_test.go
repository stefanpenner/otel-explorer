package terminal

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewExporter_UsesProvidedWriter(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	e := NewExporter(&buf, &enrichment.CICDEnricher{})
	assert.NotNil(t, e.writer)
	assert.NotEqual(t, io.Discard, e.writer)
}

func TestExporter_Finish_WritesToProvidedWriter(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	e := NewExporter(&buf, &enrichment.CICDEnricher{})

	// Export a minimal span stub so Finish produces output
	stubs := tracetest.SpanStubs{
		{Name: "test-span"},
	}.Snapshots()
	require.NoError(t, e.Export(context.Background(), stubs))
	require.NoError(t, e.Finish(context.Background()))

	output := buf.String()
	assert.Contains(t, output, "Trace Performance Report")
	assert.NotEmpty(t, output, "writer must receive output, not io.Discard")
}

func TestExporter_Finish_ZeroRuns_NoNaN(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	e := NewExporter(&buf, &enrichment.CICDEnricher{})

	// Export spans that produce zero TotalRuns (no GHA attributes)
	stubs := tracetest.SpanStubs{
		{Name: "non-run-span"},
	}.Snapshots()
	require.NoError(t, e.Export(context.Background(), stubs))
	require.NoError(t, e.Finish(context.Background()))

	output := buf.String()
	assert.NotContains(t, output, "NaN", "must not produce NaN for zero TotalRuns")
}
