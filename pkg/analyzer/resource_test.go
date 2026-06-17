package analyzer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Every synthesized span must carry service.name, or OTLP backends bucket it
// under "OTLPResourceNoServiceName".
func TestSpanBuilderStampsServiceName(t *testing.T) {
	var b SpanBuilder
	b.Add(tracetest.SpanStub{Name: "CI"}) // no resource set (the GHA path)

	spans := b.Spans()
	assert.Len(t, spans, 1)

	res := spans[0].Resource()
	assert.NotNil(t, res, "span must have a resource")

	got, ok := res.Set().Value("service.name")
	assert.True(t, ok, "service.name must be present")
	assert.Equal(t, "github-actions", got.AsString())
}

// Spans that already carry a resource (e.g. ingested non-GHA traces) keep it.
func TestSpanBuilderPreservesExistingResource(t *testing.T) {
	var b SpanBuilder
	custom := resource.NewSchemaless(attribute.String("service.name", "jenkins"))
	b.Add(tracetest.SpanStub{Name: "build", Resource: custom})

	spans := b.Spans()
	got, ok := spans[0].Resource().Set().Value("service.name")
	assert.True(t, ok)
	assert.Equal(t, "jenkins", got.AsString(), "existing resource must be preserved")
}
