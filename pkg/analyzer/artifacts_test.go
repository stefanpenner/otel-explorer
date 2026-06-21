package analyzer

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestConvertArtifactSpanDoesNotMutateOriginal(t *testing.T) {
	attrs := make([]attribute.KeyValue, 2, 5)
	attrs[0] = attribute.String("type", "workflow")
	attrs[1] = attribute.String("github.conclusion", "success")

	zeroTail := make([]attribute.KeyValue, 3)
	copy(zeroTail, attrs[2:5:cap(attrs)])

	tid := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	sid := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})

	stub := tracetest.SpanStub{
		Name:        "test-span",
		SpanContext: sc,
		StartTime:   time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		EndTime:     time.Date(2024, 1, 15, 10, 5, 0, 0, time.UTC),
		Attributes:  attrs,
	}

	snap := stub.Snapshot()

	originalCopy := make([]attribute.KeyValue, len(attrs))
	copy(originalCopy, attrs)

	spanIDs := map[trace.SpanID]bool{sid: true}
	artifact := githubapi.Artifact{
		Name:               "gha-trace-test",
		ArchiveDownloadURL: "https://example.com/download",
	}

	converted := convertArtifactSpan(snap, sc, spanIDs, 0, artifact)

	assert.Equal(t, originalCopy, snap.Attributes(), "original span attributes must not change")

	backingArray := attrs[:cap(attrs)]
	for i := len(originalCopy); i < cap(attrs); i++ {
		assert.Equal(t, zeroTail[i-len(originalCopy)], backingArray[i],
			"backing array at position %d was corrupted by append", i)
	}

	require.Len(t, converted.Attributes, 5)
	assert.Equal(t, "github.url_index", string(converted.Attributes[2].Key))
	assert.Equal(t, "github.artifact_name", string(converted.Attributes[3].Key))
	assert.Equal(t, "github.artifact.download_url", string(converted.Attributes[4].Key))
}
