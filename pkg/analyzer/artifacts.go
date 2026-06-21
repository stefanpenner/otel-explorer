package analyzer

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// IngestTraceArtifacts downloads artifacts matching "gha-trace.*" from a workflow run,
// parses them as OTLP JSON or Chrome trace format, and adds spans to the builder.
// Artifact root spans are re-parented under parentSC so they appear as children of the workflow.
// Returns the full artifact list so callers can surface metadata without an extra API call.
func IngestTraceArtifacts(ctx context.Context, client githubapi.GitHubProvider, owner, repo string, runID int64, builder *SpanBuilder, urlIndex int, parentSC oteltrace.SpanContext) ([]githubapi.Artifact, error) {
	artifacts, err := client.ListArtifacts(ctx, owner, repo, runID)
	if err != nil {
		return nil, nil // best-effort
	}

	for _, artifact := range artifacts {
		if artifact.Expired {
			continue
		}
		if !strings.HasPrefix(artifact.Name, "gha-trace") {
			continue
		}

		data, err := client.DownloadArtifact(ctx, artifact.ArchiveDownloadURL)
		if err != nil {
			continue // best-effort
		}

		spans, err := extractSpansFromZip(data)
		if err != nil {
			continue // best-effort
		}

		// Build a set of span IDs in this artifact to identify root spans
		spanIDs := make(map[oteltrace.SpanID]bool)
		for _, s := range spans {
			spanIDs[s.SpanContext().SpanID()] = true
		}

		// Convert ReadOnlySpans to SpanStubs with url_index and artifact tagging
		for _, s := range spans {
			builder.Add(convertArtifactSpan(s, parentSC, spanIDs, urlIndex, artifact))
		}
	}

	return artifacts, nil
}

func convertArtifactSpan(s sdktrace.ReadOnlySpan, parentSC oteltrace.SpanContext, spanIDs map[oteltrace.SpanID]bool, urlIndex int, artifact githubapi.Artifact) tracetest.SpanStub {
	parent := s.Parent()
	if !parent.SpanID().IsValid() || !spanIDs[parent.SpanID()] {
		parent = parentSC
	}

	original := s.Attributes()
	attrs := make([]attribute.KeyValue, len(original), len(original)+3)
	copy(attrs, original)
	attrs = append(attrs,
		attribute.Int("github.url_index", urlIndex),
		attribute.String("github.artifact_name", artifact.Name),
		attribute.String("github.artifact.download_url", artifact.ArchiveDownloadURL),
	)

	return tracetest.SpanStub{
		Name:        s.Name(),
		SpanContext: s.SpanContext(),
		Parent:      parent,
		SpanKind:    s.SpanKind(),
		StartTime:   s.StartTime(),
		EndTime:     s.EndTime(),
		Attributes:  attrs,
		Events:      s.Events(),
		Links:       s.Links(),
		Status:      s.Status(),
	}
}

// extractSpansFromZip unzips artifact data and parses any JSON files as OTLP or Chrome traces.
func extractSpansFromZip(data []byte) ([]sdktrace.ReadOnlySpan, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open zip: %w", err)
	}

	var allSpans []sdktrace.ReadOnlySpan
	for _, f := range reader.File {
		if !strings.HasSuffix(f.Name, ".json") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			continue
		}

		spans, err := otlpfile.Parse(rc)
		rc.Close()
		if err != nil || len(spans) == 0 {
			continue
		}
		allSpans = append(allSpans, spans...)
	}

	return allSpans, nil
}
