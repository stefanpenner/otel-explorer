package polling

import (
	"context"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type PollingIngestor struct {
	client   *githubapi.Client
	urls     []string
	reporter analyzer.ProgressReporter
	opts     analyzer.AnalyzeOptions
}

func NewPollingIngestor(client *githubapi.Client, urls []string, reporter analyzer.ProgressReporter, opts analyzer.AnalyzeOptions) *PollingIngestor {
	return &PollingIngestor{
		client:   client,
		urls:     urls,
		reporter: reporter,
		opts:     opts,
	}
}

func (i *PollingIngestor) Ingest(ctx context.Context) ([]analyzer.URLResult, int64, int64, []sdktrace.ReadOnlySpan, error) {
	results, _, globalEarliest, globalLatest, spans, errs := analyzer.AnalyzeURLs(ctx, i.urls, i.client, i.reporter, i.opts)
	var err error
	if len(errs) > 0 {
		err = errs[0]
	}
	return results, globalEarliest, globalLatest, spans, err
}
