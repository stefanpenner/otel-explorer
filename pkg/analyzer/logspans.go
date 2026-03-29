package analyzer

import (
	"context"
	"fmt"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/logparse"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// IngestStepLogs fetches job logs, parses them with the log parser registry,
// and adds sub-step spans to the builder as children of existing step spans.
func IngestStepLogs(
	ctx context.Context,
	client githubapi.GitHubProvider,
	owner, repo string,
	jobs []githubapi.Job,
	builder *SpanBuilder,
	urlIndex int,
	traceID oteltrace.TraceID,
	registry *logparse.Registry,
) error {
	if registry == nil {
		registry = logparse.DefaultRegistry()
	}

	for _, job := range jobs {
		logData, err := client.FetchJobLog(ctx, owner, repo, job.ID)
		if err != nil {
			continue // best-effort
		}

		stepLogs := githubapi.SplitJobLogByStep(logData, job.Steps)

		for _, step := range job.Steps {
			raw, ok := stepLogs[step.Number]
			if !ok || len(raw) == 0 {
				continue
			}

			lines := logparse.ParseLogLines(raw)
			if len(lines) == 0 {
				continue
			}

			stepStart, ok := utils.ParseTime(step.StartedAt)
			if !ok {
				continue
			}
			stepEnd, ok := utils.ParseTime(step.CompletedAt)
			if !ok {
				continue
			}

			parserName, spans := registry.Parse(lines, stepStart, stepEnd)
			if len(spans) == 0 {
				continue
			}

			// Parent is the step span
			stepSID := githubapi.NewSpanIDFromString(fmt.Sprintf("%d-%s", job.ID, step.Name))
			stepSC := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     stepSID,
				TraceFlags: oteltrace.FlagsSampled,
			})

			// Construct step URL for deep-linking to log lines
			stepURL := ""
			if job.HTMLURL != "" {
				stepURL = fmt.Sprintf("%s#step:%d:1", job.HTMLURL, step.Number)
			}

			addParsedSpans(builder, spans, stepSC, traceID, job.ID, step.Name, parserName, urlIndex, 0, stepURL)
		}
	}

	return nil
}

// AddParsedSpansToBuilder is the exported version of addParsedSpans for use by the TUI's
// on-demand log fetch feature. stepURL is the base URL for the step (e.g.
// "https://github.com/o/r/actions/runs/123/job/456#step:2:1") used to
// construct line-level deep links on derived log spans.
func AddParsedSpansToBuilder(builder *SpanBuilder, spans []logparse.ParsedSpan, parentSC oteltrace.SpanContext, traceID oteltrace.TraceID, jobID int64, stepName, parserName string, urlIndex, depth int, stepURL string) {
	addParsedSpans(builder, spans, parentSC, traceID, jobID, stepName, parserName, urlIndex, depth, stepURL)
}

// addParsedSpans recursively converts ParsedSpans to SpanStubs and adds them to the builder.
func addParsedSpans(builder *SpanBuilder, spans []logparse.ParsedSpan, parentSC oteltrace.SpanContext, traceID oteltrace.TraceID, jobID int64, stepName, parserName string, urlIndex, depth int, stepURL string) {
	for i, ps := range spans {
		spanID := githubapi.NewSpanIDFromString(fmt.Sprintf("%d-%s-%s-%d-%d", jobID, stepName, ps.Name, depth, i))
		sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: oteltrace.FlagsSampled,
		})

		attrs := []attribute.KeyValue{
			attribute.String("type", "log_span"),
			attribute.String("log.parser", parserName),
			attribute.Int("github.url_index", urlIndex),
		}

		// Build a deep-link URL to the log line if we have a step URL and line number
		if stepURL != "" {
			if lineNum, ok := ps.Attributes["log.line_number"]; ok {
				// stepURL format: .../job/456#step:N:1 — replace the trailing :1 with :lineNum
				url := stepURL
				if lastColon := strings.LastIndex(url, ":"); lastColon > 0 {
					url = url[:lastColon+1] + lineNum
				}
				attrs = append(attrs, attribute.String("github.url", url))
			}
		}

		for k, v := range ps.Attributes {
			attrs = append(attrs, attribute.String(k, v))
		}

		builder.Add(tracetest.SpanStub{
			Name:        ps.Name,
			SpanContext: sc,
			Parent:      parentSC,
			StartTime:   ps.StartTime,
			EndTime:     ps.EndTime,
			Attributes:  attrs,
		})

		if len(ps.Children) > 0 {
			addParsedSpans(builder, ps.Children, sc, traceID, jobID, stepName, parserName, urlIndex, depth+1, stepURL)
		}
	}
}
