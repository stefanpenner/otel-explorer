package results

import (
	"fmt"
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	logFetchTestJob1 = int64(456)
	logFetchTestJob2 = int64(789)
)

// logFetchTestSpans builds workflow -> 2 jobs spans with real github.job_id
// attributes so fetchLogsForCurrentItem can resolve fetch params.
func logFetchTestSpans(base time.Time) []sdktrace.ReadOnlySpan {
	tid := githubapi.NewTraceID(9, 1)
	sc := func(id int64) trace.SpanContext {
		return trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(id), TraceFlags: trace.FlagsSampled,
		})
	}
	wf, j1, j2 := sc(1), sc(2), sc(3)
	builder := &analyzer.SpanBuilder{}
	builder.Add(tracetest.SpanStub{
		Name: "Workflow: CI", SpanContext: wf,
		StartTime: base, EndTime: base.Add(10 * time.Minute),
		Attributes: []attribute.KeyValue{
			attribute.String("type", "workflow"),
			attribute.String("vcs.repository.url.full", "https://github.com/test/repo"),
		},
	})
	builder.Add(tracetest.SpanStub{
		Name: "build", SpanContext: j1, Parent: wf,
		StartTime: base, EndTime: base.Add(5 * time.Minute),
		Attributes: []attribute.KeyValue{
			attribute.String("type", "job"),
			attribute.Int64("github.job_id", logFetchTestJob1),
		},
	})
	builder.Add(tracetest.SpanStub{
		Name: "test", SpanContext: j2, Parent: wf,
		StartTime: base.Add(5 * time.Minute), EndTime: base.Add(10 * time.Minute),
		Attributes: []attribute.KeyValue{
			attribute.String("type", "job"),
			attribute.Int64("github.job_id", logFetchTestJob2),
		},
	})
	return builder.Spans()
}

// logSpanNamed builds a single log span (result of a fetch) with the given name.
func logSpanNamed(name string, base time.Time) []sdktrace.ReadOnlySpan {
	tid := githubapi.NewTraceID(9, 1)
	builder := &analyzer.SpanBuilder{}
	builder.Add(tracetest.SpanStub{
		Name: name,
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: githubapi.NewSpanID(100), TraceFlags: trace.FlagsSampled,
		}),
		Parent:    trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: githubapi.NewSpanID(2), TraceFlags: trace.FlagsSampled}),
		StartTime: base, EndTime: base.Add(time.Minute),
		Attributes: []attribute.KeyValue{attribute.String("type", "step")},
	})
	return builder.Spans()
}

func hasSpanNamed(spans []sdktrace.ReadOnlySpan, name string) bool {
	for _, s := range spans {
		if s.Name() == name {
			return true
		}
	}
	return false
}

// moveCursorToJob expands the tree and positions the cursor on the visible
// item for the given job span.
func moveCursorToJob(t *testing.T, m *Model, jobSpanID string) {
	t.Helper()
	m.expandAll()
	for i, it := range m.visibleItems {
		if it.SpanID == jobSpanID {
			m.cursor = i
			return
		}
	}
	t.Fatalf("no visible item with span ID %s", jobSpanID)
}

// newLogFetchTestModel builds a model whose spans support real log-fetch
// param resolution, with fetchFunc wired in.
func newLogFetchTestModel(base time.Time, fetchFunc LogFetchFunc) Model {
	spans := logFetchTestSpans(base)
	return NewModel(spans, base, base.Add(10*time.Minute),
		[]string{"https://github.com/test/repo/pull/1"},
		nil, nil, enrichment.DefaultEnricher(), WithLogFetchFunc(fetchFunc))
}

// TestTuiReloadSpec_StaleFetchResultDiscarded encodes the TLC witness trace
// for specs/tui-reload finding #3 (MCFinding1 / NoStaleLogSpans):
//
//  1. PressFetch(j1)      — fetch j1 starts against base gen 0
//  2. PressReload + done  — ReloadResultMsg replaces spans (gen 1),
//     resets fetch state
//  3. PressFetch(j1)      — NEW fetch of j1 against base gen 1
//  4. FetchDeliverOK      — the STALE gen-0 result arrives first and must
//     be DISCARDED, not attributed to the new fetch
//  5. the fresh gen-1 result arrives and is accepted
func TestTuiReloadSpec_StaleFetchResultDiscarded(t *testing.T) {
	t.Parallel()

	base := time.Now()
	j1SpanID := githubapi.NewSpanID(2).String()

	calls := 0
	fetchFunc := func(owner, repo string, jobID int64, existing []sdktrace.ReadOnlySpan) ([]sdktrace.ReadOnlySpan, error) {
		calls++
		return logSpanNamed(fmt.Sprintf("log-%d", calls), base), nil
	}

	m := newLogFetchTestModel(base, fetchFunc)

	// 1. fetch j1 starts against the gen-0 span set
	moveCursorToJob(t, &m, j1SpanID)
	staleCmd := m.fetchLogsForCurrentItem()
	require.NotNil(t, staleCmd, "first fetch must start")
	require.Equal(t, logFetchTestJob1, m.logFetchingJobID)

	// 2. reload completes successfully: fresh spans, fetch state reset
	nm, _ := m.Update(ReloadResultMsg{
		spans:       logFetchTestSpans(base),
		globalStart: base,
		globalEnd:   base.Add(10 * time.Minute),
	})
	m = nm.(Model)
	require.Equal(t, int64(0), m.logFetchingJobID, "reload resets fetch state")

	// 3. NEW fetch of j1 against the gen-1 span set
	moveCursorToJob(t, &m, j1SpanID)
	freshCmd := m.fetchLogsForCurrentItem()
	require.NotNil(t, freshCmd, "re-fetch after reload must start")

	// 4. the STALE result (computed against gen-0 spans) arrives first
	staleMsg := staleCmd()
	nm, _ = m.Update(staleMsg)
	m = nm.(Model)

	assert.False(t, hasSpanNamed(m.spans, "log-1"),
		"stale gen-0 log spans must not attach to gen-1 data")
	assert.False(t, m.logFetchedJobIDs[logFetchTestJob1],
		"stale result must not mark the job fetched")
	assert.Equal(t, logFetchTestJob1, m.logFetchingJobID,
		"stale result must not clear the in-flight fetch")

	// 5. the fresh result arrives and is accepted
	freshMsg := freshCmd()
	nm, _ = m.Update(freshMsg)
	m = nm.(Model)

	assert.True(t, hasSpanNamed(m.spans, "log-2"), "fresh result accepted")
	assert.True(t, m.logFetchedJobIDs[logFetchTestJob1])
	assert.Equal(t, int64(0), m.logFetchingJobID)
}

// TestTuiReloadSpec_StaleFetchCrossJob encodes the cross-job variant from
// the spec README (FetchedJobsHaveTheirSpans): a stale j1 result must not
// mark j2 as fetched nor consume j2's in-flight fetch slot.
func TestTuiReloadSpec_StaleFetchCrossJob(t *testing.T) {
	t.Parallel()

	base := time.Now()
	j1SpanID := githubapi.NewSpanID(2).String()
	j2SpanID := githubapi.NewSpanID(3).String()

	calls := 0
	fetchFunc := func(owner, repo string, jobID int64, existing []sdktrace.ReadOnlySpan) ([]sdktrace.ReadOnlySpan, error) {
		calls++
		return logSpanNamed(fmt.Sprintf("log-%d", calls), base), nil
	}

	m := newLogFetchTestModel(base, fetchFunc)

	moveCursorToJob(t, &m, j1SpanID)
	staleCmd := m.fetchLogsForCurrentItem()
	require.NotNil(t, staleCmd)

	nm, _ := m.Update(ReloadResultMsg{
		spans:       logFetchTestSpans(base),
		globalStart: base,
		globalEnd:   base.Add(10 * time.Minute),
	})
	m = nm.(Model)

	// new fetch is for j2 this time
	moveCursorToJob(t, &m, j2SpanID)
	j2Cmd := m.fetchLogsForCurrentItem()
	require.NotNil(t, j2Cmd)
	require.Equal(t, logFetchTestJob2, m.logFetchingJobID)

	// stale j1 result arrives: must not be attributed to j2
	nm, _ = m.Update(staleCmd())
	m = nm.(Model)

	assert.False(t, m.logFetchedJobIDs[logFetchTestJob2],
		"stale j1 result must not mark j2 fetched")
	assert.Equal(t, logFetchTestJob2, m.logFetchingJobID,
		"stale j1 result must not clear j2's in-flight fetch")

	// j2's real result is still deliverable
	nm, _ = m.Update(j2Cmd())
	m = nm.(Model)
	assert.True(t, m.logFetchedJobIDs[logFetchTestJob2])
}

// TestTuiReloadSpec_StaleFetchErrorDiscarded encodes the fetch-error variant
// from the spec README: a stale FAILED result must not clear
// logFetchingJobID (which would silently drop the new fetch's real result)
// and must not surface an error for a superseded fetch.
func TestTuiReloadSpec_StaleFetchErrorDiscarded(t *testing.T) {
	t.Parallel()

	base := time.Now()
	j1SpanID := githubapi.NewSpanID(2).String()

	calls := 0
	fetchFunc := func(owner, repo string, jobID int64, existing []sdktrace.ReadOnlySpan) ([]sdktrace.ReadOnlySpan, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("boom")
		}
		return logSpanNamed(fmt.Sprintf("log-%d", calls), base), nil
	}

	m := newLogFetchTestModel(base, fetchFunc)

	moveCursorToJob(t, &m, j1SpanID)
	staleCmd := m.fetchLogsForCurrentItem()
	require.NotNil(t, staleCmd)

	nm, _ := m.Update(ReloadResultMsg{
		spans:       logFetchTestSpans(base),
		globalStart: base,
		globalEnd:   base.Add(10 * time.Minute),
	})
	m = nm.(Model)

	moveCursorToJob(t, &m, j1SpanID)
	freshCmd := m.fetchLogsForCurrentItem()
	require.NotNil(t, freshCmd)

	// stale FAILED result arrives
	nm, _ = m.Update(staleCmd())
	m = nm.(Model)

	assert.Equal(t, logFetchTestJob1, m.logFetchingJobID,
		"stale error must not clear the in-flight fetch")
	assert.Empty(t, m.reloadError, "stale error must not surface")

	// fresh result still lands
	nm, _ = m.Update(freshCmd())
	m = nm.(Model)
	assert.True(t, m.logFetchedJobIDs[logFetchTestJob1])
	assert.True(t, hasSpanNamed(m.spans, "log-2"))
}
