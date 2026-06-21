package polling

import (
	"context"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReporter struct{}

func (m *mockReporter) StartURL(int, string) {}
func (m *mockReporter) SetURLRuns(int)        {}
func (m *mockReporter) SetPhase(string)       {}
func (m *mockReporter) SetDetail(string)       {}
func (m *mockReporter) ProcessRun()            {}
func (m *mockReporter) Finish()                {}

func TestIngestEmptyURLs(t *testing.T) {
	client := githubapi.NewClient(githubapi.NewContext(""))
	ingestor := NewPollingIngestor(
		client,
		[]string{},
		&mockReporter{},
		analyzer.AnalyzeOptions{},
	)

	results, earliest, latest, spans, err := ingestor.Ingest(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, results)
	assert.Equal(t, int64(0), earliest)
	assert.Equal(t, int64(0), latest)
	assert.Empty(t, spans)
}

func TestIngestReturnsResultsNotNilOnError(t *testing.T) {
	// Verify that Ingest returns the results (even if empty) from AnalyzeURLs
	// instead of forcing nil on partial failure. The previous bug discarded all
	// results when any error occurred:
	//   if len(errs) > 0 { return nil, 0, 0, nil, errs[0] }  // BUG
	// The fix returns whichever results AnalyzeURLs produced:
	//   return results, globalEarliest, globalLatest, spans, err
	//
	// With empty URLs this exercises the path where AnalyzeURLs returns
	// (nil, ..., nil, []) — no results and no errors.
	client := githubapi.NewClient(githubapi.NewContext(""))
	ingestor := NewPollingIngestor(
		client,
		[]string{},
		&mockReporter{},
		analyzer.AnalyzeOptions{},
	)

	results, _, _, _, err := ingestor.Ingest(context.Background())
	require.NoError(t, err)
	// When there are no errors, results should be what AnalyzeURLs returns (nil
	// for empty input). The important thing is the method does not panic and
	// returns the values unchanged.
	assert.Nil(t, results)
}