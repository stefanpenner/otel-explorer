package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
)

// SyncStats reports what a sync did.
type SyncStats struct {
	RunsFetched int // runs returned by the listing API this sync
	JobsFetched int // runs whose job detail was fetched this sync
	JobsSkipped int // runs already holding job detail (no API cost)
}

// syncWorkers bounds concurrent job-detail requests, matching the trends
// fetcher: well inside GitHub's 100-concurrent / 900-points-min limits.
const syncWorkers = 8

// watermarkOverlap re-fetches a little history behind the watermark so runs
// that were still in progress during the previous sync get their final
// status and duration.
const watermarkOverlap = 24 * time.Hour

// Sync fetches runs (and the job detail the store doesn't already hold) for
// the last `days` and upserts them. Re-syncs are incremental: the run
// listing is bounded by the stored watermark, and job detail is fetched
// only for completed runs missing it.
func Sync(ctx context.Context, client githubapi.GitHubProvider, st *Store, owner, repo string, days int, progress func(string)) (SyncStats, error) {
	var stats SyncStats
	report := func(format string, args ...any) {
		if progress != nil {
			progress(fmt.Sprintf(format, args...))
		}
	}

	// Bound the listing window by the watermark — nothing older changes —
	// but only when the store already covers the requested depth. A deeper
	// request than ever synced is a backfill and must list the full window.
	fetchDays := days
	wm, err := st.Watermark(owner, repo)
	if err != nil {
		return stats, err
	}
	oldest, err := st.OldestRun(owner, repo)
	if err != nil {
		return stats, err
	}
	requestStart := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	if !wm.IsZero() && !oldest.IsZero() && oldest.Before(requestStart.Add(24*time.Hour)) {
		gap := time.Since(wm.Add(-watermarkOverlap))
		gapDays := int(gap/(24*time.Hour)) + 1
		if gapDays < fetchDays {
			fetchDays = gapDays
		}
	}

	report("listing runs (last %dd)", fetchDays)
	runs, err := client.FetchRecentWorkflowRuns(ctx, owner, repo, fetchDays, "", "", nil)
	if err != nil {
		return stats, fmt.Errorf("listing workflow runs: %w", err)
	}
	stats.RunsFetched = len(runs)
	byID := make(map[int64]githubapi.WorkflowRun, len(runs))
	for _, run := range runs {
		byID[run.ID] = run
	}
	if err := st.UpsertRuns(owner, repo, analyzer.ConvertRuns(runs)); err != nil {
		return stats, err
	}

	// Fetch job detail for completed runs in the requested window that the
	// store doesn't hold yet — including gaps left by older partial syncs.
	since := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	needJobs, err := st.RunsNeedingJobs(owner, repo, since, time.Now().Add(time.Hour))
	if err != nil {
		return stats, err
	}
	stats.JobsSkipped = stats.RunsFetched - len(needJobs)
	if stats.JobsSkipped < 0 {
		stats.JobsSkipped = 0
	}
	report("fetching job detail for %d runs", len(needJobs))

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, syncWorkers)
		fetched  atomic.Int64
		mu       sync.Mutex
		firstErr error
	)
	for _, runID := range needJobs {
		if ctx.Err() != nil {
			break
		}
		// RunAttempt 0 means "accept all attempts" in ConvertJobs filtering;
		// use the listed attempt when this run was part of the listing.
		var attempt int64
		if run, ok := byID[runID]; ok {
			attempt = run.RunAttempt
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(runID, attempt int64) {
			defer wg.Done()
			defer func() { <-sem }()
			jobsURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%d/jobs",
				owner, repo, runID)
			jobs, err := client.FetchJobsPaginated(ctx, jobsURL)
			if err != nil {
				return // transient: the run stays jobs_fetched=0 for the next sync
			}
			converted := analyzer.ConvertJobs(jobs, attempt)
			if err := st.UpsertJobs(owner, repo, runID, converted); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			fetched.Add(1)
		}(runID, attempt)
	}
	wg.Wait()
	stats.JobsFetched = int(fetched.Load())
	if firstErr != nil {
		return stats, firstErr
	}
	if ctx.Err() != nil {
		return stats, fmt.Errorf("sync aborted: %w", ctx.Err())
	}
	return stats, nil
}
