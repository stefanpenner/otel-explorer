// Package store persists workflow runs and jobs in a local SQLite database
// so repeat analyses are incremental: runs are append-only once completed,
// and a sync only fetches what the store doesn't already hold. SQLite via
// modernc.org/sqlite keeps the binary cgo-free and cross-compilable.
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	_ "modernc.org/sqlite"
)

// Store wraps the SQLite database holding synced run/job history.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS runs (
	id            INTEGER NOT NULL,
	owner         TEXT NOT NULL,
	repo          TEXT NOT NULL,
	workflow_name TEXT NOT NULL DEFAULT '',
	head_sha      TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL DEFAULT '',
	conclusion    TEXT NOT NULL DEFAULT '',
	created_at    INTEGER NOT NULL DEFAULT 0, -- unix ms
	started_at    INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER NOT NULL DEFAULT 0,
	duration_ms   INTEGER NOT NULL DEFAULT 0,
	jobs_fetched  INTEGER NOT NULL DEFAULT 0,
	run_attempt   INTEGER NOT NULL DEFAULT 1,
	PRIMARY KEY (owner, repo, id)
);
CREATE INDEX IF NOT EXISTS idx_runs_repo_created ON runs(owner, repo, created_at);

CREATE TABLE IF NOT EXISTS jobs (
	id            INTEGER NOT NULL,
	owner         TEXT NOT NULL,
	repo          TEXT NOT NULL,
	run_id        INTEGER NOT NULL,
	name          TEXT NOT NULL DEFAULT '',
	url           TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL DEFAULT '',
	conclusion    TEXT NOT NULL DEFAULT '',
	created_at    INTEGER NOT NULL DEFAULT 0,
	started_at    INTEGER NOT NULL DEFAULT 0,
	completed_at  INTEGER NOT NULL DEFAULT 0,
	duration_ms   INTEGER NOT NULL DEFAULT 0,
	queue_time_ms INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (owner, repo, id)
);
CREATE INDEX IF NOT EXISTS idx_jobs_run ON jobs(owner, repo, run_id);
`

// DefaultPath returns the global database location, honoring XDG_DATA_HOME.
func DefaultPath() (string, error) {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dataHome = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(dataHome, "ote", "ote.db"), nil
}

// Open opens (creating if necessary) the database at path and migrates it.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("creating store directory: %w", err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating store schema: %w", err)
	}
	// Additive migration for databases created before run_attempt existed;
	// a duplicate-column error means the schema above already provided it.
	if _, err := db.Exec(`ALTER TABLE runs ADD COLUMN run_attempt INTEGER NOT NULL DEFAULT 1`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		db.Close()
		return nil, fmt.Errorf("migrating run_attempt column: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// UpsertRuns inserts or updates runs (and, for runs carrying job detail,
// replaces their jobs) inside one transaction.
func (s *Store) UpsertRuns(owner, repo string, runs []analyzer.RunData) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	runStmt, err := tx.Prepare(`
		INSERT INTO runs (id, owner, repo, workflow_name, head_sha, status, conclusion,
			created_at, started_at, updated_at, duration_ms, jobs_fetched, run_attempt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (owner, repo, id) DO UPDATE SET
			workflow_name=excluded.workflow_name, head_sha=excluded.head_sha,
			status=excluded.status, conclusion=excluded.conclusion,
			created_at=excluded.created_at, started_at=excluded.started_at,
			updated_at=excluded.updated_at, duration_ms=excluded.duration_ms,
			jobs_fetched=max(runs.jobs_fetched, excluded.jobs_fetched),
			run_attempt=excluded.run_attempt`)
	if err != nil {
		return err
	}
	defer runStmt.Close()

	jobStmt, err := tx.Prepare(`
		INSERT INTO jobs (id, owner, repo, run_id, name, url, status, conclusion,
			created_at, started_at, completed_at, duration_ms, queue_time_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (owner, repo, id) DO UPDATE SET
			run_id=excluded.run_id, name=excluded.name, url=excluded.url,
			status=excluded.status, conclusion=excluded.conclusion,
			created_at=excluded.created_at, started_at=excluded.started_at,
			completed_at=excluded.completed_at, duration_ms=excluded.duration_ms,
			queue_time_ms=excluded.queue_time_ms`)
	if err != nil {
		return err
	}
	defer jobStmt.Close()

	for _, run := range runs {
		jobsFetched := 0
		if len(run.Jobs) > 0 {
			jobsFetched = 1
		}
		attempt := run.Attempt
		if attempt == 0 {
			attempt = 1
		}
		if _, err := runStmt.Exec(run.ID, owner, repo, run.WorkflowName, run.HeadSHA,
			run.Status, run.Conclusion, msOf(run.CreatedAt), msOf(run.StartedAt),
			msOf(run.UpdatedAt), run.Duration, jobsFetched, attempt); err != nil {
			return fmt.Errorf("upserting run %d: %w", run.ID, err)
		}
		if len(run.Jobs) == 0 {
			continue
		}
		// Replace this run's jobs wholesale so re-syncs never duplicate.
		if _, err := tx.Exec(`DELETE FROM jobs WHERE owner=? AND repo=? AND run_id=?`,
			owner, repo, run.ID); err != nil {
			return err
		}
		for _, job := range run.Jobs {
			if _, err := jobStmt.Exec(job.ID, owner, repo, run.ID, job.Name, job.URL,
				job.Status, job.Conclusion, msOf(job.CreatedAt), msOf(job.StartedAt),
				msOf(job.CompletedAt), job.Duration, job.QueueTime); err != nil {
				return fmt.Errorf("upserting job %d: %w", job.ID, err)
			}
		}
	}
	return tx.Commit()
}

// LoadRuns returns the stored runs (with jobs) created within [since, until],
// oldest first.
func (s *Store) LoadRuns(owner, repo string, since, until time.Time) ([]analyzer.RunData, error) {
	rows, err := s.db.Query(`
		SELECT id, workflow_name, head_sha, status, conclusion,
			created_at, started_at, updated_at, duration_ms, run_attempt
		FROM runs WHERE owner=? AND repo=? AND created_at BETWEEN ? AND ?
		ORDER BY created_at, id`, owner, repo, msOf(since), msOf(until))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []analyzer.RunData
	index := make(map[int64]int)
	for rows.Next() {
		var run analyzer.RunData
		var created, started, updated int64
		if err := rows.Scan(&run.ID, &run.WorkflowName, &run.HeadSHA, &run.Status,
			&run.Conclusion, &created, &started, &updated, &run.Duration, &run.Attempt); err != nil {
			return nil, err
		}
		run.CreatedAt = timeOf(created)
		run.StartedAt = timeOf(started)
		run.UpdatedAt = timeOf(updated)
		index[run.ID] = len(runs)
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	jobRows, err := s.db.Query(`
		SELECT j.id, j.run_id, j.name, j.url, j.status, j.conclusion,
			j.created_at, j.started_at, j.completed_at, j.duration_ms, j.queue_time_ms
		FROM jobs j JOIN runs r ON r.owner=j.owner AND r.repo=j.repo AND r.id=j.run_id
		WHERE j.owner=? AND j.repo=? AND r.created_at BETWEEN ? AND ?
		ORDER BY j.run_id, j.started_at, j.id`, owner, repo, msOf(since), msOf(until))
	if err != nil {
		return nil, err
	}
	defer jobRows.Close()

	for jobRows.Next() {
		var job analyzer.JobData
		var runID, created, started, completed int64
		if err := jobRows.Scan(&job.ID, &runID, &job.Name, &job.URL, &job.Status,
			&job.Conclusion, &created, &started, &completed, &job.Duration, &job.QueueTime); err != nil {
			return nil, err
		}
		job.CreatedAt = timeOf(created)
		job.StartedAt = timeOf(started)
		job.CompletedAt = timeOf(completed)
		if i, ok := index[runID]; ok {
			runs[i].Jobs = append(runs[i].Jobs, job)
		}
	}
	return runs, jobRows.Err()
}

// Watermark returns the newest run CreatedAt stored for the repo, or the
// zero time when nothing is stored yet.
func (s *Store) Watermark(owner, repo string) (time.Time, error) {
	var ms sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(created_at) FROM runs WHERE owner=? AND repo=?`,
		owner, repo).Scan(&ms)
	if err != nil || !ms.Valid || ms.Int64 == 0 {
		return time.Time{}, err
	}
	return timeOf(ms.Int64), nil
}

// RunsNeedingJobs returns IDs of completed runs in the window whose job
// detail has not been fetched yet, oldest first.
func (s *Store) RunsNeedingJobs(owner, repo string, since, until time.Time) ([]int64, error) {
	rows, err := s.db.Query(`
		SELECT id FROM runs
		WHERE owner=? AND repo=? AND status='completed' AND jobs_fetched=0
			AND created_at BETWEEN ? AND ?
		ORDER BY created_at, id`, owner, repo, msOf(since), msOf(until))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func msOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func timeOf(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// UpsertJobs replaces one run's jobs and marks the run's job detail as
// fetched, without touching any other run column — safe to call with data
// from a jobs-only fetch where the run listing fields are unknown.
func (s *Store) UpsertJobs(owner, repo string, runID int64, jobs []analyzer.JobData) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM jobs WHERE owner=? AND repo=? AND run_id=?`,
		owner, repo, runID); err != nil {
		return err
	}
	for _, job := range jobs {
		if _, err := tx.Exec(`
			INSERT INTO jobs (id, owner, repo, run_id, name, url, status, conclusion,
				created_at, started_at, completed_at, duration_ms, queue_time_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (owner, repo, id) DO UPDATE SET
				run_id=excluded.run_id, name=excluded.name, url=excluded.url,
				status=excluded.status, conclusion=excluded.conclusion,
				created_at=excluded.created_at, started_at=excluded.started_at,
				completed_at=excluded.completed_at, duration_ms=excluded.duration_ms,
				queue_time_ms=excluded.queue_time_ms`,
			job.ID, owner, repo, runID, job.Name, job.URL, job.Status, job.Conclusion,
			msOf(job.CreatedAt), msOf(job.StartedAt), msOf(job.CompletedAt),
			job.Duration, job.QueueTime); err != nil {
			return fmt.Errorf("upserting job %d: %w", job.ID, err)
		}
	}
	if _, err := tx.Exec(`UPDATE runs SET jobs_fetched=1 WHERE owner=? AND repo=? AND id=?`,
		owner, repo, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// OldestRun returns the oldest run CreatedAt stored for the repo, or the
// zero time when nothing is stored yet.
func (s *Store) OldestRun(owner, repo string) (time.Time, error) {
	var ms sql.NullInt64
	err := s.db.QueryRow(`SELECT MIN(created_at) FROM runs WHERE owner=? AND repo=? AND created_at > 0`,
		owner, repo).Scan(&ms)
	if err != nil || !ms.Valid || ms.Int64 == 0 {
		return time.Time{}, err
	}
	return timeOf(ms.Int64), nil
}
