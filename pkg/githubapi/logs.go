package githubapi

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// FetchJobLog downloads the log for a single job.
// The GitHub API returns plain text with step delimiters.
func (c *Client) FetchJobLog(ctx context.Context, owner, repo string, jobID int64) ([]byte, error) {
	ctx, span := getTracer().Start(ctx, "FetchJobLog", trace.WithAttributes(
		attribute.String("github.owner", owner),
		attribute.String("github.repo", repo),
		attribute.Int64("github.job_id", jobID),
	))
	defer span.End()

	endpoint := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/jobs/%d/logs", owner, repo, jobID)
	resp, err := fetchWithAuth(ctx, c, endpoint, "application/vnd.github.v3+raw")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// SplitJobLogByStep splits a job log into per-step segments using step
// time ranges from the API. GHA job logs are a flat stream of timestamped
// lines with no step-level boundary markers, so we assign each line to
// the step whose [startedAt, completedAt) range contains the line's
// timestamp. Returns a map from step number to the step's log content.
func SplitJobLogByStep(logData []byte, steps []Step) map[int][]byte {
	if len(steps) == 0 {
		return nil
	}

	// Parse step time ranges (GHA API times are second-precision)
	type stepRange struct {
		number int
		start  time.Time
		end    time.Time
	}
	var ranges []stepRange
	for _, s := range steps {
		st, err := time.Parse(time.RFC3339, s.StartedAt)
		if err != nil {
			continue
		}
		et, err := time.Parse(time.RFC3339, s.CompletedAt)
		if err != nil {
			continue
		}
		ranges = append(ranges, stepRange{number: s.Number, start: st, end: et})
	}

	if len(ranges) == 0 {
		return nil
	}

	result := make(map[int][]byte)
	buckets := make(map[int][][]byte) // step number -> lines

	scanner := bufio.NewScanner(bytes.NewReader(logData))
	for scanner.Scan() {
		line := scanner.Bytes()
		lineCopy := make([]byte, len(line))
		copy(lineCopy, line)

		// Parse timestamp from line (28 chars: "2024-01-15T10:30:45.1234567Z")
		if len(line) < 28 {
			// Line too short; attach to most recent step or skip
			if len(ranges) > 0 {
				last := ranges[len(ranges)-1].number
				buckets[last] = append(buckets[last], lineCopy)
			}
			continue
		}

		tsStr := string(line[:28])
		t, err := time.Parse("2006-01-02T15:04:05.0000000Z", tsStr)
		if err != nil {
			continue
		}

		// First try: find the step whose [start, end] range contains the line.
		// GHA API times are second-precision, so boundary seconds may overlap
		// between adjacent steps. When multiple steps match, prefer the one
		// whose start is closest to the line timestamp (i.e. the earlier step
		// that actually produced the line, not the later step that happens to
		// start at the same second).
		bestIdx := -1
		var bestStartDist time.Duration
		for i := 0; i < len(ranges); i++ {
			if !t.Before(ranges[i].start) && !t.After(ranges[i].end) {
				dist := t.Sub(ranges[i].start)
				if bestIdx < 0 || dist < bestStartDist {
					bestIdx = i
					bestStartDist = dist
				}
			}
		}

		// Fallback: last step whose start <= line timestamp
		if bestIdx < 0 {
			for i := 0; i < len(ranges); i++ {
				if !t.Before(ranges[i].start) {
					bestIdx = i
				}
			}
		}

		// Last resort: closest step by start time
		if bestIdx < 0 {
			bestIdx = 0
			bestDist := t.Sub(ranges[0].start).Abs()
			for i := 1; i < len(ranges); i++ {
				d := t.Sub(ranges[i].start).Abs()
				if d < bestDist {
					bestDist = d
					bestIdx = i
				}
			}
		}

		buckets[ranges[bestIdx].number] = append(buckets[ranges[bestIdx].number], lineCopy)
	}

	for num, lines := range buckets {
		result[num] = bytes.Join(lines, []byte("\n"))
	}

	return result
}

// StepLogKey creates a lookup key from a step number.
func StepLogKey(stepNumber int) string {
	return strconv.Itoa(stepNumber)
}
