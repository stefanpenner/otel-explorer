// arc-sim simulates ARC (Actions Runner Controller) OTel span emission
// for a real GitHub Actions workflow run. It fetches job data from the
// GitHub API, constructs matching JobCompleted messages with plausible
// runner lifecycle timestamps, and sends them through the OTelRecorder.
//
// Usage:
//
//	arc-sim --run=<owner/repo/runID> --endpoint=localhost:4318
//	arc-sim --run=stefanpenner/otel-explorer/15088041641 --endpoint=localhost:4318
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/arc"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

func main() {
	run := flag.String("run", "", "owner/repo/runID (required)")
	endpoint := flag.String("endpoint", "localhost:4318", "OTLP HTTP endpoint")
	insecure := flag.Bool("insecure", true, "use HTTP instead of HTTPS")
	queueDelay := flag.Duration("queue-delay", 3*time.Second, "simulated queue wait before ARC picks up the job")
	startupDelay := flag.Duration("startup-delay", 8*time.Second, "simulated runner pod startup time")
	flag.Parse()

	if *run == "" {
		fmt.Fprintln(os.Stderr, "Usage: arc-sim --run=owner/repo/runID [--endpoint=host:port]")
		os.Exit(1)
	}

	parts := strings.SplitN(*run, "/", 3)
	if len(parts) != 3 {
		log.Fatalf("--run must be owner/repo/runID, got %q", *run)
	}
	owner, repo := parts[0], parts[1]
	runID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		log.Fatalf("invalid runID %q: %v", parts[2], err)
	}

	ctx := context.Background()

	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		if ghPath, err := exec.LookPath("gh"); err == nil {
			if out, err := exec.Command(ghPath, "auth", "token").Output(); err == nil {
				token = strings.TrimSpace(string(out))
			}
		}
	}
	if token == "" {
		log.Fatal("GITHUB_TOKEN not set and gh CLI not available")
	}

	ghCtx := githubapi.NewContext(token)
	client := githubapi.NewClient(ghCtx)

	// Fetch real workflow run and jobs
	log.Printf("Fetching workflow run %s/%s/%d ...", owner, repo, runID)
	wfRun, err := client.FetchWorkflowRun(ctx, owner, repo, runID)
	if err != nil {
		log.Fatalf("Failed to fetch workflow run: %v", err)
	}
	log.Printf("Workflow: %s (status=%s, conclusion=%s)", wfRun.Name, wfRun.Status, wfRun.Conclusion)

	jobsURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/actions/runs/%d/attempts/%d/jobs?per_page=100",
		owner, repo, runID, wfRun.RunAttempt)
	jobs, err := client.FetchJobsPaginated(ctx, jobsURL)
	if err != nil {
		log.Fatalf("Failed to fetch jobs: %v", err)
	}
	log.Printf("Found %d jobs", len(jobs))

	// Create OTLP exporter
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(*endpoint),
	}
	if *insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		log.Fatalf("Failed to create OTLP exporter: %v", err)
	}
	defer exporter.Shutdown(ctx)

	recorder := arc.NewOTelRecorder(exporter)
	recorder.SetRunAttempt(wfRun.RunAttempt)

	// Simulate ARC messages for each completed job
	for _, job := range jobs {
		if job.Status != "completed" {
			log.Printf("  Skipping %s (status=%s)", job.Name, job.Status)
			continue
		}

		createdAt := parseTime(job.CreatedAt)
		startedAt := parseTime(job.StartedAt)
		completedAt := parseTime(job.CompletedAt)

		// Simulate ARC timestamps:
		// QueueTime = when GitHub queued the job
		// ScaleSetAssignTime = QueueTime + small delay (ARC picks it up)
		// RunnerAssignTime = when the runner actually started (from GitHub API)
		// FinishTime = when the job completed
		queueTime := createdAt
		scaleSetAssignTime := createdAt.Add(*queueDelay)
		runnerAssignTime := startedAt
		finishTime := completedAt

		// If simulated startup would exceed actual start, clamp it
		if scaleSetAssignTime.Add(*startupDelay).After(runnerAssignTime) && runnerAssignTime.After(scaleSetAssignTime) {
			// Keep scaleSetAssignTime as is, runner was fast
		} else if scaleSetAssignTime.After(runnerAssignTime) {
			// ARC pickup was slower than actual start — clamp
			scaleSetAssignTime = queueTime.Add(time.Second)
		}

		msg := &arc.JobCompleted{
			JobMessageBase: arc.JobMessageBase{
				RunnerRequestID:    job.ID,
				WorkflowRunID:      runID,
				JobID:              strconv.FormatInt(job.ID, 10),
				JobDisplayName:     job.Name,
				OwnerName:          owner,
				RepositoryName:     repo,
				JobWorkflowRef:     fmt.Sprintf("%s/%s/%s@refs/heads/%s", owner, repo, wfRun.Path, wfRun.HeadBranch),
				EventName:          "push",
				QueueTime:          queueTime,
				ScaleSetAssignTime: scaleSetAssignTime,
				RunnerAssignTime:   runnerAssignTime,
				FinishTime:         finishTime,
			},
			Result:     job.Conclusion,
			RunnerName: job.RunnerName,
			RunnerID:   int(job.ID % 1000),
		}

		log.Printf("  Sending spans for job %q (ID=%d): queue=%s startup=%s exec=%s",
			job.Name, job.ID,
			scaleSetAssignTime.Sub(queueTime),
			runnerAssignTime.Sub(scaleSetAssignTime),
			finishTime.Sub(runnerAssignTime),
		)

		recorder.RecordJobCompleted(msg)
	}

	// Give exporter time to flush
	time.Sleep(2 * time.Second)
	log.Println("Done. Spans exported.")
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

