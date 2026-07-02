package output

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/perfetto"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/sdk/trace"
)

func OutputCombinedResultsMarkdown(w io.Writer, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics, traceEvents []analyzer.TraceEvent, globalEarliestTime, globalLatestTime int64, perfettoFile string, openInPerfetto bool, spans []trace.ReadOnlySpan, enricher enrichment.Enricher) error {
	fmt.Fprintln(w, "# Trace Performance Report")
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "- Perfetto UI: %s\n", markdownLink("https://ui.perfetto.dev", "https://ui.perfetto.dev"))
	if perfettoFile != "" {
		fmt.Fprintf(w, "- Perfetto trace: `%s`\n", perfettoFile)
	}
	fmt.Fprintln(w, "")

	fmt.Fprintln(w, "## Summary")
	fmt.Fprintf(w, "- URLs: **%d**\n", len(urlResults))
	fmt.Fprintf(w, "- Runs: **%d**\n", combined.TotalRuns)
	fmt.Fprintf(w, "- Jobs: **%d**\n", combined.TotalJobs)
	fmt.Fprintf(w, "- Steps: **%d**\n", combined.TotalSteps)
	fmt.Fprintf(w, "- Success rate: **%s%% workflows**, **%s%% jobs**\n", combined.SuccessRate, combined.JobSuccessRate)
	fmt.Fprintf(w, "- Peak concurrency: **%d**\n", combined.MaxConcurrency)
	fmt.Fprintln(w, "")

	if len(traceEvents) > 0 {
		fmt.Fprintf(w, "- Trace events: **%d**\n", len(traceEvents))
		fmt.Fprintln(w, "")
	}

	pending := collectPending(urlResults)
	if len(pending) > 0 {
		fmt.Fprintln(w, "## Pending Jobs")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "| Job | Status | Source |")
		fmt.Fprintln(w, "| --- | --- | --- |")
		for _, job := range pending {
			fmt.Fprintf(w, "| %s | %s | %s |\n",
				markdownLink(job.URL, job.Name+requiredEmoji(job.IsRequired)),
				job.Status,
				job.SourceName,
			)
		}
		fmt.Fprintln(w, "")
	}

	sortedResults := sortByEarliest(urlResults)
	fmt.Fprintln(w, "## URLs")
	fmt.Fprintln(w, "")
	for _, result := range sortedResults {
		label := result.DisplayName
		if result.Type == "pr" && result.BranchName != "" {
			label = fmt.Sprintf("%s (%s)", result.DisplayName, result.BranchName)
		}
		fmt.Fprintf(w, "- %s\n", markdownLink(result.DisplayURL, label))
	}
	fmt.Fprintln(w, "")

	if len(spans) > 0 {
		renderResourceMarkdown(w, spans)
		renderGenAIUsageMarkdown(w, spans)

		timelineStr := RenderTimelineToBuffer(spans, globalEarliestTime, globalLatestTime, enricher)
		if timelineStr != "" {
			fmt.Fprintln(w, "## Pipeline Timeline")
			fmt.Fprintln(w, "")
			fmt.Fprintln(w, "```")
			fmt.Fprint(w, utils.StripANSI(timelineStr))
			fmt.Fprintln(w, "```")
			fmt.Fprintln(w, "")
		}
	}

	if len(urlResults) > 0 {
		fmt.Fprintln(w, "## Run Summary")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "| URL | Runs | Wall | Compute | Approvals | Merged |")
		fmt.Fprintln(w, "| --- | ---: | ---: | ---: | ---: | :---: |")
		for _, result := range urlResults {
			wallMs, computeMs := computeTimelineDurations(result.Metrics.JobTimeline)
			approvals := countReviewEvents(result.ReviewEvents, "shippit") + countReviewEvents(result.ReviewEvents, "merged")
			merged := countReviewEvents(result.ReviewEvents, "merged") > 0
			fmt.Fprintf(w, "| %s | %d | %s | %s | %d | %s |\n",
				markdownLink(result.DisplayURL, result.DisplayName),
				result.Metrics.TotalRuns,
				utils.HumanizeTime(float64(wallMs)/1000),
				utils.HumanizeTime(float64(computeMs)/1000),
				approvals,
				boolYesNo(merged),
			)
		}
		fmt.Fprintln(w, "")
	}

	if len(urlResults) > 0 {
		fmt.Fprintln(w, "## Slowest Jobs")
		fmt.Fprintln(w, "")

		// Collect bottleneck keys across all results
		bottleneckKeys := map[string]struct{}{}
		for _, result := range sortedResults {
			for _, job := range analyzer.FindBottleneckJobs(result.Metrics.JobTimeline) {
				key := fmt.Sprintf("%s-%d-%d", job.Name, job.StartTime, job.EndTime)
				bottleneckKeys[key] = struct{}{}
			}
		}

		grouped := map[string][]analyzer.CombinedTimelineJob{}
		for _, job := range combined.JobTimeline {
			grouped[job.SourceURL] = append(grouped[job.SourceURL], job)
		}

		for _, result := range sortedResults {
			jobs := grouped[result.DisplayURL]
			if len(jobs) == 0 {
				continue
			}
			analyzer.SortCombinedJobsByDuration(jobs)
			if len(jobs) > 5 {
				jobs = jobs[:5]
			}
			fmt.Fprintf(w, "### %s\n", markdownLink(result.DisplayURL, result.DisplayName))
			for _, job := range jobs {
				duration := float64(job.EndTime-job.StartTime) / 1000
				key := fmt.Sprintf("%s-%d-%d", job.Name, job.StartTime, job.EndTime)
				bottleneck := ""
				if _, ok := bottleneckKeys[key]; ok {
					bottleneck = " \U0001F525" // 🔥
				}
				jobText := fmt.Sprintf("%s — %s%s%s", utils.HumanizeTime(duration), job.Name, bottleneck, requiredEmoji(job.IsRequired))
				if job.URL != "" {
					jobText = markdownLink(job.URL, jobText)
				}
				fmt.Fprintf(w, "- %s\n", jobText)
			}
			fmt.Fprintln(w, "")
		}
	}

	// Commit aggregates
	commitAggregates := []CommitAggregate{}
	for _, result := range urlResults {
		if result.Type == "commit" {
			commitAggregates = append(commitAggregates, CommitAggregate{
				Name:                    result.DisplayName,
				URLIndex:                result.URLIndex,
				TotalRunsForCommit:      result.AllCommitRunsCount,
				TotalComputeMsForCommit: result.AllCommitRunsComputeMs,
			})
		}
	}
	if len(commitAggregates) > 0 {
		fmt.Fprintln(w, "## Commit Runs (All Runs for Commit SHA)")
		fmt.Fprintln(w, "")
		for _, agg := range commitAggregates {
			computeDisplay := utils.HumanizeTime(float64(agg.TotalComputeMsForCommit) / 1000)
			fmt.Fprintf(w, "- **%s**: %d runs, compute %s\n", agg.Name, agg.TotalRunsForCommit, computeDisplay)
		}
		fmt.Fprintln(w, "")
	}

	if perfettoFile != "" {
		if err := perfetto.WriteTrace(w, urlResults, combined, traceEvents, globalEarliestTime, perfettoFile, openInPerfetto, spans); err != nil {
			return err
		}
	}
	return nil
}

// spanAttrsWithResource flattens a span's attributes merged with its resource
// attributes (resource wins), for the summary aggregators.
func spanAttrsWithResource(s trace.ReadOnlySpan) map[string]string {
	attrs := make(map[string]string)
	for _, a := range s.Attributes() {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	if s.Resource() != nil {
		for _, a := range s.Resource().Attributes() {
			attrs[string(a.Key)] = a.Value.Emit()
		}
	}
	return attrs
}

// renderResourceMarkdown emits a per-service deployment/infrastructure context
// section in Markdown when the trace's spans carry resource attributes.
func renderResourceMarkdown(w io.Writer, spans []trace.ReadOnlySpan) {
	r := enrichment.NewResourceSummary()
	for _, s := range spans {
		r.Add(spanAttrsWithResource(s))
	}
	if !r.HasData() {
		return
	}
	fmt.Fprintln(w, "## Resources")
	fmt.Fprintln(w, "")
	for _, line := range r.Lines() {
		fmt.Fprintf(w, "- %s\n", line)
	}
	fmt.Fprintln(w, "")
}

// renderGenAIUsageMarkdown emits an LLM token-usage summary in Markdown when
// the trace contains GenAI spans.
func renderGenAIUsageMarkdown(w io.Writer, spans []trace.ReadOnlySpan) {
	u := enrichment.NewGenAIUsage()
	for _, s := range spans {
		attrs := make(map[string]string)
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.Emit()
		}
		u.Add(attrs)
	}
	if !u.HasData() {
		return
	}
	fmt.Fprintln(w, "## LLM Usage")
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "**%s**\n", u.Summary())
	fmt.Fprintln(w, "")
	for _, line := range u.ModelLines() {
		fmt.Fprintf(w, "- %s\n", line)
	}
	fmt.Fprintln(w, "")
}

func markdownLink(url, text string) string {
	// Escape the characters that break a GFM table cell or the link syntax
	// itself: `|` ends the cell, `]` ends the link text.
	text = strings.ReplaceAll(text, "|", "\\|")
	text = strings.ReplaceAll(text, "]", "\\]")
	return fmt.Sprintf("[%s](%s)", text, url)
}

func collectPending(results []analyzer.URLResult) []PendingJobWithSource {
	allPending := []PendingJobWithSource{}
	for _, result := range results {
		for _, job := range result.Metrics.PendingJobs {
			allPending = append(allPending, PendingJobWithSource{
				PendingJob: job,
				SourceURL:  result.DisplayURL,
				SourceName: result.DisplayName,
			})
		}
	}
	sort.Slice(allPending, func(i, j int) bool {
		return strings.Compare(allPending[i].Name, allPending[j].Name) < 0
	})
	return allPending
}

func computeTimelineDurations(timeline []analyzer.TimelineJob) (int64, int64) {
	if len(timeline) == 0 {
		return 0, 0
	}
	start := timeline[0].StartTime
	end := timeline[0].EndTime
	computeMs := int64(0)
	for _, job := range timeline {
		if job.StartTime < start {
			start = job.StartTime
		}
		if job.EndTime > end {
			end = job.EndTime
		}
		if job.EndTime > job.StartTime {
			computeMs += job.EndTime - job.StartTime
		}
	}
	return maxInt64(0, end-start), computeMs
}
