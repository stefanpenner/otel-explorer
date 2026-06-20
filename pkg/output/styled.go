package output

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/sdk/trace"
)

// OutputStyledResults renders a lipgloss-styled terminal report.
// It writes to w (typically os.Stderr) and mirrors the sections of the TUI
// header: summary box, pending jobs, run summary, slowest jobs, and timeline.
func OutputStyledResults(w io.Writer, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics, traceEvents []analyzer.TraceEvent, globalEarliestTime, globalLatestTime int64, spans []trace.ReadOnlySpan, enricher enrichment.Enricher) error {
	width := 90
	contentWidth := width - 4 // minus "│ " and " │"

	sep := labelStyle.Render(" • ")

	// ── Header box ────────────────────────────────────────────────────
	topBorder := borderStyle.Render("╭" + strings.Repeat("─", width-2) + "╮")
	botBorder := borderStyle.Render("╰" + strings.Repeat("─", width-2) + "╯")

	buildLeftLine := func(content string) string {
		w := lipgloss.Width(content)
		pad := contentWidth - w
		if pad < 0 {
			pad = 0
		}
		return borderStyle.Render("│") + " " + content + strings.Repeat(" ", pad) + " " + borderStyle.Render("│")
	}

	// Line 1: Title
	line1 := buildLeftLine(titleStyle.Render("Trace Analyzer"))

	// Compute success rates
	successRate := float64(0)
	jobSuccessRate := float64(0)
	if combined.TotalRuns > 0 {
		// Parse from the string representation
		fmt.Sscanf(combined.SuccessRate, "%f", &successRate)
	}
	if combined.TotalJobs > 0 {
		fmt.Sscanf(combined.JobSuccessRate, "%f", &jobSuccessRate)
	}

	// Line 2: Success rates (left) + Counts (right). Empty rate strings mean
	// the input carried no outcome data — show "–" rather than a false 0%.
	wfRate, jobRate := combined.SuccessRate+"%", combined.JobSuccessRate+"%"
	wfRateStyled := colorForSuccessRate(successRate).Render(wfRate)
	jobRateStyled := colorForSuccessRate(jobSuccessRate).Render(jobRate)
	if combined.SuccessRate == "" {
		wfRate = "–"
		wfRateStyled = dimStyle.Render(wfRate)
	}
	if combined.JobSuccessRate == "" {
		jobRate = "–"
		jobRateStyled = dimStyle.Render(jobRate)
	}
	leftStyled := labelStyle.Render("Workflows: ") + wfRateStyled +
		sep + labelStyle.Render("Jobs: ") + jobRateStyled
	rightStyled := numStyle.Render(fmt.Sprintf("%d", combined.TotalRuns)) + labelStyle.Render(" runs") +
		sep + numStyle.Render(fmt.Sprintf("%d", combined.TotalJobs)) + labelStyle.Render(" jobs") +
		sep + numStyle.Render(fmt.Sprintf("%d", combined.TotalSteps)) + labelStyle.Render(" steps")
	leftPlain := fmt.Sprintf("Workflows: %s • Jobs: %s", wfRate, jobRate)
	rightPlain := fmt.Sprintf("%d runs • %d jobs • %d steps", combined.TotalRuns, combined.TotalJobs, combined.TotalSteps)
	line2 := buildLineAligned(contentWidth, leftStyled, leftPlain, rightStyled, rightPlain)

	// Line 3: Wall time + Compute time
	wallMs, computeMs := combinedWallCompute(urlResults)
	if wallMs == 0 && len(spans) > 0 {
		// Pure span input: derive wall/compute from the spans themselves.
		wallMs, computeMs = analyzer.SpansWallCompute(spans, enricher)
	}
	wallTime := utils.HumanizeTime(float64(wallMs) / 1000)
	computeTime := utils.HumanizeTime(float64(computeMs) / 1000)
	leftStyled3 := labelStyle.Render("Wall: ") + numStyle.Render(wallTime) +
		sep + labelStyle.Render("Compute: ") + numStyle.Render(computeTime)
	rightStyled3 := labelStyle.Render("Concurrency: ") + numStyle.Render(fmt.Sprintf("%d", combined.MaxConcurrency))
	leftPlain3 := fmt.Sprintf("Wall: %s • Compute: %s", wallTime, computeTime)
	rightPlain3 := fmt.Sprintf("Concurrency: %d", combined.MaxConcurrency)
	line3 := buildLineAligned(contentWidth, leftStyled3, leftPlain3, rightStyled3, rightPlain3)

	// Aggregate enrichment metrics across URL results
	var totalQueueTimes []float64
	var totalRetriedRuns, totalRunCount int
	totalBillable := map[string]int64{}
	totalRunnerJobs := map[string]int{}
	totalRunnerDur := map[string]float64{}
	for _, result := range urlResults {
		totalQueueTimes = append(totalQueueTimes, result.Metrics.QueueTimes...)
		totalRetriedRuns += result.Metrics.RetriedRuns
		totalRunCount += result.Metrics.TotalRuns
		for os, ms := range result.Metrics.BillableMs {
			totalBillable[os] += ms
		}
		for runner, count := range result.Metrics.RunnerJobCounts {
			totalRunnerJobs[runner] += count
		}
		for runner, dur := range result.Metrics.RunnerDurations {
			totalRunnerDur[runner] += dur
		}
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, topBorder)
	fmt.Fprintln(w, line1)
	fmt.Fprintln(w, line2)
	fmt.Fprintln(w, line3)

	// Line 4: Queue time + Retry rate (conditional)
	hasQueueData := len(totalQueueTimes) > 0
	hasRetryData := totalRetriedRuns > 0
	if hasQueueData || hasRetryData {
		var parts4 []string
		if hasQueueData {
			avgQ := 0.0
			maxQ := 0.0
			for _, qt := range totalQueueTimes {
				avgQ += qt
				if qt > maxQ {
					maxQ = qt
				}
			}
			avgQ /= float64(len(totalQueueTimes))
			avgQStr := utils.HumanizeTime(avgQ / 1000)
			maxQStr := utils.HumanizeTime(maxQ / 1000)
			parts4 = append(parts4, labelStyle.Render("Queue: avg ")+numStyle.Render(avgQStr)+labelStyle.Render(" / max ")+numStyle.Render(maxQStr))
		}
		if hasRetryData {
			retryPct := fmt.Sprintf("%.0f%%", float64(totalRetriedRuns)/float64(totalRunCount)*100)
			retryDetail := fmt.Sprintf("(%d/%d runs)", totalRetriedRuns, totalRunCount)
			parts4 = append(parts4, labelStyle.Render("Retries: ")+numStyle.Render(retryPct)+" "+labelStyle.Render(retryDetail))
		}
		fmt.Fprintln(w, buildLeftLine(strings.Join(parts4, sep)))
	}

	// Line 5: Billable timing (conditional)
	if len(totalBillable) > 0 {
		osNames := map[string]string{"UBUNTU": "Ubuntu", "MACOS": "macOS", "WINDOWS": "Windows"}
		var billParts []string
		for _, osKey := range []string{"UBUNTU", "MACOS", "WINDOWS"} {
			ms := totalBillable[osKey]
			durStr := utils.HumanizeTime(float64(ms) / 1000)
			billParts = append(billParts, labelStyle.Render(osNames[osKey]+" ")+numStyle.Render(durStr))
		}
		fmt.Fprintln(w, buildLeftLine(labelStyle.Render("Billable: ")+strings.Join(billParts, "  ")))
	}

	// Line 6: Runner distribution (conditional)
	if len(totalRunnerJobs) > 0 {
		var runnerParts []string
		for runner, count := range totalRunnerJobs {
			dur := totalRunnerDur[runner]
			durStr := utils.HumanizeTime(dur / 1000)
			runnerParts = append(runnerParts, numStyle.Render(runner)+labelStyle.Render(fmt.Sprintf(" ×%d ", count))+dimStyle.Render("("+durStr+")"))
		}
		fmt.Fprintln(w, buildLeftLine(labelStyle.Render("Runners: ")+strings.Join(runnerParts, "  ")))
	}

	// Changed files + artifacts line (extracted from workflow span attributes)
	{
		var parts []string
		for _, s := range spans {
			var filesCount, filesAdd, filesDel, artCount, artSize string
			for _, a := range s.Attributes() {
				switch string(a.Key) {
				case "vcs.changes.count":
					filesCount = a.Value.AsString()
				case "vcs.changes.additions":
					filesAdd = a.Value.AsString()
				case "vcs.changes.deletions":
					filesDel = a.Value.AsString()
				case "cicd.pipeline.artifacts.count":
					artCount = a.Value.AsString()
				case "cicd.pipeline.artifacts.size":
					artSize = a.Value.AsString()
				}
			}
			if filesCount != "" && filesCount != "0" {
				parts = append(parts,
					labelStyle.Render("Files: ")+numStyle.Render(filesCount)+labelStyle.Render(" changed")+
						labelStyle.Render(" (")+
						lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render("+"+filesAdd)+
						labelStyle.Render(" / ")+
						lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render("-"+filesDel)+
						labelStyle.Render(")"))
			}
			if artCount != "" && artCount != "0" {
				artPart := labelStyle.Render("Artifacts: ") + numStyle.Render(artCount) +
					labelStyle.Render(" (") + numStyle.Render(artSize) + labelStyle.Render(")")
				// Include artifact names
				for _, a := range s.Attributes() {
					if string(a.Key) == "cicd.pipeline.artifacts.names" {
						names := a.Value.AsString()
						if names != "" {
							artPart += labelStyle.Render(" — ") + numStyle.Render(names)
						}
						break
					}
				}
				parts = append(parts, artPart)
			}
			if len(parts) > 0 {
				break
			}
		}
		if len(parts) > 0 {
			fmt.Fprintln(w, buildLeftLine(strings.Join(parts, sep)))
		}
	}

	// Workflow files line (extracted from workflow span attributes)
	{
		seen := make(map[string]bool)
		var wfPaths []string
		for _, s := range spans {
			for _, a := range s.Attributes() {
				if string(a.Key) == "cicd.pipeline.definition" {
					p := a.Value.AsString()
					if p != "" && !seen[p] {
						seen[p] = true
						wfPaths = append(wfPaths, p)
					}
				}
			}
		}
		if len(wfPaths) > 0 {
			fmt.Fprintln(w, buildLeftLine(labelStyle.Render("Workflows: ")+numStyle.Render(strings.Join(wfPaths, ", "))))
		}
	}

	// URL lines inside header box
	for _, result := range urlResults {
		urlText := result.DisplayURL
		maxW := contentWidth
		if lipgloss.Width(urlText) > maxW {
			urlText = urlText[:maxW-3] + "..."
		}
		linked := utils.MakeClickableLink(utils.ExpandGitHubURL(result.DisplayURL), urlText)
		fmt.Fprintln(w, buildLeftLine(linked))
	}
	fmt.Fprintln(w, botBorder)

	// ── Pending Jobs ──────────────────────────────────────────────────
	allPending := collectPending(urlResults)
	if len(allPending) > 0 {
		styledSection(w, "Pending Jobs")
		fmt.Fprintf(w, "  %s %d jobs still running\n",
			warningStyle.Render("WARNING:"), len(allPending))
		for i, job := range allPending {
			jobLink := utils.MakeClickableLink(job.URL, job.Name+requiredEmoji(job.IsRequired))
			fmt.Fprintf(w, "  %s  %s %s %s\n",
				dimStyle.Render(fmt.Sprintf("%d.", i+1)),
				subheaderStyle.Render(jobLink),
				dimStyle.Render("("+job.Status+")"),
				labelStyle.Render("← "+job.SourceName))
		}
	}

	// ── Run Summary ───────────────────────────────────────────────────
	sortedResults := sortByEarliest(urlResults)
	if len(urlResults) > 0 {
		styledSection(w, "Run Summary")
		// Table header
		hdr := fmt.Sprintf("  %-40s %6s %10s %10s %9s %7s",
			labelStyle.Render("URL"),
			labelStyle.Render("Runs"),
			labelStyle.Render("Wall"),
			labelStyle.Render("Compute"),
			labelStyle.Render("Approvals"),
			labelStyle.Render("Merged"))
		fmt.Fprintln(w, hdr)
		fmt.Fprintf(w, "  %s\n", dimStyle.Render(strings.Repeat("─", 86)))

		for _, result := range urlResults {
			wMs, cMs := computeTimelineDurations(result.Metrics.JobTimeline)
			approvals := countReviewEvents(result.ReviewEvents, "shippit") + countReviewEvents(result.ReviewEvents, "merged")
			merged := countReviewEvents(result.ReviewEvents, "merged") > 0
			name := result.DisplayName
			if len(name) > 38 {
				name = name[:35] + "..."
			}
			nameLinked := utils.MakeClickableLink(result.DisplayURL, name)
			mergedText := dimStyle.Render("no")
			if merged {
				mergedText = successStyle.Render("yes")
			}
			fmt.Fprintf(w, "  %-40s %s %10s %10s %9d %7s\n",
				nameLinked,
				numStyle.Render(fmt.Sprintf("%6d", result.Metrics.TotalRuns)),
				numStyle.Render(utils.HumanizeTime(float64(wMs)/1000)),
				numStyle.Render(utils.HumanizeTime(float64(cMs)/1000)),
				approvals,
				mergedText)
		}
	}

	// ── Commit Aggregates ─────────────────────────────────────────────
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
		styledSection(w, "Commit Runs (All Runs for Commit SHA)")
		for _, agg := range commitAggregates {
			computeDisplay := utils.HumanizeTime(float64(agg.TotalComputeMsForCommit) / 1000)
			fmt.Fprintf(w, "  %s %s  runs=%s  compute=%s\n",
				dimStyle.Render(fmt.Sprintf("[%d]", agg.URLIndex+1)),
				valueStyle.Render(agg.Name),
				numStyle.Render(fmt.Sprintf("%d", agg.TotalRunsForCommit)),
				numStyle.Render(computeDisplay))
		}
	}

	// ── Slowest Jobs ──────────────────────────────────────────────────
	allJobs := append([]analyzer.CombinedTimelineJob{}, combined.JobTimeline...)
	analyzer.SortCombinedJobsByDuration(allJobs)
	slowJobs := allJobs
	if len(slowJobs) > 10 {
		slowJobs = slowJobs[:10]
	}
	if len(slowJobs) > 0 {
		styledSection(w, "Slowest Jobs")
		bottleneckKeys := map[string]struct{}{}
		for _, result := range sortedResults {
			for _, job := range analyzer.FindBottleneckJobs(result.Metrics.JobTimeline) {
				key := fmt.Sprintf("%s-%d-%d", job.Name, job.StartTime, job.EndTime)
				bottleneckKeys[key] = struct{}{}
			}
		}

		grouped := map[string][]analyzer.CombinedTimelineJob{}
		for _, job := range slowJobs {
			grouped[job.SourceURL] = append(grouped[job.SourceURL], job)
		}

		for _, result := range sortedResults {
			jobs := grouped[result.DisplayURL]
			if len(jobs) == 0 {
				continue
			}
			headerText := fmt.Sprintf("[%d] %s", result.URLIndex+1, result.DisplayName)
			fmt.Fprintf(w, "\n  %s\n", subheaderStyle.Render(utils.MakeClickableLink(result.DisplayURL, headerText)))
			analyzer.SortCombinedJobsByDuration(jobs)
			for i, job := range jobs {
				duration := float64(job.EndTime-job.StartTime) / 1000
				key := fmt.Sprintf("%s-%d-%d", job.Name, job.StartTime, job.EndTime)
				bottleneck := ""
				if _, ok := bottleneckKeys[key]; ok {
					bottleneck = " 🔥"
				}
				durationStr := utils.HumanizeTime(duration)
				jobText := fmt.Sprintf("%s %s%s%s",
					numStyle.Render(durationStr),
					valueStyle.Render(job.Name),
					bottleneck,
					requiredEmoji(job.IsRequired))
				if job.URL != "" {
					jobText = utils.MakeClickableLink(job.URL, fmt.Sprintf("%s — %s%s%s", durationStr, job.Name, bottleneck, requiredEmoji(job.IsRequired)))
				}
				fmt.Fprintf(w, "    %s  %s\n",
					dimStyle.Render(fmt.Sprintf("%d.", i+1)),
					jobText)
			}
		}
	}

	// ── LLM Usage ─────────────────────────────────────────────────────
	renderGenAIUsageSection(w, spans)

	// ── Pipeline Timelines ────────────────────────────────────────────
	styledSection(w, "Pipeline Timelines")
	RenderOTelTimeline(w, spans, time.UnixMilli(globalEarliestTime), time.UnixMilli(globalLatestTime), enricher)

	return nil
}

// renderGenAIUsageSection prints an LLM token-usage summary when the trace
// contains GenAI spans, so the total model cost of a request is visible
// without summing individual spans. No-op when there are no LLM calls.
func renderGenAIUsageSection(w io.Writer, spans []trace.ReadOnlySpan) {
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
	styledSection(w, "LLM Usage")
	fmt.Fprintf(w, "  %s\n", u.Summary())
	for _, line := range u.ModelLines() {
		fmt.Fprintf(w, "    %s\n", dimStyle.Render(line))
	}
}

// styledSection prints a section header with lipgloss styling.
func styledSection(w io.Writer, title string) {
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %s\n", titleStyle.Render(title))
	fmt.Fprintf(w, "  %s\n", dimStyle.Render(strings.Repeat("─", len(title)+2)))
}

// buildLineAligned builds a bordered line with left- and right-aligned content
// using plain-text widths for alignment.
func buildLineAligned(contentWidth int, leftStyled, leftPlain, rightStyled, rightPlain string) string {
	leftW := lipgloss.Width(leftPlain)
	rightW := lipgloss.Width(rightPlain)
	pad := contentWidth - leftW - rightW
	if pad < 1 {
		pad = 1
	}
	return borderStyle.Render("│") + " " + leftStyled + strings.Repeat(" ", pad) + rightStyled + " " + borderStyle.Render("│")
}

// combinedWallCompute returns overall wall and compute times across all results.
func combinedWallCompute(urlResults []analyzer.URLResult) (int64, int64) {
	totalComputeMs := int64(0)
	globalStart := int64(0)
	globalEnd := int64(0)
	first := true
	for _, result := range urlResults {
		for _, job := range result.Metrics.JobTimeline {
			if job.EndTime > job.StartTime {
				totalComputeMs += job.EndTime - job.StartTime
			}
			if first || job.StartTime < globalStart {
				globalStart = job.StartTime
			}
			if first || job.EndTime > globalEnd {
				globalEnd = job.EndTime
			}
			first = false
		}
	}
	return maxInt64(0, globalEnd-globalStart), totalComputeMs
}

// RenderTimelineToBuffer renders the OTel timeline into a buffer and returns
// the content as a string. Useful for embedding in markdown code blocks.
func RenderTimelineToBuffer(spans []trace.ReadOnlySpan, globalEarliestTime, globalLatestTime int64, enricher enrichment.Enricher) string {
	var buf bytes.Buffer
	RenderOTelTimeline(&buf, spans, time.UnixMilli(globalEarliestTime), time.UnixMilli(globalLatestTime), enricher)
	return buf.String()
}
