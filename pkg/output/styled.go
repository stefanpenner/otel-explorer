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

// OutputStyledResults renders a lipgloss-styled terminal report to w
// (typically os.Stderr): header, pending jobs, run summary, commit runs,
// slowest jobs, resources, LLM usage, then the timeline.
func OutputStyledResults(w io.Writer, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics, traceEvents []analyzer.TraceEvent, globalEarliestTime, globalLatestTime int64, spans []trace.ReadOnlySpan, enricher enrichment.Enricher) error {
	writeStyledHeader(w, urlResults, combined, spans, enricher)
	writePendingJobs(w, urlResults)
	writeRunSummary(w, urlResults)
	writeCommitRuns(w, urlResults)
	writeSlowestJobs(w, urlResults, combined)
	renderResourceSection(w, spans)
	renderGenAIUsageSection(w, spans)
	styledSection(w, "Pipeline Timelines")
	RenderOTelTimeline(w, spans, time.UnixMilli(globalEarliestTime), time.UnixMilli(globalLatestTime), enricher)
	return nil
}

func writeStyledHeader(w io.Writer, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics, spans []trace.ReadOnlySpan, enricher enrichment.Enricher) {
	const width = 90
	contentWidth := width - 4 // "│ " + content + " │"
	top := borderStyle.Render("╭" + strings.Repeat("─", width-2) + "╮")
	bot := borderStyle.Render("╰" + strings.Repeat("─", width-2) + "╯")

	fmt.Fprintln(w)
	fmt.Fprintln(w, top)
	fmt.Fprintln(w, boxLine(contentWidth, titleStyle.Render("Trace Analyzer")))
	fmt.Fprintln(w, ratesLine(contentWidth, combined))
	fmt.Fprintln(w, wallLine(contentWidth, urlResults, combined, spans, enricher))
	writeQueueBillableRunners(w, contentWidth, urlResults)
	if line := changedFilesLine(spans); line != "" {
		fmt.Fprintln(w, boxLine(contentWidth, line))
	}
	if line := workflowFilesLine(spans); line != "" {
		fmt.Fprintln(w, boxLine(contentWidth, line))
	}
	for _, result := range urlResults {
		fmt.Fprintln(w, boxLine(contentWidth, fittedURL(result.DisplayURL, contentWidth)))
	}
	fmt.Fprintln(w, bot)
}

func boxLine(contentWidth int, content string) string {
	pad := contentWidth - lipgloss.Width(content)
	if pad < 0 {
		pad = 0
	}
	return borderStyle.Render("│") + " " + content + strings.Repeat(" ", pad) + " " + borderStyle.Render("│")
}

func ratesLine(contentWidth int, combined analyzer.CombinedMetrics) string {
	sep := labelStyle.Render(" • ")
	wfRate, wfStyled := shownRate(combined.TotalRuns, combined.SuccessRate)
	jobRate, jobStyled := shownRate(combined.TotalJobs, combined.JobSuccessRate)
	leftStyled := labelStyle.Render("Workflows: ") + wfStyled +
		sep + labelStyle.Render("Jobs: ") + jobStyled
	rightStyled := numStyle.Render(fmt.Sprintf("%d", combined.TotalRuns)) + labelStyle.Render(" runs") +
		sep + numStyle.Render(fmt.Sprintf("%d", combined.TotalJobs)) + labelStyle.Render(" jobs") +
		sep + numStyle.Render(fmt.Sprintf("%d", combined.TotalSteps)) + labelStyle.Render(" steps")
	leftPlain := fmt.Sprintf("Workflows: %s • Jobs: %s", wfRate, jobRate)
	rightPlain := fmt.Sprintf("%d runs • %d jobs • %d steps", combined.TotalRuns, combined.TotalJobs, combined.TotalSteps)
	return buildLineAligned(contentWidth, leftStyled, leftPlain, rightStyled, rightPlain)
}

// shownRate paints a success percentage.
// Empty or unparsable input is "–", not a false 0%.
// A zero total is not parsed: a non-empty raw rate is still shown, colored as 0.
func shownRate(total int, raw string) (plain, styled string) {
	rate := 0.0
	parsed := true
	if total > 0 {
		_, err := fmt.Sscanf(raw, "%f", &rate)
		parsed = err == nil
	}
	plain = raw + "%"
	styled = colorForSuccessRate(rate).Render(plain)
	if raw == "" || !parsed {
		plain = "–"
		styled = dimStyle.Render(plain)
	}
	return plain, styled
}

func wallLine(contentWidth int, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics, spans []trace.ReadOnlySpan, enricher enrichment.Enricher) string {
	sep := labelStyle.Render(" • ")
	wallMs, computeMs := combinedWallCompute(urlResults)
	if wallMs == 0 && len(spans) > 0 {
		// Pure span input: derive wall and compute from the spans themselves.
		wallMs, computeMs = analyzer.SpansWallCompute(spans, enricher)
	}
	wallTime := utils.HumanizeTime(float64(wallMs) / 1000)
	computeTime := utils.HumanizeTime(float64(computeMs) / 1000)
	leftStyled := labelStyle.Render("Wall: ") + numStyle.Render(wallTime) +
		sep + labelStyle.Render("Compute: ") + numStyle.Render(computeTime)
	rightStyled := labelStyle.Render("Concurrency: ") + numStyle.Render(fmt.Sprintf("%d", combined.MaxConcurrency))
	leftPlain := fmt.Sprintf("Wall: %s • Compute: %s", wallTime, computeTime)
	rightPlain := fmt.Sprintf("Concurrency: %d", combined.MaxConcurrency)
	return buildLineAligned(contentWidth, leftStyled, leftPlain, rightStyled, rightPlain)
}

func writeQueueBillableRunners(w io.Writer, contentWidth int, urlResults []analyzer.URLResult) {
	var queueTimes []float64
	var retried, runs int
	billable := map[string]int64{}
	runnerJobs := map[string]int{}
	runnerDur := map[string]float64{}
	for _, result := range urlResults {
		queueTimes = append(queueTimes, result.Metrics.QueueTimes...)
		retried += result.Metrics.RetriedRuns
		runs += result.Metrics.TotalRuns
		for osName, ms := range result.Metrics.BillableMs {
			billable[osName] += ms
		}
		for runner, count := range result.Metrics.RunnerJobCounts {
			runnerJobs[runner] += count
		}
		for runner, dur := range result.Metrics.RunnerDurations {
			runnerDur[runner] += dur
		}
	}
	if line := queueRetryLine(queueTimes, retried, runs); line != "" {
		fmt.Fprintln(w, boxLine(contentWidth, line))
	}
	if line := billableLine(billable); line != "" {
		fmt.Fprintln(w, boxLine(contentWidth, line))
	}
	if line := runnersLine(runnerJobs, runnerDur); line != "" {
		fmt.Fprintln(w, boxLine(contentWidth, line))
	}
}

func queueRetryLine(queueTimes []float64, retried, runs int) string {
	sep := labelStyle.Render(" • ")
	var parts []string
	if len(queueTimes) > 0 {
		avgQ := 0.0
		maxQ := 0.0
		for _, qt := range queueTimes {
			avgQ += qt
			if qt > maxQ {
				maxQ = qt
			}
		}
		avgQ /= float64(len(queueTimes))
		avgQStr := utils.HumanizeTime(avgQ / 1000)
		maxQStr := utils.HumanizeTime(maxQ / 1000)
		parts = append(parts, labelStyle.Render("Queue: avg ")+numStyle.Render(avgQStr)+labelStyle.Render(" / max ")+numStyle.Render(maxQStr))
	}
	if retried > 0 {
		retryPct := fmt.Sprintf("%.0f%%", float64(retried)/float64(runs)*100)
		retryDetail := fmt.Sprintf("(%d/%d runs)", retried, runs)
		parts = append(parts, labelStyle.Render("Retries: ")+numStyle.Render(retryPct)+" "+labelStyle.Render(retryDetail))
	}
	return strings.Join(parts, sep)
}

func billableLine(billable map[string]int64) string {
	if len(billable) == 0 {
		return ""
	}
	osNames := map[string]string{"UBUNTU": "Ubuntu", "MACOS": "macOS", "WINDOWS": "Windows"}
	var parts []string
	for _, osKey := range []string{"UBUNTU", "MACOS", "WINDOWS"} {
		dur := utils.HumanizeTime(float64(billable[osKey]) / 1000)
		parts = append(parts, labelStyle.Render(osNames[osKey]+" ")+numStyle.Render(dur))
	}
	return labelStyle.Render("Billable: ") + strings.Join(parts, "  ")
}

func runnersLine(counts map[string]int, durs map[string]float64) string {
	if len(counts) == 0 {
		return ""
	}
	var parts []string
	for runner, count := range counts {
		durStr := utils.HumanizeTime(durs[runner] / 1000)
		parts = append(parts, numStyle.Render(runner)+labelStyle.Render(fmt.Sprintf(" ×%d ", count))+dimStyle.Render("("+durStr+")"))
	}
	return labelStyle.Render("Runners: ") + strings.Join(parts, "  ")
}

// changedFilesLine is files and artifacts from the first span that has either.
// An empty artifact-names value wins over a later one on that same span.
func changedFilesLine(spans []trace.ReadOnlySpan) string {
	sep := labelStyle.Render(" • ")
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
		var parts []string
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
			return strings.Join(parts, sep)
		}
	}
	return ""
}

func workflowFilesLine(spans []trace.ReadOnlySpan) string {
	seen := make(map[string]bool)
	var paths []string
	for _, s := range spans {
		for _, a := range s.Attributes() {
			if string(a.Key) != "cicd.pipeline.definition" {
				continue
			}
			p := a.Value.AsString()
			if p == "" || seen[p] {
				continue
			}
			seen[p] = true
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		return ""
	}
	return labelStyle.Render("Workflows: ") + numStyle.Render(strings.Join(paths, ", "))
}

func fittedURL(displayURL string, maxW int) string {
	urlText := displayURL
	if lipgloss.Width(urlText) > maxW {
		runes := []rune(urlText)
		for len(runes) > 3 && lipgloss.Width(string(runes))+3 > maxW {
			runes = runes[:len(runes)-1]
		}
		urlText = string(runes) + "..."
	}
	return utils.MakeClickableLink(utils.ExpandGitHubURL(displayURL), urlText)
}

func writePendingJobs(w io.Writer, urlResults []analyzer.URLResult) {
	allPending := collectPending(urlResults)
	if len(allPending) == 0 {
		return
	}
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

func writeRunSummary(w io.Writer, urlResults []analyzer.URLResult) {
	if len(urlResults) == 0 {
		return
	}
	styledSection(w, "Run Summary")
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
		if runes := []rune(name); len(runes) > 38 {
			name = string(runes[:35]) + "..."
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

func writeCommitRuns(w io.Writer, urlResults []analyzer.URLResult) {
	var commits []analyzer.URLResult
	for _, result := range urlResults {
		if result.Type == "commit" {
			commits = append(commits, result)
		}
	}
	if len(commits) == 0 {
		return
	}
	styledSection(w, "Commit Runs (All Runs for Commit SHA)")
	for _, result := range commits {
		computeDisplay := utils.HumanizeTime(float64(result.AllCommitRunsComputeMs) / 1000)
		fmt.Fprintf(w, "  %s %s  runs=%s  compute=%s\n",
			dimStyle.Render(fmt.Sprintf("[%d]", result.URLIndex+1)),
			valueStyle.Render(result.DisplayName),
			numStyle.Render(fmt.Sprintf("%d", result.AllCommitRunsCount)),
			numStyle.Render(computeDisplay))
	}
}

func writeSlowestJobs(w io.Writer, urlResults []analyzer.URLResult, combined analyzer.CombinedMetrics) {
	allJobs := append([]analyzer.CombinedTimelineJob{}, combined.JobTimeline...)
	analyzer.SortCombinedJobsByDuration(allJobs)
	slowJobs := allJobs
	if len(slowJobs) > 10 {
		slowJobs = slowJobs[:10]
	}
	if len(slowJobs) == 0 {
		return
	}
	styledSection(w, "Slowest Jobs")
	sortedResults := sortByEarliest(urlResults)
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

// renderResourceSection prints a per-service deployment/infrastructure context
// summary (environment, cloud region, k8s pod, host, …) when the trace's spans
// carry resource attributes. No-op when no service context is present.
func renderResourceSection(w io.Writer, spans []trace.ReadOnlySpan) {
	r := enrichment.NewResourceSummary()
	for _, s := range spans {
		attrs := make(map[string]string)
		// Span attributes first, then resource attributes (resource wins for
		// the well-known service/deployment keys).
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.Emit()
		}
		if s.Resource() != nil {
			for _, a := range s.Resource().Attributes() {
				attrs[string(a.Key)] = a.Value.Emit()
			}
		}
		r.Add(attrs)
	}
	if !r.HasData() {
		return
	}
	styledSection(w, "Resources")
	for _, line := range r.Lines() {
		fmt.Fprintf(w, "  %s\n", line)
	}
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
	return max(int64(0), globalEnd-globalStart), totalComputeMs
}

// RenderTimelineToBuffer renders the OTel timeline into a buffer and returns
// the content as a string. Useful for embedding in markdown code blocks.
func RenderTimelineToBuffer(spans []trace.ReadOnlySpan, globalEarliestTime, globalLatestTime int64, enricher enrichment.Enricher) string {
	var buf bytes.Buffer
	RenderOTelTimeline(&buf, spans, time.UnixMilli(globalEarliestTime), time.UnixMilli(globalLatestTime), enricher)
	return buf.String()
}
