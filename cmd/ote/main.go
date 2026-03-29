package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/core"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	otelexport "github.com/stefanpenner/otel-explorer/pkg/export/otel"
	perfettoexport "github.com/stefanpenner/otel-explorer/pkg/export/perfetto"
	"github.com/stefanpenner/otel-explorer/pkg/export/terminal"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/filter"
	"github.com/stefanpenner/otel-explorer/pkg/logparse"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/polling"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/receiver"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/traceapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/webhook"
	"github.com/stefanpenner/otel-explorer/pkg/output"
	"github.com/stefanpenner/otel-explorer/pkg/perfetto"
	"github.com/stefanpenner/otel-explorer/pkg/tui"
	tuiresults "github.com/stefanpenner/otel-explorer/pkg/tui/results"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// ANSI color codes
const (
	colorRed   = "\033[31m"
	colorReset = "\033[0m"
)

// reloadProgressAdapter adapts tuiresults.LoadingReporter to analyzer.ProgressReporter
type reloadProgressAdapter struct {
	reporter tuiresults.LoadingReporter
}

func (a *reloadProgressAdapter) StartURL(urlIndex int, url string) {
	if a.reporter != nil {
		a.reporter.SetURL(url)
	}
}

func (a *reloadProgressAdapter) SetURLRuns(runCount int) {
	// Not directly reportable to LoadingReporter
}

func (a *reloadProgressAdapter) SetPhase(phase string) {
	if a.reporter != nil {
		a.reporter.SetPhase(phase)
	}
}

func (a *reloadProgressAdapter) SetDetail(detail string) {
	if a.reporter != nil {
		a.reporter.SetDetail(detail)
	}
}

func (a *reloadProgressAdapter) ProcessRun() {
	// Not directly reportable to LoadingReporter
}

func (a *reloadProgressAdapter) Finish() {
	// Not directly reportable to LoadingReporter
}

func printError(err error, context string) {
	// Print the full error message, not just flattened
	fmt.Fprintf(os.Stderr, "%sError: %s: %v%s\n", colorRed, context, err, colorReset)
}

func printErrorMsg(message string) {
	fmt.Fprintf(os.Stderr, "%sError: %s%s\n", colorRed, message, colorReset)
}

type config struct {
	urls             []string
	traceFiles       []string // --trace=<file.json> OTel trace files
	tempoURL         string   // --tempo=<baseURL> Tempo backend
	jaegerURL        string   // --jaeger=<baseURL> Jaeger v2 backend
	traceIDs         []string // trace IDs to fetch from backends
	perfettoFile     string
	openInPerfetto   bool
	openInOTel       bool
	otelEndpoint     string
	otelStdout       bool
	otelGRPCEndpoint string
	tuiMode          bool
	outputFormat     string // "stdout" or "markdown"
	clearCache       bool
	window           time.Duration
	showHelp         bool
	trendsMode       bool
	trendsRepo       string
	trendsDays       int
	trendsFormat     string
	trendsBranch     string
	trendsWorkflow   string
	trendsNoSample   bool
	trendsConfidence float64
	trendsMargin     float64
	noArtifacts      bool
	convertMode      bool
	convertFiles     []string
	// OTel alignment features
	filterExpr     string // --filter=<expr>
	errorsOnly     bool   // --errors-only
	listenAddr     string // --listen=<addr>
	enrichmentFile string // --enrichment=<file>
	lintMode       bool   // --lint
	fetchLogs      bool   // --logs: fetch and parse step logs for sub-step spans
}

func parseArgs(args []string, terminal bool) (config, error) {
	cfg := config{
		tuiMode:          terminal,
		trendsDays:       30, // default to 30 days
		trendsFormat:     "terminal",
		trendsConfidence: 0.95,
		trendsMargin:     0.10,
	}

	// Check if first arg is "convert" subcommand
	if len(args) > 0 && args[0] == "convert" {
		cfg.convertMode = true
		args = args[1:] // consume the "convert" subcommand
		// Remaining non-flag args are files to convert
		for _, a := range args {
			if a == "help" || a == "--help" || a == "-h" {
				cfg.showHelp = true
			} else if !strings.HasPrefix(a, "-") {
				cfg.convertFiles = append(cfg.convertFiles, a)
			}
		}
		return cfg, nil
	}

	// Check if first arg is "trends" subcommand
	if len(args) > 0 && args[0] == "trends" {
		cfg.trendsMode = true
		args = args[1:] // consume the "trends" subcommand
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if arg == "help" || arg == "--help" || arg == "-h" {
			cfg.showHelp = true
			continue
		}
		if strings.HasPrefix(arg, "--perfetto=") {
			cfg.perfettoFile = strings.TrimPrefix(arg, "--perfetto=")
			continue
		}
		if strings.HasPrefix(arg, "--window=") {
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--window="))
			if err != nil {
				return cfg, fmt.Errorf("invalid window duration %s: %w", arg, err)
			}
			cfg.window = d
			continue
		}
		if arg == "--open-in-perfetto" {
			cfg.openInPerfetto = true
			continue
		}
		if arg == "--open-in-otel" {
			cfg.openInOTel = true
			continue
		}
		if strings.HasPrefix(arg, "--otel=") {
			cfg.otelEndpoint = strings.TrimPrefix(arg, "--otel=")
			continue
		}
		if arg == "--otel" {
			cfg.otelStdout = true
			continue
		}
		if strings.HasPrefix(arg, "--otel-grpc=") {
			cfg.otelGRPCEndpoint = strings.TrimPrefix(arg, "--otel-grpc=")
			continue
		}
		if arg == "--otel-grpc" {
			cfg.otelGRPCEndpoint = "localhost:4317"
			continue
		}
		if arg == "--tui" {
			cfg.tuiMode = true
			continue
		}
		if arg == "--no-tui" || arg == "--notui" {
			cfg.tuiMode = false
			continue
		}
		if strings.HasPrefix(arg, "--output=") {
			cfg.outputFormat = strings.TrimPrefix(arg, "--output=")
			if cfg.outputFormat != "stdout" && cfg.outputFormat != "markdown" {
				return cfg, fmt.Errorf("invalid --output value: %s (must be 'stdout' or 'markdown')", cfg.outputFormat)
			}
			cfg.tuiMode = false
			continue
		}
		if strings.HasPrefix(arg, "--trace=") {
			cfg.traceFiles = append(cfg.traceFiles, strings.TrimPrefix(arg, "--trace="))
			continue
		}
		if strings.HasPrefix(arg, "--tempo=") {
			cfg.tempoURL = strings.TrimPrefix(arg, "--tempo=")
			continue
		}
		if strings.HasPrefix(arg, "--jaeger=") {
			cfg.jaegerURL = strings.TrimPrefix(arg, "--jaeger=")
			continue
		}
		if strings.HasPrefix(arg, "--trace-id=") {
			cfg.traceIDs = append(cfg.traceIDs, strings.TrimPrefix(arg, "--trace-id="))
			continue
		}
		if arg == "--clear-cache" {
			cfg.clearCache = true
			continue
		}
		if arg == "--no-artifacts" {
			cfg.noArtifacts = true
			continue
		}
		if arg == "--logs" {
			cfg.fetchLogs = true
			continue
		}
		if strings.HasPrefix(arg, "--filter=") {
			cfg.filterExpr = strings.TrimPrefix(arg, "--filter=")
			continue
		}
		if arg == "--errors-only" {
			cfg.errorsOnly = true
			continue
		}
		if strings.HasPrefix(arg, "--listen=") {
			cfg.listenAddr = strings.TrimPrefix(arg, "--listen=")
			continue
		}
		if arg == "--listen" {
			cfg.listenAddr = ":4318"
			continue
		}
		if strings.HasPrefix(arg, "--enrichment=") {
			cfg.enrichmentFile = strings.TrimPrefix(arg, "--enrichment=")
			continue
		}
		if arg == "--lint" {
			cfg.lintMode = true
			continue
		}

		// Trends-specific flags
		if strings.HasPrefix(arg, "--days=") {
			days := strings.TrimPrefix(arg, "--days=")
			var err error
			_, err = fmt.Sscanf(days, "%d", &cfg.trendsDays)
			if err != nil || cfg.trendsDays < 1 {
				return cfg, fmt.Errorf("invalid --days value: %s", days)
			}
			continue
		}
		if strings.HasPrefix(arg, "--format=") {
			cfg.trendsFormat = strings.TrimPrefix(arg, "--format=")
			if cfg.trendsFormat != "terminal" && cfg.trendsFormat != "json" {
				return cfg, fmt.Errorf("invalid --format value: %s (must be 'terminal' or 'json')", cfg.trendsFormat)
			}
			continue
		}
		if strings.HasPrefix(arg, "--branch=") {
			cfg.trendsBranch = strings.TrimPrefix(arg, "--branch=")
			continue
		}
		if strings.HasPrefix(arg, "--workflow=") {
			cfg.trendsWorkflow = strings.TrimPrefix(arg, "--workflow=")
			continue
		}
		if arg == "--no-sample" {
			cfg.trendsNoSample = true
			continue
		}
		if strings.HasPrefix(arg, "--confidence=") {
			val, err := strconv.ParseFloat(strings.TrimPrefix(arg, "--confidence="), 64)
			if err != nil || val <= 0 || val >= 1 {
				return cfg, fmt.Errorf("invalid --confidence value: must be between 0 and 1 (e.g., 0.95)")
			}
			cfg.trendsConfidence = val
			continue
		}
		if strings.HasPrefix(arg, "--margin=") {
			val, err := strconv.ParseFloat(strings.TrimPrefix(arg, "--margin="), 64)
			if err != nil || val <= 0 || val >= 1 {
				return cfg, fmt.Errorf("invalid --margin value: must be between 0 and 1 (e.g., 0.10)")
			}
			cfg.trendsMargin = val
			continue
		}

		// For trends mode, first non-flag arg is the repo
		if cfg.trendsMode && cfg.trendsRepo == "" && !strings.HasPrefix(arg, "-") {
			cfg.trendsRepo = arg
			continue
		}

		// If the arg looks like a local file (not a URL, not a flag), check if
		// it exists on disk — if so, treat it as a trace file input.
		if !strings.HasPrefix(arg, "http") && !strings.HasPrefix(arg, "-") {
			if _, err := os.Stat(arg); err == nil {
				cfg.traceFiles = append(cfg.traceFiles, arg)
				continue
			}
		}

		cfg.urls = append(cfg.urls, arg)
	}

	return cfg, nil
}

func main() {
	cfg, err := parseArgs(os.Args[1:], isTerminal())
	if err != nil {
		printErrorMsg(err.Error())
		os.Exit(1)
	}

	if cfg.showHelp {
		printUsage()
		os.Exit(0)
	}

	args := cfg.urls

	// Handle --clear-cache flag
	if cfg.clearCache {
		cacheDir := githubapi.DefaultCacheDir()
		if err := os.RemoveAll(cacheDir); err != nil {
			printError(err, "failed to clear cache")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Cache cleared: %s\n", cacheDir)
		if len(args) == 0 && !cfg.trendsMode {
			os.Exit(0)
		}
	}

	// Handle trends mode
	if cfg.trendsMode {
		if cfg.trendsRepo == "" {
			printErrorMsg("Trends mode requires a repository in format 'owner/repo'\n\n  Usage: ote trends owner/repo [--days=30] [--format=terminal|json]\n\n  Run 'ote --help' for more information.")
			os.Exit(1)
		}

		// Parse owner/repo
		parts := strings.Split(cfg.trendsRepo, "/")
		if len(parts) != 2 {
			printErrorMsg(fmt.Sprintf("Invalid repository format: %s (expected 'owner/repo')", cfg.trendsRepo))
			os.Exit(1)
		}
		owner, repo := parts[0], parts[1]

		token := resolveGitHubToken()
		if token == "" {
			printErrorMsg("GITHUB_TOKEN environment variable is required.\n  Tip: install the GitHub CLI (gh) and run `gh auth login` to authenticate automatically.")
			os.Exit(1)
		}

		ctx := context.Background()
		client := githubapi.NewClient(githubapi.NewContext(token))

		// Setup progress spinner for trends mode
		progress := tui.NewProgress(1, os.Stderr)
		progress.Start()
		progress.StartURL(0, cfg.trendsRepo)

		// Perform trend analysis
		analysis, err := analyzer.AnalyzeTrends(ctx, client, owner, repo, cfg.trendsDays, cfg.trendsBranch, cfg.trendsWorkflow, analyzer.TrendOptions{
			NoSample:      cfg.trendsNoSample,
			Confidence:    cfg.trendsConfidence,
			MarginOfError: cfg.trendsMargin,
		}, progress)

		progress.Finish()
		progress.Wait()

		if err != nil {
			printError(err, "trend analysis failed")
			os.Exit(1)
		}

		// Output results
		if err := output.OutputTrends(os.Stderr, analysis, cfg.trendsFormat); err != nil {
			printError(err, "output failed")
			os.Exit(1)
		}

		return
	}

	// Handle convert mode
	if cfg.convertMode {
		if cfg.showHelp {
			printUsage()
			os.Exit(0)
		}

		var allSpans []sdktrace.ReadOnlySpan

		if len(cfg.convertFiles) == 0 {
			// Read from stdin
			spans, err := otlpfile.Parse(os.Stdin)
			if err != nil {
				printError(err, "parsing stdin")
				os.Exit(1)
			}
			allSpans = append(allSpans, spans...)
		} else {
			for _, f := range cfg.convertFiles {
				spans, err := otlpfile.ParseFile(f)
				if err != nil {
					printError(err, fmt.Sprintf("parsing %s", f))
					os.Exit(1)
				}
				allSpans = append(allSpans, spans...)
			}
		}

		if len(allSpans) == 0 {
			fmt.Fprintln(os.Stderr, "No spans found in input.")
			os.Exit(0)
		}

		exporter, err := otelexport.NewStdoutExporter(os.Stdout)
		if err != nil {
			printError(err, "creating stdout exporter")
			os.Exit(1)
		}

		ctx := context.Background()
		if err := exporter.Export(ctx, allSpans); err != nil {
			printError(err, "exporting spans")
			os.Exit(1)
		}
		if err := exporter.Finish(ctx); err != nil {
			printError(err, "finishing export")
			os.Exit(1)
		}

		return
	}

	hasTraceBackend := cfg.tempoURL != "" || cfg.jaegerURL != ""

	ctx := context.Background()

	// Setup enricher chain (needed by both receiver and normal modes)
	var enricher enrichment.Enricher
	var enrichers []enrichment.Enricher
	if len(args) > 0 {
		enrichers = append(enrichers, &enrichment.GHAEnricher{})
	}
	enrichers = append(enrichers, &enrichment.CICDEnricher{})
	if cfg.enrichmentFile != "" {
		ruleEnricher, err := enrichment.LoadRules(cfg.enrichmentFile)
		if err != nil {
			printError(err, "failed to load enrichment rules")
			os.Exit(1)
		}
		enrichers = append(enrichers, ruleEnricher)
		fmt.Fprintf(os.Stderr, "Loaded %d enrichment rules from %s\n", len(ruleEnricher.Rules), cfg.enrichmentFile)
	}
	enrichers = append(enrichers, &enrichment.GenericEnricher{})
	enricher = enrichment.NewChainEnricher(enrichers...)

	// Setup span filter (needed by both receiver and normal modes)
	var spanFilter *filter.Filter
	if cfg.errorsOnly {
		spanFilter = filter.ErrorsOnly()
	} else if cfg.filterExpr != "" {
		var err error
		spanFilter, err = filter.Parse(cfg.filterExpr)
		if err != nil {
			printError(err, "invalid filter expression")
			os.Exit(1)
		}
	}

	// Handle OTLP receiver mode
	if cfg.listenAddr != "" {
		fmt.Fprintf(os.Stderr, "Starting OTLP/HTTP receiver on %s...\n", cfg.listenAddr)
		fmt.Fprintf(os.Stderr, "  POST traces to http://localhost%s/v1/traces\n", cfg.listenAddr)
		fmt.Fprintf(os.Stderr, "  Set OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost%s in your app\n", cfg.listenAddr)
		fmt.Fprintf(os.Stderr, "  Press Ctrl+C to stop and analyze collected spans\n")

		recv := receiver.New(cfg.listenAddr)
		ctx, cancel := context.WithCancel(ctx)

		errCh := make(chan error, 1)
		go func() {
			errCh <- recv.Start(ctx)
		}()

		// Pure receiver mode: wait for user input to stop
		if len(args) == 0 && len(cfg.traceFiles) == 0 && !hasTraceBackend {
			fmt.Fprintf(os.Stderr, "  Waiting for traces... (press Enter to stop)\n")
			buf := make([]byte, 1)
			os.Stdin.Read(buf)
		}

		cancel()
		<-errCh

		receivedSpans := recv.Spans()
		fmt.Fprintf(os.Stderr, "Received %d spans\n", len(receivedSpans))

		if spanFilter != nil {
			receivedSpans = spanFilter.Apply(receivedSpans)
			fmt.Fprintf(os.Stderr, "After filtering: %d spans\n", len(receivedSpans))
		}

		if cfg.lintMode {
			lintData := buildLintData(receivedSpans)
			results := enrichment.LintSpans(lintData)
			fmt.Fprint(os.Stderr, enrichment.FormatLintResults(results))
		}

		spans := receivedSpans
		var globalEarliest, globalLatest int64
		for _, s := range spans {
			startMs := s.StartTime().UnixMilli()
			endMs := s.EndTime().UnixMilli()
			if globalEarliest == 0 || startMs < globalEarliest {
				globalEarliest = startMs
			}
			if endMs > globalLatest {
				globalLatest = endMs
			}
		}

		pipeline := core.NewPipeline(terminal.NewExporter(os.Stderr, enricher))
		if err := pipeline.Process(ctx, spans); err != nil {
			printError(err, "processing spans failed")
		}

		if cfg.tuiMode {
			globalStartTime := time.UnixMilli(globalEarliest)
			globalEndTime := time.UnixMilli(globalLatest)
			if err := tuiresults.Run(spans, globalStartTime, globalEndTime, []string{"receiver"}, nil, nil, enricher); err != nil {
				fmt.Fprintf(os.Stderr, "%sError: TUI failed: %v%s\n", colorRed, err, colorReset)
				os.Exit(1)
			}
		}
		return
	}

	// If no URL args, no trace files, no trace backend, and stdin is piped, read webhook from stdin
	if len(args) == 0 && len(cfg.traceFiles) == 0 && !hasTraceBackend && !isStdinTerminal() {
		fmt.Fprintf(os.Stderr, "Reading webhook from stdin...\n")
		urls, err := webhook.ParseWebhook(os.Stdin)
		if err != nil {
			printError(err, "failed to parse webhook")
			os.Exit(1)
		}
		args = urls
	}

	if len(args) == 0 && len(cfg.traceFiles) == 0 && !hasTraceBackend {
		printErrorMsg("No GitHub URLs or trace files provided.\n\n  Usage: ote <github_url> [flags]\n         ote <trace_file.json> [flags]\n         ote --tempo=<url> --trace-id=<id> [flags]\n         ote --listen[=<addr>] [flags]\n\n  Run 'ote --help' for more information.")
		os.Exit(1)
	}

	// When --otel stdout is used, disable TUI so output goes to stdout cleanly
	if cfg.otelStdout {
		cfg.tuiMode = false
	}

	perfettoFile := cfg.perfettoFile

	// Auto-generate perfetto file if --open-in-perfetto is used without --perfetto
	if cfg.openInPerfetto && perfettoFile == "" {
		tmpFile, err := os.CreateTemp("", "gha-trace-*.pftrace")
		if err == nil {
			perfettoFile = tmpFile.Name()
			tmpFile.Close()
		}
	}

	// Setup GitHub Token (only required when GHA URLs are provided)
	var token string
	if len(args) > 0 {
		token = resolveGitHubToken()
		if token == "" {
			// Fall back to parsing token from positional args (legacy behavior)
			for i, arg := range args {
				if !strings.HasPrefix(arg, "http") && !strings.HasPrefix(arg, "-") {
					if _, err := utils.ParseGitHubURL(arg); err == nil {
						continue
					}
					token = arg
					args = append(args[:i], args[i+1:]...)
					break
				}
			}
		}

		if token == "" {
			printErrorMsg("GITHUB_TOKEN environment variable or token argument is required.\n  Tip: install the GitHub CLI (gh) and run `gh auth login` to authenticate automatically.")
			printUsage()
			os.Exit(1)
		}
	}

	// 3. Setup Exporters
	exporters := []core.Exporter{
		terminal.NewExporter(os.Stderr, enricher),
	}

	if perfettoFile != "" {
		exporters = append(exporters, perfettoexport.NewExporter(os.Stderr, perfettoFile, cfg.openInPerfetto))
	}

	if cfg.otelStdout {
		stdoutExporter, err := otelexport.NewStdoutExporter(os.Stdout)
		if err == nil {
			exporters = append(exporters, stdoutExporter)
		}
	}

	if cfg.otelEndpoint != "" {
		otelExporter, err := otelexport.NewExporter(ctx, cfg.otelEndpoint)
		if err == nil {
			exporters = append(exporters, otelExporter)
		}
	}

	if cfg.otelGRPCEndpoint != "" {
		grpcExporter, err := otelexport.NewGRPCExporter(ctx, cfg.otelGRPCEndpoint)
		if err == nil {
			exporters = append(exporters, grpcExporter)
		}
	}

	pipeline := core.NewPipeline(exporters...)

	// 4. Load trace files if provided
	// Each trace file gets its own url_index (offset after GitHub URLs)
	// so the TUI can group and label them separately.
	var traceSpans []sdktrace.ReadOnlySpan
	for i, tf := range cfg.traceFiles {
		fileSpans, err := otlpfile.ParseFile(tf)
		if err != nil {
			printError(err, fmt.Sprintf("failed to load trace file %s", tf))
			os.Exit(1)
		}
		urlIndex := len(args) + i
		taggedSpans := tagSpansWithIndex(fileSpans, urlIndex)
		traceSpans = append(traceSpans, taggedSpans...)
		fmt.Fprintf(os.Stderr, "Loaded %d spans from %s\n", len(fileSpans), tf)
	}

	// 4. Fetch traces from backends (Tempo/Jaeger)
	if hasTraceBackend && len(cfg.traceIDs) > 0 {
		var backendURL string
		var backendName string
		if cfg.tempoURL != "" {
			backendURL = cfg.tempoURL
			backendName = "Tempo"
		} else {
			backendURL = cfg.jaegerURL
			backendName = "Jaeger"
		}
		client := traceapi.New(backendURL)
		for _, traceID := range cfg.traceIDs {
			fmt.Fprintf(os.Stderr, "Fetching trace %s from %s (%s)...\n", traceID, backendName, backendURL)
			fetchedSpans, err := client.FetchTrace(traceID)
			if err != nil {
				printError(err, fmt.Sprintf("failed to fetch trace %s from %s", traceID, backendName))
				os.Exit(1)
			}
			traceSpans = append(traceSpans, fetchedSpans...)
			fmt.Fprintf(os.Stderr, "Fetched %d spans for trace %s\n", len(fetchedSpans), traceID)
		}
	}

	// 5. Run GHA Ingestor (only when URLs are provided)
	var results []analyzer.URLResult
	var globalEarliest, globalLatest int64
	var ghaSpans []sdktrace.ReadOnlySpan
	if len(args) > 0 {
		client := githubapi.NewClient(githubapi.NewContext(token))
		progress := tui.NewProgress(len(args), os.Stderr)
		progress.Start()

		ingestor := polling.NewPollingIngestor(client, args, progress, analyzer.AnalyzeOptions{
			Window:      cfg.window,
			NoArtifacts: cfg.noArtifacts,
			FetchLogs:   cfg.fetchLogs,
		})
		var err error
		results, globalEarliest, globalLatest, ghaSpans, err = ingestor.Ingest(ctx)

		progress.Finish()
		progress.Wait()

		if err != nil {
			printError(err, "ingestion failed")
			os.Exit(1)
		}
	}

	// 6. Combine all spans
	spans := append(ghaSpans, traceSpans...)
	// Update global time bounds from trace spans
	for _, s := range traceSpans {
		startMs := s.StartTime().UnixMilli()
		endMs := s.EndTime().UnixMilli()
		if globalEarliest == 0 || startMs < globalEarliest {
			globalEarliest = startMs
		}
		if endMs > globalLatest {
			globalLatest = endMs
		}
	}

	// Apply span filter
	if spanFilter != nil {
		before := len(spans)
		spans = spanFilter.Apply(spans)
		fmt.Fprintf(os.Stderr, "Filter: %d → %d spans\n", before, len(spans))
	}

	// Lint mode: analyze spans for semconv compliance
	if cfg.lintMode {
		lintData := buildLintData(spans)
		lintResults := enrichment.LintSpans(lintData)
		fmt.Fprint(os.Stderr, enrichment.FormatLintResults(lintResults))
	}

	if err := pipeline.Process(ctx, spans); err != nil {
		printError(err, "processing spans failed")
	}

	// If TUI mode is enabled, launch interactive TUI
	if cfg.tuiMode {
		// Handle perfetto export before TUI starts (so it opens immediately)
		if perfettoFile != "" {
			combined := analyzer.CalculateCombinedMetrics(results, sumRuns(results), collectStarts(results), collectEnds(results))
			var allTraceEvents []analyzer.TraceEvent
			for _, res := range results {
				allTraceEvents = append(allTraceEvents, res.TraceEvents...)
			}
			if err := perfetto.WriteTrace(os.Stderr, results, combined, allTraceEvents, globalEarliest, perfettoFile, cfg.openInPerfetto, spans); err != nil {
				printError(err, "writing perfetto trace failed")
			}
		}

		globalStartTime := time.UnixMilli(globalEarliest)
		globalEndTime := time.UnixMilli(globalLatest)

		// Create reload function that clears cache and refetches data
		reloadFunc := func(reporter tuiresults.LoadingReporter) ([]sdktrace.ReadOnlySpan, time.Time, time.Time, error) {
			var allSpans []sdktrace.ReadOnlySpan
			var reloadEarliest, reloadLatest int64

			// Re-read trace files
			if len(cfg.traceFiles) > 0 {
				if reporter != nil {
					reporter.SetPhase("Loading trace files")
				}
				for i, tf := range cfg.traceFiles {
					fileSpans, err := otlpfile.ParseFile(tf)
					if err != nil {
						return nil, time.Time{}, time.Time{}, fmt.Errorf("failed to load trace file %s: %w", tf, err)
					}
					urlIdx := len(args) + i
					allSpans = append(allSpans, tagSpansWithIndex(fileSpans, urlIdx)...)
				}
				for _, s := range allSpans {
					startMs := s.StartTime().UnixMilli()
					endMs := s.EndTime().UnixMilli()
					if reloadEarliest == 0 || startMs < reloadEarliest {
						reloadEarliest = startMs
					}
					if endMs > reloadLatest {
						reloadLatest = endMs
					}
				}
			}

			// Re-fetch from GitHub if URLs were provided
			if len(args) > 0 {
				if reporter != nil {
					reporter.SetPhase("Clearing cache")
				}

				if err := os.RemoveAll(githubapi.DefaultCacheDir()); err != nil {
					return nil, time.Time{}, time.Time{}, fmt.Errorf("failed to clear cache: %w", err)
				}

				var progressReporter analyzer.ProgressReporter
				if reporter != nil {
					progressReporter = &reloadProgressAdapter{reporter: reporter}
				}

				reloadClient := githubapi.NewClient(githubapi.NewContext(token))
				reloadIngestor := polling.NewPollingIngestor(reloadClient, args, progressReporter, analyzer.AnalyzeOptions{
					Window: cfg.window,
				})
				_, ghaEarliest, ghaLatest, reloadGHASpans, err := reloadIngestor.Ingest(ctx)
				if err != nil {
					return nil, time.Time{}, time.Time{}, err
				}

				allSpans = append(allSpans, reloadGHASpans...)
				if reloadEarliest == 0 || ghaEarliest < reloadEarliest {
					reloadEarliest = ghaEarliest
				}
				if ghaLatest > reloadLatest {
					reloadLatest = ghaLatest
				}
			}

			return allSpans, time.UnixMilli(reloadEarliest), time.UnixMilli(reloadLatest), nil
		}

		// Create function to open in Perfetto from TUI
		openPerfettoFunc := func(visibleSpans []sdktrace.ReadOnlySpan, activityHidden bool) {
			// Create temp file for perfetto trace
			tmpFile, err := os.CreateTemp("", "gha-trace-*.pftrace")
			if err != nil {
				return
			}
			tmpFile.Close()

			combined := analyzer.CalculateCombinedMetrics(results, sumRuns(results), collectStarts(results), collectEnds(results))
			var allTraceEvents []analyzer.TraceEvent
			for _, res := range results {
				allTraceEvents = append(allTraceEvents, res.TraceEvents...)
			}
			// Filter out legacy marker events if activity is hidden in the TUI
			if activityHidden {
				filtered := make([]analyzer.TraceEvent, 0, len(allTraceEvents))
				for _, ev := range allTraceEvents {
					if ev.Ph == "i" && ev.Pid == 999 {
						continue // skip review/merge marker events
					}
					filtered = append(filtered, ev)
				}
				allTraceEvents = filtered
			}
			_ = perfetto.WriteTrace(io.Discard, results, combined, allTraceEvents, globalEarliest, tmpFile.Name(), true, visibleSpans)
		}

		// Build input sources: GitHub URLs + trace file basenames
		inputSources := make([]string, 0, len(args)+len(cfg.traceFiles))
		inputSources = append(inputSources, args...)
		for _, tf := range cfg.traceFiles {
			inputSources = append(inputSources, filepath.Base(tf))
		}

		var tuiOpts []tuiresults.ModelOption
		if len(args) > 0 {
			logFetchClient := githubapi.NewClient(githubapi.NewContext(token))
			tuiOpts = append(tuiOpts, tuiresults.WithLogFetchFunc(
				makeLogFetchFunc(logFetchClient),
			))
		}

		if err := tuiresults.Run(spans, globalStartTime, globalEndTime, inputSources, reloadFunc, openPerfettoFunc, enricher, tuiOpts...); err != nil {
			fmt.Fprintf(os.Stderr, "%sError: TUI failed: %v%s\n", colorRed, err, colorReset)
			os.Exit(1)
		}
		return
	}

	// Non-TUI output
	combined := analyzer.CalculateCombinedMetrics(results, sumRuns(results), collectStarts(results), collectEnds(results))
	var allTraceEvents []analyzer.TraceEvent
	for _, res := range results {
		allTraceEvents = append(allTraceEvents, res.TraceEvents...)
	}

	switch cfg.outputFormat {
	case "markdown":
		output.OutputCombinedResultsMarkdown(os.Stdout, results, combined, allTraceEvents, globalEarliest, globalLatest, perfettoFile, cfg.openInPerfetto, spans, enricher)
	default:
		output.OutputStyledResults(os.Stderr, results, combined, allTraceEvents, globalEarliest, globalLatest, spans, enricher)
		// Handle perfetto export for styled output
		if perfettoFile != "" {
			perfetto.WriteTrace(os.Stderr, results, combined, allTraceEvents, globalEarliest, perfettoFile, cfg.openInPerfetto, spans)
		}
	}

	if err := pipeline.Finish(ctx); err != nil {
		printError(err, "finalizing pipeline failed")
	}

	if cfg.openInOTel {
		fmt.Println("Opening OTel Desktop Viewer...")
		_ = utils.OpenBrowser("http://localhost:8000")
	}
}

func sumRuns(results []analyzer.URLResult) int {
	total := 0
	for _, result := range results {
		total += result.Metrics.TotalRuns
	}
	return total
}

func collectStarts(results []analyzer.URLResult) []analyzer.JobEvent {
	var events []analyzer.JobEvent
	for _, result := range results {
		events = append(events, result.JobStartTimes...)
	}
	return events
}

func collectEnds(results []analyzer.URLResult) []analyzer.JobEvent {
	var events []analyzer.JobEvent
	for _, result := range results {
		events = append(events, result.JobEndTimes...)
	}
	return events
}

// makeLogFetchFunc creates a LogFetchFunc that uses the given GitHub client
// to fetch and parse step logs on demand from the TUI.
func makeLogFetchFunc(client *githubapi.Client) tuiresults.LogFetchFunc {
	return func(owner, repo string, jobID int64, existingSpans []sdktrace.ReadOnlySpan) ([]sdktrace.ReadOnlySpan, error) {
		ctx := context.Background()

		logData, err := client.FetchJobLog(ctx, owner, repo, jobID)
		if err != nil {
			return nil, fmt.Errorf("fetching job log: %w", err)
		}

		// Collect step spans belonging to this job, building step info for log splitting
		type stepInfo struct {
			span    sdktrace.ReadOnlySpan
			number  int
			attrs   map[string]string
		}
		var jobStepSpans []stepInfo

		for _, s := range existingSpans {
			attrs := make(map[string]string)
			for _, a := range s.Attributes() {
				attrs[string(a.Key)] = a.Value.Emit()
			}
			if attrs["type"] != "step" {
				continue
			}
			parentSpanID := s.Parent().SpanID()
			for _, js := range existingSpans {
				jAttrs := make(map[string]string)
				for _, a := range js.Attributes() {
					jAttrs[string(a.Key)] = a.Value.Emit()
				}
				if jAttrs["type"] == "job" && js.SpanContext().SpanID() == parentSpanID {
					if jAttrs["github.job_id"] == fmt.Sprintf("%d", jobID) {
						stepNum := 0
						if sn, ok := attrs["github.step_number"]; ok {
							fmt.Sscanf(sn, "%d", &stepNum)
						}
						jobStepSpans = append(jobStepSpans, stepInfo{span: s, number: stepNum, attrs: attrs})
					}
					break
				}
			}
		}

		if len(jobStepSpans) == 0 {
			return nil, nil
		}

		// Build Step slice for timestamp-based log splitting
		var apiSteps []githubapi.Step
		for _, si := range jobStepSpans {
			apiSteps = append(apiSteps, githubapi.Step{
				Name:        si.span.Name(),
				Number:      si.number,
				StartedAt:   si.span.StartTime().Format(time.RFC3339),
				CompletedAt: si.span.EndTime().Format(time.RFC3339),
			})
		}

		stepLogs := githubapi.SplitJobLogByStep(logData, apiSteps)
		if len(stepLogs) == 0 {
			return nil, nil
		}

		builder := &analyzer.SpanBuilder{}
		registry := logparse.DefaultRegistry()

		for _, si := range jobStepSpans {
			raw, ok := stepLogs[si.number]
			if !ok || len(raw) == 0 {
				continue
			}

			lines := logparse.ParseLogLines(raw)
			if len(lines) == 0 {
				continue
			}

			parserName, spans := registry.Parse(lines, si.span.StartTime(), si.span.EndTime())
			if len(spans) == 0 {
				continue
			}

			traceID := si.span.SpanContext().TraceID()
			stepSC := si.span.SpanContext()

			stepURL := ""
			if ghURL, ok := si.attrs["github.url"]; ok && ghURL != "" {
				stepURL = ghURL
			}

			analyzer.AddParsedSpansToBuilder(builder, spans, stepSC, traceID, jobID, si.span.Name(), parserName, 0, 0, stepURL)
		}

		return builder.Spans(), nil
	}
}

// tagSpansWithIndex wraps ReadOnlySpans with a github.url_index attribute
// so the TUI can group spans by their source file.
func tagSpansWithIndex(spans []sdktrace.ReadOnlySpan, urlIndex int) []sdktrace.ReadOnlySpan {
	stubs := tracetest.SpanStubsFromReadOnlySpans(spans)
	for i := range stubs {
		stubs[i].Attributes = append(stubs[i].Attributes, attribute.Int("github.url_index", urlIndex))
	}
	return stubs.Snapshots()
}

func printUsage() {
	fmt.Println("OTel Analyzer")
	fmt.Println("\nUsage:")
	fmt.Println("  ote <github_url1> [github_url2...] [token] [flags]")
	fmt.Println("  ote <trace_file.json> [flags]")
	fmt.Println("  ote convert <file1> [file2...] [flags]")
	fmt.Println("  ote trends <owner/repo> [flags]")
	fmt.Println("\nFlags:")
	fmt.Println("  --tui                     Force interactive TUI mode (default when terminal is available)")
	fmt.Println("  --no-tui                  Disable interactive TUI, use CLI output instead")
	fmt.Println("  --output=<format>         Output format: 'stdout' (styled terminal) or 'markdown' (implies --no-tui)")
	fmt.Println("  --perfetto=<file.pftrace> Save trace for Perfetto.dev analysis")
	fmt.Println("  --open-in-perfetto        Automatically open the generated trace in Perfetto UI")
	fmt.Println("  --otel                    Write OTel spans as JSON to stdout")
	fmt.Println("  --otel=<endpoint>         Export traces via OTLP/HTTP (default port: 4318)")
	fmt.Println("  --otel-grpc[=<endpoint>]  Export traces via OTLP/gRPC (default: localhost:4317)")
	fmt.Println("  --open-in-otel            Automatically open the OTel Desktop Viewer")
	fmt.Println("  --window=<duration>       Only show events within <duration> of merge/latest activity (e.g. 24h, 2h)")
	fmt.Println("  --trace=<file.json>       Load OTel spans from a trace file (can be repeated)")
	fmt.Println("  --tempo=<baseURL>         Fetch traces from Grafana Tempo (e.g., http://localhost:3200)")
	fmt.Println("  --jaeger=<baseURL>        Fetch traces from Jaeger v2 (e.g., http://localhost:16686)")
	fmt.Println("  --trace-id=<id>           Trace ID to fetch from Tempo/Jaeger (can be repeated)")
	fmt.Println("  --no-artifacts            Skip downloading and ingesting trace artifacts from workflow runs")
	fmt.Println("  --logs                    Fetch and parse step logs to create sub-step spans")
	fmt.Println("  --filter=<expr>           Filter spans by attributes (e.g., 'service.name=checkout,http.status_code=5*')")
	fmt.Println("  --errors-only             Only show spans with ERROR status")
	fmt.Println("  --listen[=<addr>]         Start OTLP/HTTP receiver (default: :4318)")
	fmt.Println("  --enrichment=<file>       Load custom enrichment rules from a JSON file")
	fmt.Println("  --lint                    Analyze spans for OTel semantic convention compliance")
	fmt.Println("  --clear-cache             Clear the HTTP cache (can be combined with other flags)")
	fmt.Println("  help, --help, -h          Show this help message")
	fmt.Println("\nTrends Mode Flags:")
	fmt.Println("  --days=<n>                Number of days to analyze (default: 30)")
	fmt.Println("  --format=<format>         Output format: 'terminal' or 'json' (default: terminal)")
	fmt.Println("  --branch=<name>           Filter by branch name (e.g., main, master)")
	fmt.Println("  --workflow=<file>         Filter by workflow file name (e.g., post-merge.yaml)")
	fmt.Println("  --no-sample               Fetch job details for all runs (disables statistical sampling)")
	fmt.Println("  --confidence=<0-1>        Confidence level for sampling (default: 0.95)")
	fmt.Println("  --margin=<0-1>            Margin of error for sampling (default: 0.10)")
	fmt.Println("\nConvert Mode:")
	fmt.Println("  Converts any supported trace format to OTel JSON on stdout.")
	fmt.Println("  Supported formats: Chrome Tracing, Jaeger, Zipkin, OTLP proto-JSON, stdouttrace, binary protobuf.")
	fmt.Println("\nEnvironment Variables:")
	fmt.Println("  GITHUB_TOKEN              GitHub PAT (alternatively pass as argument)")
	fmt.Println("\nExamples:")
	fmt.Println("  ote https://github.com/owner/repo/pull/123")
	fmt.Println("  ote https://github.com/owner/repo/actions/runs/12345")
	fmt.Println("  ote https://github.com/owner/repo/commit/sha --perfetto=trace.pftrace")
	fmt.Println("  ote https://github.com/owner/repo/pull/123 --no-tui")
	fmt.Println("  ote https://github.com/owner/repo/pull/123 --output=stdout")
	fmt.Println("  ote https://github.com/owner/repo/pull/123 --output=markdown > report.md")
	fmt.Println("  ote trends owner/repo")
	fmt.Println("  ote trends owner/repo --days=7 --format=json")
	fmt.Println("  ote trends owner/repo --branch=main --workflow=post-merge.yaml")
	fmt.Println("  ote trace.json                      # auto-detects OTel or Chrome Tracing format")
	fmt.Println("  ote chrome-profile.json spans.json   # multiple trace files as args")
	fmt.Println("  ote --trace=spans.json https://github.com/owner/repo/pull/123")
	fmt.Println("  ote --tempo=http://localhost:3200 --trace-id=abc123def456")
	fmt.Println("  ote --jaeger=http://localhost:16686 --trace-id=abc123def456")
	fmt.Println("  ote --listen                       # accept OTLP traces on :4318")
	fmt.Println("  ote trace.json --filter=service.name=checkout")
	fmt.Println("  ote trace.json --errors-only       # only show error spans")
	fmt.Println("  ote trace.json --lint              # check semconv compliance")
	fmt.Println("  ote trace.json --enrichment=rules.json")
	fmt.Println("  ote convert chrome-profile.json      # Chrome Tracing → OTel JSON")
	fmt.Println("  ote convert spans.json                # any format → OTel JSON")
	fmt.Println("  ote convert file1.json file2.json     # multiple files")
	fmt.Println("  cat trace.json | ote convert          # stdin → OTel JSON")
	fmt.Println("  ote --clear-cache")
}

// resolveGitHubToken returns a GitHub token from GITHUB_TOKEN env var or gh CLI.
func resolveGitHubToken() string {
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		return token
	}
	if ghPath, err := exec.LookPath("gh"); err == nil {
		if out, err := exec.Command(ghPath, "auth", "token").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	return ""
}

// isTerminal checks if stdout and stderr are connected to a terminal
func isTerminal() bool {
	// Check if stdout is a terminal using file mode
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// isStdinTerminal checks if stdin is connected to a terminal
func isStdinTerminal() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// buildLintData converts ReadOnlySpans into the simplified SpanData format for linting.
func buildLintData(spans []sdktrace.ReadOnlySpan) []enrichment.SpanData {
	var data []enrichment.SpanData
	for _, s := range spans {
		attrs := make(map[string]string)
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}
		data = append(data, enrichment.SpanData{
			Name:      s.Name(),
			Attrs:     attrs,
			SpanKind:  s.SpanKind().String(),
			ScopeName: s.InstrumentationScope().Name,
			HasEvents: len(s.Events()) > 0,
		})
	}
	return data
}
