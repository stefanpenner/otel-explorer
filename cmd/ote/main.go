package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/core"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/export"
	otelexport "github.com/stefanpenner/otel-explorer/pkg/export/otel"
	perfettoexport "github.com/stefanpenner/otel-explorer/pkg/export/perfetto"
	"github.com/stefanpenner/otel-explorer/pkg/export/terminal"
	"github.com/stefanpenner/otel-explorer/pkg/githubapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/filter"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/polling"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/receiver"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/traceapi"
	"github.com/stefanpenner/otel-explorer/pkg/ingest/webhook"
	"github.com/stefanpenner/otel-explorer/pkg/logparse"
	"github.com/stefanpenner/otel-explorer/pkg/output"
	"github.com/stefanpenner/otel-explorer/pkg/perfetto"
	"github.com/stefanpenner/otel-explorer/pkg/store"
	"github.com/stefanpenner/otel-explorer/pkg/tui"
	tuiresults "github.com/stefanpenner/otel-explorer/pkg/tui/results"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// version is set by goreleaser via ldflags.
var version = "dev"

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

// wireAPIMeter feeds the client's live API-request count and remaining rate
// limit into the progress spinner.
func wireAPIMeter(p *tui.Progress, c *githubapi.Client) {
	p.SetStatsProvider(func() (requests, remaining int) {
		s := c.RequestStats()
		remaining = -1
		if s.RateLimitKnown {
			remaining = s.RateLimitRemaining
		}
		return s.NetworkRequests, remaining
	})
}

// printAPIMeter writes the final API-traffic summary to stderr, leaving stdout
// clean for analysis output / JSON / pipes.
func printAPIMeter(c *githubapi.Client) {
	fmt.Fprintln(os.Stderr, c.RequestStats().Summary())
}

type config struct {
	urls             []string
	traceFiles       []string // --trace=<file.json> OTel trace files
	tempoURL         string   // --tempo=<baseURL> Tempo backend
	jaegerURL        string   // --jaeger=<baseURL> Jaeger v2 backend
	traceIDs         []string // trace IDs to fetch from backends
	perfettoFile     string
	openInPerfetto   bool
	perfettoUI       string // custom Perfetto UI origin (or PERFETTO_UI_URL); default ui.perfetto.dev
	openInOTel       bool
	otelEndpoint     string
	otelStdout       bool
	otelGRPCEndpoint string
	tuiMode          bool
	outputFormat     string // stdout, markdown, otel, json, xlsx, doc, html, slack
	outFile          string // destination for binary formats (xlsx/doc); default chosen per format
	slackWebhook     string // when set with --output=slack, POST the payload here (or SLACK_WEBHOOK_URL)
	clearCache       bool
	window           time.Duration
	showHelp         bool
	showVersion      bool
	trendsMode       bool
	trendsRepo       string
	trendsDays       int
	trendsFormat     string
	trendsBranch     string
	trendsWorkflow   string
	trendsNoSample   bool
	trendsDumpRuns   string
	trendsMargin     float64
	trendsFacet      string
	syncMode         bool
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
		tuiMode:      terminal,
		trendsDays:   30, // default to 30 days
		trendsFormat: "terminal",
		trendsMargin: 0.10,
	}

	// The subcommand is the first non-flag argument, so global flags like
	// --clear-cache may precede it (`ote --clear-cache trends owner/repo`).
	subcommand := ""
	for i, a := range args {
		if strings.HasPrefix(a, "-") {
			continue
		}
		if a == "convert" || a == "trends" || a == "sync" {
			subcommand = a
			args = append(append([]string{}, args[:i]...), args[i+1:]...)
		}
		break // only the first non-flag argument can be a subcommand
	}

	if subcommand == "convert" {
		cfg.convertMode = true
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

	if subcommand == "trends" {
		cfg.trendsMode = true
	}
	if subcommand == "sync" {
		cfg.syncMode = true
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if arg == "help" || arg == "--help" || arg == "-h" {
			cfg.showHelp = true
			continue
		}
		if arg == "--version" || arg == "-v" {
			// Signal via config rather than exiting here: parseArgs is a pure,
			// unit-tested parser, and calling os.Exit mid-parse makes the path
			// untestable and would terminate any embedding caller. main()
			// prints the version and exits, mirroring showHelp.
			cfg.showVersion = true
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
			if !validOutputFormat(cfg.outputFormat) {
				return cfg, fmt.Errorf("invalid --output value: %s (must be 'stdout', 'markdown', 'otel', 'json', 'xlsx', 'doc', 'html', or 'slack')", cfg.outputFormat)
			}
			continue
		}
		if strings.HasPrefix(arg, "--out=") {
			cfg.outFile = strings.TrimPrefix(arg, "--out=")
			continue
		}
		if strings.HasPrefix(arg, "--slack-webhook=") {
			cfg.slackWebhook = strings.TrimPrefix(arg, "--slack-webhook=")
			continue
		}
		if strings.HasPrefix(arg, "--perfetto-ui=") {
			cfg.perfettoUI = strings.TrimPrefix(arg, "--perfetto-ui=")
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
			n, err := strconv.Atoi(days)
			if err != nil || n < 1 {
				return cfg, fmt.Errorf("invalid --days value: %s", days)
			}
			cfg.trendsDays = n
			continue
		}
		if strings.HasPrefix(arg, "--format=") {
			cfg.trendsFormat = strings.TrimPrefix(arg, "--format=")
			if !validTrendsFormat(cfg.trendsFormat) {
				return cfg, fmt.Errorf("invalid --format value: %s (must be 'terminal', 'json', 'xlsx', 'doc', 'html', or 'slack')", cfg.trendsFormat)
			}
			continue
		}
		if strings.HasPrefix(arg, "--out=") {
			cfg.outFile = strings.TrimPrefix(arg, "--out=")
			continue
		}
		if strings.HasPrefix(arg, "--slack-webhook=") {
			cfg.slackWebhook = strings.TrimPrefix(arg, "--slack-webhook=")
			continue
		}
		if strings.HasPrefix(arg, "--perfetto-ui=") {
			cfg.perfettoUI = strings.TrimPrefix(arg, "--perfetto-ui=")
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
		if strings.HasPrefix(arg, "--facet=") {
			cfg.trendsFacet = strings.TrimPrefix(arg, "--facet=")
			continue
		}
		if arg == "--no-sample" {
			cfg.trendsNoSample = true
			continue
		}
		if strings.HasPrefix(arg, "--dump-runs=") {
			cfg.trendsDumpRuns = strings.TrimPrefix(arg, "--dump-runs=")
			continue
		}
		if strings.HasPrefix(arg, "--confidence=") {
			// Sampling switched from a global confidence/margin formula to
			// per-workflow observation targets; only --margin tunes those.
			fmt.Fprintln(os.Stderr, "Warning: --confidence is deprecated and ignored; use --margin to tune sampling")
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
		if (cfg.trendsMode || cfg.syncMode) && cfg.trendsRepo == "" && !strings.HasPrefix(arg, "-") {
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

	// --output implies --no-tui regardless of flag order, so
	// `--output=markdown --tui` behaves the same as `--tui --output=markdown`.
	if cfg.outputFormat != "" {
		cfg.tuiMode = false
	}

	// Receiver mode never consults --output; reject the combination instead
	// of silently ignoring the flag.
	if cfg.listenAddr != "" && cfg.outputFormat != "" {
		return cfg, fmt.Errorf("--output is not supported with --listen (receiver mode); use --no-tui to disable the TUI")
	}

	if cfg.tempoURL != "" && cfg.jaegerURL != "" {
		return cfg, fmt.Errorf("--tempo and --jaeger cannot be used together; specify a single trace backend")
	}
	if (cfg.tempoURL != "" || cfg.jaegerURL != "") && len(cfg.traceIDs) == 0 {
		return cfg, fmt.Errorf("--tempo/--jaeger requires at least one --trace-id=<id>")
	}

	return cfg, nil
}

func main() {
	cfg, err := parseArgs(os.Args[1:], isTerminal())
	if err != nil {
		printErrorMsg(err.Error())
		os.Exit(1)
	}

	if cfg.showVersion {
		fmt.Println("ote", version)
		os.Exit(0)
	}

	if cfg.showHelp {
		printUsage()
		os.Exit(0)
	}

	// Point Perfetto trace-opening at a self-hosted UI when configured.
	perfetto.SetUIOrigin(resolvePerfettoUI(cfg.perfettoUI))

	// Gate raw ANSI/OSC escape emission on the destination of human-readable
	// output (stderr by default) being a terminal, honoring NO_COLOR.
	utils.SetColorEnabled(colorsEnabledFor(os.Stderr))

	// hadError tracks non-fatal failures (export/pipeline errors that are
	// reported but don't stop the run) so the process can exit non-zero.
	hadError := false

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

	// Handle sync mode: incrementally mirror run/job history into the
	// local store so repeat analyses are nearly API-free.
	if cfg.syncMode {
		if cfg.trendsRepo == "" {
			printErrorMsg("Sync requires a repository in format 'owner/repo'\n\n  Usage: ote sync owner/repo [--days=30]")
			os.Exit(1)
		}
		owner, repo, err := parseTrendsRepo(cfg.trendsRepo)
		if err != nil {
			printErrorMsg(err.Error())
			os.Exit(1)
		}
		token := resolveGitHubToken()
		if token == "" {
			printErrorMsg("GITHUB_TOKEN environment variable is required.\n  Tip: install the GitHub CLI (gh) and run `gh auth login` to authenticate automatically.")
			os.Exit(1)
		}
		dbPath, err := store.DefaultPath()
		if err != nil {
			printError(err, "resolving store path")
			os.Exit(1)
		}
		st, err := store.Open(dbPath)
		if err != nil {
			printError(err, "opening store")
			os.Exit(1)
		}
		defer st.Close()

		client := githubapi.NewClient(githubapi.NewContext(token))
		stats, err := store.Sync(context.Background(), client, st, owner, repo, cfg.trendsDays,
			func(msg string) { fmt.Fprintf(os.Stderr, "  %s\n", msg) })
		if err != nil {
			printError(err, "sync failed")
			os.Exit(1)
		}
		fmt.Printf("Synced %s/%s: %d runs listed, job detail fetched for %d runs (%d already stored)\n",
			owner, repo, stats.RunsFetched, stats.JobsFetched, stats.JobsSkipped)
		fmt.Printf("Store: %s\n", dbPath)
		return
	}

	// Handle trends mode
	if cfg.trendsMode {
		if cfg.trendsRepo == "" {
			printErrorMsg("Trends mode requires a repository in format 'owner/repo'\n\n  Usage: ote trends owner/repo [--days=30] [--format=terminal|json|xlsx|doc|html]\n\n  Run 'ote --help' for more information.")
			os.Exit(1)
		}

		// Parse owner/repo
		owner, repo, err := parseTrendsRepo(cfg.trendsRepo)
		if err != nil {
			printErrorMsg(err.Error())
			os.Exit(1)
		}

		token := resolveGitHubToken()
		if token == "" {
			printErrorMsg("GITHUB_TOKEN environment variable is required.\n  Tip: install the GitHub CLI (gh) and run `gh auth login` to authenticate automatically.")
			os.Exit(1)
		}

		// Signal-aware context so ctrl+c (and SIGTERM) cancels in-flight
		// fetches. The progress spinner runs the terminal in raw mode, so a
		// keyboard ctrl+c arrives as a keystroke rather than a SIGINT — it is
		// relayed to this context via progress.SetInterruptHandler below.
		ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stopSignals()
		client := githubapi.NewClient(githubapi.NewContext(token))

		// Repos previously opted in via `ote sync` analyze from the local
		// store: an incremental sync brings it current, then the analysis is
		// exact (full job detail) with near-zero API cost. Branch/workflow
		// filters and dump/no-sample knobs still use the API path.
		facets, facetErr := analyzer.ParseFacets(cfg.trendsFacet)
		if facetErr != nil {
			printError(facetErr, "trend analysis failed")
			os.Exit(1)
		}

		var analysis *analyzer.TrendAnalysis
		// Faceting needs head_branch/event/labels on every fetched run, so it
		// always uses the API path rather than the store.
		if cfg.trendsBranch == "" && cfg.trendsWorkflow == "" && cfg.trendsDumpRuns == "" && !cfg.trendsNoSample && cfg.trendsFacet == "" {
			analysis = trendsFromStore(ctx, client, owner, repo, cfg.trendsDays)
		}

		if analysis == nil {
			// Setup progress spinner for trends mode
			progress := tui.NewProgress(1, os.Stderr)
			progress.Start()
			progress.SetInterruptHandler(stopSignals)
			wireAPIMeter(progress, client)
			progress.StartURL(0, cfg.trendsRepo)

			// Perform trend analysis
			var err error
			analysis, err = analyzer.AnalyzeTrends(ctx, client, owner, repo, cfg.trendsDays, cfg.trendsBranch, cfg.trendsWorkflow, analyzer.TrendOptions{
				NoSample:      cfg.trendsNoSample,
				MarginOfError: cfg.trendsMargin,
				DumpRunsPath:  cfg.trendsDumpRuns,
				Facets:        facets,
			}, progress)

			progress.Finish()
			progress.Wait()

			if err != nil {
				if ctx.Err() != nil { // cancelled via ctrl+c / SIGTERM
					fmt.Fprintln(os.Stderr, "Interrupted.")
					os.Exit(130)
				}
				printError(err, "trend analysis failed")
				os.Exit(1)
			}
		}

		printAPIMeter(client)

		// Output results go to stdout (the spinner above stays on stderr) so
		// `ote trends owner/repo --format=json | jq .` and `> out.json` work.
		utils.SetColorEnabled(colorsEnabledFor(os.Stdout))
		if isExportFormat(cfg.trendsFormat) {
			rep := export.BuildTrendReport(analysis, generatedAt())
			if err := deliverReport(rep, cfg.trendsFormat, cfg.outFile, resolveSlackWebhook(cfg.slackWebhook)); err != nil {
				printError(err, "output failed")
				os.Exit(1)
			}
		} else if err := output.OutputTrends(os.Stdout, analysis, cfg.trendsFormat); err != nil {
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
	// GHAEnricher is attribute-gated (only spans carrying GHA-shaped attrs
	// match), so it belongs in every chain: traces ote itself exported and
	// re-ingested via files or the receiver carry those attrs too.
	enrichers = append(enrichers, &enrichment.GHAEnricher{})
	enrichers = append(enrichers, &enrichment.CICDEnricher{})
	// GenAIEnricher is attribute-gated on gen_ai.* — it claims LLM spans
	// (Anthropic/OpenAI SDKs, OpenLLMetry, LangChain, …) before the generic
	// catch-all so models and token usage surface.
	enrichers = append(enrichers, &enrichment.GenAIEnricher{})
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

		// Pure receiver mode: wait for user input or a signal to stop.
		if len(args) == 0 && len(cfg.traceFiles) == 0 && !hasTraceBackend {
			fmt.Fprintf(os.Stderr, "  Waiting for traces... (press Enter or Ctrl+C to stop)\n")
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			stdinCh := make(chan struct{})
			go func() {
				buf := make([]byte, 1)
				for {
					n, err := os.Stdin.Read(buf)
					if n > 0 {
						close(stdinCh)
						return
					}
					if err != nil {
						// Stdin is closed or redirected (e.g. </dev/null,
						// nohup, CI): an immediate EOF must not shut the
						// receiver down — rely on SIGINT/SIGTERM instead.
						return
					}
				}
			}()
			select {
			case <-stdinCh:
			case <-sigCh:
			}
			signal.Stop(sigCh)
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

		if cfg.tuiMode {
			globalStartTime := time.UnixMilli(globalEarliest)
			globalEndTime := time.UnixMilli(globalLatest)
			if err := tuiresults.Run(spans, globalStartTime, globalEndTime, []string{"receiver"}, nil, nil, enricher); err != nil {
				fmt.Fprintf(os.Stderr, "%sError: TUI failed: %v%s\n", colorRed, err, colorReset)
				os.Exit(1)
			}
		} else if len(spans) > 0 {
			// Render the same styled report as trace-file input — the
			// banner promises "stop and analyze collected spans".
			combined := analyzer.CombinedMetricsFromSpans(spans, enricher)
			colorsEnabled := colorsEnabledFor(os.Stderr)
			utils.SetColorEnabled(colorsEnabled)
			var styledW io.Writer = os.Stderr
			if !colorsEnabled {
				styledW = utils.NewStripANSIWriter(os.Stderr)
			}
			if err := output.OutputStyledResults(styledW, nil, combined, nil, globalEarliest, globalLatest, spans, enricher); err != nil {
				printError(err, "styled output failed")
				hadError = true
			}
		}
		if hadError {
			os.Exit(1)
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
		// A positional arg shaped like a GitHub token (legacy usage:
		// `ote <url> <token>`) is always treated as the token — even when
		// ambient auth (GITHUB_TOKEN / gh CLI) exists — so it is never
		// mistakenly passed to the ingestor as a URL.
		for i, arg := range args {
			if looksLikeGitHubToken(arg) {
				token = arg
				args = append(args[:i], args[i+1:]...)
				break
			}
		}
		if token == "" {
			token = resolveGitHubToken()
		}

		// Everything left must be a parsable GitHub URL: fail loudly instead
		// of silently consuming a bad arg as a token (which previously made
		// `ote owner/repo` print an empty report with exit 0).
		for _, arg := range args {
			if _, err := utils.ParseGitHubURL(arg); err != nil {
				printErrorMsg(err.Error())
				os.Exit(1)
			}
		}

		if len(args) == 0 && len(cfg.traceFiles) == 0 && !hasTraceBackend {
			printErrorMsg("No GitHub URLs or trace files provided (only a token).\n\n  Usage: ote <github_url> [flags]\n\n  Run 'ote --help' for more information.")
			os.Exit(1)
		}

		if len(args) > 0 && token == "" {
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
	var ghClient githubapi.GitHubProvider
	if len(args) > 0 {
		client := githubapi.NewClient(githubapi.NewContext(token))
		ghClient = client
		progress := tui.NewProgress(len(args), os.Stderr)
		progress.Start()
		wireAPIMeter(progress, client)

		ingestor := polling.NewPollingIngestor(client, args, progress, analyzer.AnalyzeOptions{
			Window:      cfg.window,
			NoArtifacts: cfg.noArtifacts,
			FetchLogs:   cfg.fetchLogs,
		})
		var err error
		results, globalEarliest, globalLatest, ghaSpans, err = ingestor.Ingest(ctx)

		progress.Finish()
		progress.Wait()
		printAPIMeter(client)

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
		hadError = true
	}

	// If TUI mode is enabled, launch interactive TUI
	if cfg.tuiMode {
		// Handle perfetto export before TUI starts (so it opens immediately)
		if perfettoFile != "" {
			combined := analyzer.CalculateCombinedMetrics(results, sumRuns(results), collectStarts(results), collectEnds(results))
			if combined.TotalRuns == 0 && len(spans) > 0 {
				combined = analyzer.CombinedMetricsFromSpans(spans, enricher)
			}
			var allTraceEvents []analyzer.TraceEvent
			for _, res := range results {
				allTraceEvents = append(allTraceEvents, res.TraceEvents...)
			}
			if err := perfetto.WriteTrace(os.Stderr, results, combined, allTraceEvents, globalEarliest, perfettoFile, cfg.openInPerfetto, spans); err != nil {
				printError(err, "writing perfetto trace failed")
				hadError = true
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
			if combined.TotalRuns == 0 && len(spans) > 0 {
				combined = analyzer.CombinedMetricsFromSpans(spans, enricher)
			}
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
		if hadError {
			os.Exit(1)
		}
		return
	}

	// Non-TUI output
	combined := analyzer.CalculateCombinedMetrics(results, sumRuns(results), collectStarts(results), collectEnds(results))
	if combined.TotalRuns == 0 && len(spans) > 0 {
		combined = analyzer.CombinedMetricsFromSpans(spans, enricher)
	}
	var allTraceEvents []analyzer.TraceEvent
	for _, res := range results {
		allTraceEvents = append(allTraceEvents, res.TraceEvents...)
	}

	switch cfg.outputFormat {
	case "json", "xlsx", "doc", "html", "slack":
		// Trace files / receiver inputs have no URL results; reconstruct runs
		// from spans so those inputs still produce a populated report.
		spanRuns := analyzer.RunsFromSpans(spans, enricher)
		rep := export.BuildRunReport(results, spanRuns, combined, globalEarliest, globalLatest, generatedAt())
		if err := deliverReport(rep, cfg.outputFormat, cfg.outFile, resolveSlackWebhook(cfg.slackWebhook)); err != nil {
			printError(err, "writing report")
			hadError = true
		}
	case "otel":
		exporter, err := otelexport.NewStdoutExporter(os.Stdout)
		if err != nil {
			printError(err, "creating OTel exporter")
			os.Exit(1)
		}
		if err := exporter.Export(ctx, spans); err != nil {
			printError(err, "exporting OTel spans")
			os.Exit(1)
		}
		if err := exporter.Finish(ctx); err != nil {
			printError(err, "finishing OTel export")
			os.Exit(1)
		}
	case "markdown":
		if err := output.OutputCombinedResultsMarkdown(os.Stdout, results, combined, allTraceEvents, globalEarliest, globalLatest, perfettoFile, cfg.openInPerfetto, spans, enricher); err != nil {
			printError(err, "markdown output failed")
			hadError = true
		}
	default:
		// --output=stdout sends the report to stdout (so `> report.txt`
		// works); the bare default keeps the legacy stderr destination.
		dest := os.Stderr
		if cfg.outputFormat == "stdout" {
			dest = os.Stdout
		}
		colorsEnabled := colorsEnabledFor(dest)
		utils.SetColorEnabled(colorsEnabled)
		var styledW io.Writer = dest
		if !colorsEnabled {
			// Also strip escapes produced by lipgloss styles, which detect
			// their color profile from stdout rather than the report writer.
			styledW = utils.NewStripANSIWriter(dest)
		}
		if err := output.OutputStyledResults(styledW, results, combined, allTraceEvents, globalEarliest, globalLatest, spans, enricher); err != nil {
			printError(err, "styled output failed")
			hadError = true
		}
		renderRunVsTypicalFromStore(styledW, results)
		// Handle perfetto export for styled output
		if perfettoFile != "" {
			if err := perfetto.WriteTrace(os.Stderr, results, combined, allTraceEvents, globalEarliest, perfettoFile, cfg.openInPerfetto, spans); err != nil {
				printError(err, "writing perfetto trace failed")
				hadError = true
			}
		}
	}

	// Seed the local store with the completed runs we just analyzed so the
	// typical-run baseline builds passively from ordinary usage. Runs after
	// rendering so the run-vs-typical comparison above reads a baseline that
	// excludes the current run.
	if ghClient != nil {
		persistRunsToStore(ctx, ghClient, results)
	}

	if err := pipeline.Finish(ctx); err != nil {
		printError(err, "finalizing pipeline failed")
		hadError = true
	}

	if cfg.openInOTel {
		fmt.Println("Opening OTel Desktop Viewer...")
		_ = utils.OpenBrowser("http://localhost:8000")
	}

	if hadError {
		os.Exit(1)
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
			span   sdktrace.ReadOnlySpan
			number int
			attrs  map[string]string
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
	fmt.Println("  ote sync <owner/repo> [--days=30]")
	fmt.Println("\nFlags:")
	fmt.Println("  --tui                     Force interactive TUI mode (default when terminal is available)")
	fmt.Println("  --no-tui                  Disable interactive TUI, use CLI output instead")
	fmt.Println("  --output=<format>         Output format: stdout, markdown, otel, json, xlsx, doc, html, slack (implies --no-tui)")
	fmt.Println("  --slack-webhook=<url>     With --output=slack, POST the message to this Slack webhook (or SLACK_WEBHOOK_URL)")
	fmt.Println("  --out=<file>              Destination file for binary formats (xlsx/doc); default: ote-report.<ext>")
	fmt.Println("  --perfetto=<file.pftrace> Save trace for Perfetto.dev analysis")
	fmt.Println("  --open-in-perfetto        Automatically open the generated trace in Perfetto UI")
	fmt.Println("  --perfetto-ui=<url>       Perfetto UI origin to open traces in (or PERFETTO_UI_URL); default https://ui.perfetto.dev")
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
	fmt.Println("  --format=<format>         Output format: terminal, json, xlsx, doc, html (default: terminal)")
	fmt.Println("  --branch=<name>           Filter by branch name (e.g., main, master)")
	fmt.Println("  --workflow=<file>         Filter by workflow file name (e.g., post-merge.yaml)")
	fmt.Println("  --facet=<dims>            Bucket comparison table: branch (upstream vs feature), event, runner.")
	fmt.Println("                            Comma-separate to cross dimensions (--facet=branch,event); 'all' = all three.")
	fmt.Println("  --no-sample               Fetch job details for all runs (disables statistical sampling)")
	fmt.Println("  --dump-runs=<file>        Write fetched run/job data as JSON (ground truth with --no-sample)")
	fmt.Println("  --confidence=<0-1>        Deprecated and ignored; use --margin")
	fmt.Println("  --margin=<0-1>            Margin of error for sampling (default: 0.10)")
	fmt.Println("  Output includes a Typical Run section: per-job median timeline with p75/p95 range bands,")
	fmt.Println("  presence and pass rates, aggregated across the sampled runs and commits.")
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
	fmt.Println("  ote https://github.com/owner/repo/pull/123 --output=json | jq '.run.runs[].jobs[]'")
	fmt.Println("  ote https://github.com/owner/repo/pull/123 --output=xlsx --out=run.xlsx")
	fmt.Println("  ote sync owner/repo --days=7      # mirror run/job history into the local store")
	fmt.Println("  ote trends owner/repo")
	fmt.Println("  ote trends owner/repo --days=7 --format=json")
	fmt.Println("  ote trends owner/repo --format=xlsx --out=trends.xlsx")
	fmt.Println("  ote trends owner/repo --format=html > trends.html")
	fmt.Println("  ote trends owner/repo --branch=main --workflow=post-merge.yaml")
	fmt.Println("  ote trends owner/repo --facet=branch         # upstream vs feature comparison")
	fmt.Println("  ote trends owner/repo --facet=branch,event   # crossed: per branch-bucket × trigger")
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

// looksLikeGitHubToken reports whether arg has the shape of a GitHub token:
// a modern prefixed token (ghp_, gho_, ghu_, ghs_, ghr_, github_pat_) or a
// legacy 40-character hex personal access token.
func looksLikeGitHubToken(arg string) bool {
	for _, prefix := range []string{"ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"} {
		if strings.HasPrefix(arg, prefix) {
			return true
		}
	}
	if len(arg) != 40 {
		return false
	}
	for _, r := range arg {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

// parseTrendsRepo extracts owner and repo from the trends repository argument,
// accepting "owner/repo", trailing slashes, and full GitHub URLs like
// "https://github.com/owner/repo".
func parseTrendsRepo(raw string) (string, string, error) {
	s := raw
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimPrefix(s, "www.")
	s = strings.TrimPrefix(s, "github.com/")
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '/' })
	if len(parts) < 2 {
		return "", "", fmt.Errorf("Invalid repository format: %s (expected 'owner/repo')", raw)
	}
	return parts[0], parts[1], nil
}

// colorsEnabledFor reports whether ANSI escape sequences should be written to
// f: NO_COLOR must be unset and f must be a terminal.
func colorsEnabledFor(f *os.File) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
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

// trendsFromStore analyzes trends from the local store when the repo has
// been synced before (see `ote sync`). Returns nil to fall back to the API
// sampling path: store not opted in, store errors, or an empty window.
func trendsFromStore(ctx context.Context, client githubapi.GitHubProvider, owner, repo string, days int) *analyzer.TrendAnalysis {
	dbPath, err := store.DefaultPath()
	if err != nil {
		return nil
	}
	if _, err := os.Stat(dbPath); err != nil {
		return nil // no store yet
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return nil
	}
	defer st.Close()

	wm, err := st.Watermark(owner, repo)
	if err != nil || wm.IsZero() {
		return nil // repo never synced
	}

	fmt.Fprintf(os.Stderr, "Using local store (last synced %s) — syncing...\n",
		utils.HumanizeTime(time.Since(wm).Seconds())+" ago")
	if _, err := store.Sync(ctx, client, st, owner, repo, days, nil); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: incremental sync failed (%v); analyzing stored data as-is\n", err)
	}

	now := time.Now()
	runs, err := st.LoadRuns(owner, repo, now.Add(-time.Duration(days)*24*time.Hour), now)
	if err != nil || len(runs) == 0 {
		return nil
	}
	return analyzer.AnalyzeTrendsFromRuns(owner, repo, days, runs)
}

// persistRunsToStore seeds the local store with the completed runs from a
// normal analysis, so the typical-run baseline grows from ordinary usage and
// not just explicit `ote sync`. It is opt-in: it only writes when a store
// already exists, so users who never sync incur no new file or fetch cost.
// In-progress runs are skipped by CollectCompletedRunData, keeping partial
// timings out of the baseline.
func persistRunsToStore(ctx context.Context, client githubapi.GitHubProvider, results []analyzer.URLResult) {
	if len(results) == 0 {
		return
	}
	dbPath, err := store.DefaultPath()
	if err != nil {
		return
	}
	if _, err := os.Stat(dbPath); err != nil {
		return // no store yet: don't create one unless the user opted into sync
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return
	}
	defer st.Close()

	for _, res := range results {
		if res.Owner == "" || res.Repo == "" || len(res.RawRuns) == 0 {
			continue
		}
		runData := analyzer.CollectCompletedRunData(ctx, client, res.RawRuns, nil)
		if len(runData) == 0 {
			continue
		}
		if err := st.UpsertRuns(res.Owner, res.Repo, runData); err != nil {
			return // best-effort: a write failure shouldn't fail the analysis
		}
	}
}

// renderRunVsTypicalFromStore compares the analyzed run's job durations
// against the repo's typical baseline from the local store (see `ote sync`)
// and prints notable deviations. Silently does nothing when the repo was
// never synced — the baseline must exist before "vs typical" means anything.
func renderRunVsTypicalFromStore(w io.Writer, urlResults []analyzer.URLResult) {
	if len(urlResults) == 0 {
		return
	}
	owner, repo := urlResults[0].Owner, urlResults[0].Repo
	if owner == "" || repo == "" {
		return
	}
	dbPath, err := store.DefaultPath()
	if err != nil {
		return
	}
	if _, err := os.Stat(dbPath); err != nil {
		return
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return
	}
	defer st.Close()
	if wm, err := st.Watermark(owner, repo); err != nil || wm.IsZero() {
		return
	}
	now := time.Now()
	runs, err := st.LoadRuns(owner, repo, now.AddDate(0, 0, -30), now)
	if err != nil || len(runs) == 0 {
		return
	}
	baseline := analyzer.AnalyzeTrendsFromRuns(owner, repo, 30, runs).Typical
	if baseline == nil {
		return
	}

	var observations []analyzer.JobObservation
	for _, res := range urlResults {
		if res.Owner != owner || res.Repo != repo {
			continue
		}
		for _, job := range res.Metrics.JobTimeline {
			if job.EndTime > job.StartTime {
				observations = append(observations, analyzer.JobObservation{
					Name:        job.Name,
					DurationSec: float64(job.EndTime-job.StartTime) / 1000.0,
				})
			}
		}
	}
	deltas := analyzer.CompareJobsToTypical(observations, baseline)
	output.RenderRunVsTypical(w, deltas, baseline.SampledRuns)
}
