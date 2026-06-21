<p align="center">
  <strong>ote</strong> (otel-explorer)<br>
  See where your CI/CD time actually goes.
</p>

<p align="center">
  <a href="#install">Install</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#features">Features</a> &middot;
  <a href="#trends">Trends</a> &middot;
  <a href="#opentelemetry">OpenTelemetry</a>
</p>

---

An interactive terminal tool that turns OpenTelemetry traces and CI/CD runs into navigable timelines — so you can find the slow jobs, the flaky tests, and the queue-time bottlenecks. Works with GitHub Actions, Jenkins, GitLab CI, Buildkite, Dagger, and any system that emits OTel traces.

![Interactive TUI with timeline visualization](docs/demo.svg)

## Install

```bash
brew install stefanpenner/tap/ote
```

Or install the latest binary with `curl` (macOS and Linux):

```bash
curl -fsSL https://raw.githubusercontent.com/stefanpenner/otel-explorer/main/install.sh | sh
```

Or with Go:

```bash
go install github.com/stefanpenner/otel-explorer/cmd/ote@latest
```

## Quick Start

Point it at any PR or commit:

```bash
ote nodejs/node/pull/60369
```

That's it. If you have [GitHub CLI](https://cli.github.com/) installed and authenticated, the token is picked up automatically. Otherwise:

```bash
export GITHUB_TOKEN="your_token_here"
```

## Features

### Interactive TUI

The default view is a full-screen terminal UI with a tree of workflows, jobs, and steps on the left and a Gantt-style timeline on the right. Navigate with arrow keys or vim bindings, expand/collapse nodes, multi-select ranges, search, and drill into details.

### Perfetto Export

Export any analysis as a [Perfetto](https://ui.perfetto.dev) trace for deep-dive visualization with full zoom, search, and flame-chart views:

```bash
ote <url> --perfetto=trace.pftrace --open-in-perfetto
```

### Trace Backend Integration

Pull traces directly from Grafana Tempo or Jaeger:

```bash
ote --tempo=http://localhost:3200 --trace-id=abc123
ote --jaeger=http://localhost:16686 --trace-id=abc123
```

### Webhook Input

Pipe a GitHub Actions webhook payload to analyze the associated commit — useful for event-driven analysis:

```bash
echo '{"workflow_run":{"head_sha":"abc123"},"repository":{"full_name":"owner/repo"}}' \
  | ote --otel
```

### Enrichment

Beyond raw timings, the analyzer enriches spans with:

- **Queue time** — how long jobs waited for a runner
- **Runner distribution** — which runners ran which jobs
- **Billable minutes** — computed cost breakdown
- **Retry detection** — identifies re-run jobs and counts attempts
- **PR annotations** — review approvals, comments, merge events shown as markers on the timeline
- **CI/CD pipeline recognition** — auto-classifies spans using [OTel CI/CD semantic conventions](https://opentelemetry.io/docs/specs/semconv/cicd/) (`cicd.pipeline.*` attributes)

## Trends

Analyze workflow performance over time to spot regressions, flaky jobs, and slow-downs:

```bash
ote trends owner/repo                          # last 30 days
ote trends owner/repo --days=7 --branch=main   # scoped
ote trends owner/repo --format=json             # machine-readable
```

```
================================================================================
  Historical Trend Analysis: stefanpenner/otel-explorer
================================================================================

Summary Statistics
------------------
Average Duration                        1m 46s
Median Duration                         1m 41s
95th Percentile                         3m 13s
Average Success Rate                     61.7%
Trend Direction           Improving (-20.7%)
Flaky Jobs Detected                          1
```

Trend analysis covers success rates, duration percentiles, per-job breakdowns, flaky detection (>10% failure rate), and trend direction. Run-level metrics are always exact (run listings are cheap). Job-level detail is sampled **per workflow**: every workflow gets up to 50 temporally-stratified observations (20 for workflows under 1% of total compute), so per-job percentiles stay honest while API cost scales with the number of workflows instead of the size of the window. Targets follow `--margin` (`0.10` → 50/20; `0.05` → 100/40) and were calibrated against full-scan ground truth on nodejs/node and rails/rails with `cmd/sample-eval`: worst-job p50 error ≲10% and p95 error ≲30% at the defaults, versus up to 84% p95 error for a same-size global sample. Job fetches run 8-way concurrent, well inside GitHub's secondary rate limits.

### Typical Run

For busy repos (hundreds of commits a week), no single run is representative. The **Typical Run** section aggregates the sampled runs into the *statistically typical* pipeline, grouped per workflow: each job is drawn at its median start offset, with the bar shaded from median duration (`█`) through p75 (`▓`) to p95 (`░`) so the right tail — the variance that actually hurts — is visible at a glance:

```
▸ CI  9/75 runs sampled — run p50 6m 25s  p95 10m 19s
                          0──────────────────────────────────────12m
build                     ████████▓▓░░                            → p50 3m   p95 4m 12s
test (linux)                      ██████████████▓▓▓▓░░░░          ⚠ p50 5m   p95 8m 40s
test (macos)                      ████████████▓▓░                 → p50 4m   p95 5m 30s
deploy-preview                                  ███▓░             → p50 1m   in 40% of runs  92% pass
```

Each segment also reports its presence rate (how often it appears at all), pass rate, and trend direction; skipped and cancelled jobs are excluded so they can't pollute the statistics. The header shows how many distinct commits the sample covered — a 1,000-commit week summarizes from ~90 sampled runs while still bounding the error statistically.

```bash
ote trends owner/repo --no-sample               # exact, more API calls
ote trends owner/repo --margin=0.05            # tighter sampling (100/40 obs targets)
```

### Incremental sync

For repos you analyze repeatedly, mirror the run/job history into a local SQLite store (`~/.local/share/ote/ote.db`):

```bash
ote sync owner/repo --days=7
```

Syncs are incremental: completed runs never change, so a re-sync lists only what's newer than the stored watermark and fetches job detail only for runs the store doesn't hold — a rails/rails week costs ~90s once, then ~10s and zero job-detail API calls to stay current. Once a repo is synced, `ote trends` automatically analyzes from the store: **exact** job detail for every run (no sampling), full commit coverage, ~10s end-to-end. Branch/workflow filters and `--no-sample`/`--dump-runs` still use the API path.

## OpenTelemetry

Export analysis data as OpenTelemetry spans — feed them into any observability stack:

```bash
# JSON spans to stdout
ote <url> --otel

# OTLP/HTTP
ote <url> --otel=localhost:4318

# OTLP/gRPC
ote <url> --otel-grpc=localhost:4317
```

You can also **ingest** OTel trace files from any CI/CD system — Jenkins, GitLab CI, Buildkite, Dagger, and anything else that emits traces following the [OTel CI/CD semantic conventions](https://opentelemetry.io/docs/specs/semconv/cicd/):

```bash
ote --trace=spans.json
```

`ote` isn't limited to CI/CD — it renders **any** OTel trace, recognizing the
major semantic conventions and surfacing the attribute that matters for each
span: GenAI/LLM model and token usage, HTTP routes, SQL, gRPC methods, GraphQL
operations, messaging destinations, exceptions, feature flags, and per-service
deployment context. See **[docs/otel-sources.md](docs/otel-sources.md)** for the
full catalog with runnable examples and screenshots.

## Development

Built with [Bazel](https://bazel.build/) for hermetic, reproducible builds.

```bash
bazel run //:ote -- <url>             # run
bazel build //...                     # build all
bazel test //...                      # test all
bazel run //:gazelle                  # regenerate BUILD files
```

## License

MIT
