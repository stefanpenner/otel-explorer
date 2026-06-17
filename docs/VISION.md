# otel-explorer Vision

**See where your CI/CD time actually goes — statistically, not anecdotally.**

GitHub Actions natively shows per-run logs and billing minutes, and nothing else: no
duration trends, no failure-pattern history, no queue-time visibility. Commercial tools
(Datadog CI Visibility, Buildkite Test Engine, Trunk, BuildPulse, Mergify) fill that gap
with hosted dashboards. `ote` fills it from the terminal, with nothing but a GitHub token —
and speaks OpenTelemetry natively so the same analysis plugs into any observability stack.

## Pillars

### 1. Statistical CI analysis, not single-run anecdotes

A repo with 1,000 weekly commits has no "representative run". The unit of insight is the
distribution:

- **Typical Run view** *(shipped)* — per-job Gantt aggregated across a statistical sample:
  bars at median start offset and p50 duration, shaded extensions to p75/p95, presence
  rate, pass rate, and per-segment trend direction. Run-level metrics stay exact; job-level
  detail is stratified-sampled at a configurable confidence/margin (95% / ±10% default), so
  a 1,000-commit week summarizes from ~90 fetched runs with bounded error.
- **p50 + p95 always paired** — the industry-consensus pairing (CircleCI, GitLab, Datadog
  guidance): median tracks the everyday experience, p95 tracks the worst-case pain.
  Averages are never headline numbers.
- **Regression → commit attribution** *(shipped: changepoint detection)* — change-point
  detection on per-job duration series, reported as "job +31% since `abc123`" with a
  compare URL. Pinning a slowdown to a commit is the single most actionable output.
- **Exemplars** *(shipped)* — typical-view p50/p95 values hyperlink to the real run
  nearest that percentile, so aggregate statistics stay grounded in concrete,
  inspectable runs. Run-vs-typical diff answers "why was this run slow?" per job.
- **Flake scoring** *(next)* — adopt the industry-standard definitions, both computable
  purely from the GitHub API: same-SHA-different-conclusion (Mergify/BuildPulse) as the
  flaky flag, plus a transition-count score (Buildkite: state changes over a sliding window
  on the default branch) for ranking.
- **Queue time as a first-class series** *(shipped in trends; extend to bars)* — render
  created→started as a dimmed prefix segment on each job bar. A queue-time regression
  means "buy runners", not "optimize code"; conflating them corrupts both signals.
- **Waste metrics** *(later)* — minutes burned re-running the same SHA, staircase gaps
  where independent jobs ran sequentially, cache-step durations. Industry analyses put
  flaky-rerun waste at 15–25% of CI spend.
- **Faceting** *(later)* — split distributions by branch, trigger event, runner label, and
  actor. Mixing populations (PR runs vs. scheduled runs) corrupts percentiles.

### 2. Spec-faithful OpenTelemetry

Every `cicd.*` / `vcs.*` / `test.*` convention is still Development-stability (semconv
v1.41.x), so the strategy is: centralize attribute names in one package, target the
current spec, and track renames deliberately.

- **Span model** — root span per `(run_id, run_attempt)` with `SpanKind=SERVER` named
  `{action} {pipeline}`; jobs and steps as `INTERNAL` children carrying
  `cicd.pipeline.task.*`; `cicd.pipeline.result` (Required) + `error.type` on failures;
  span status per the Recording Errors guidance (Error only for failure; Unset for
  cancelled/skipped).
- **Deterministic IDs, githubreceiver-compatible** — adopt the collector-contrib
  githubreceiver hash scheme (`traceID = sha256("{run_id}{run_attempt}t")[0:32]`, etc.) so
  `ote` traces correlate with telemetry emitted from inside Actions jobs and with the
  official receiver.
- **Retries as linked traces** — one trace per attempt, with a span Link from attempt N's
  root to attempt N−1's trace, plus `cicd.pipeline.run.previous_attempt.url.full`.
- **Queue spans + pending-state metrics** — synthetic `queue-{job}` spans
  (receiver-compatible) and `cicd.pipeline.run.duration` histogram observations
  partitioned by `cicd.pipeline.run.state=pending` (the spec-compliant queue-time metric).
- **Metrics for trends** — emit the v1.30.0 metric set (`cicd.pipeline.run.duration`,
  `.active`, `.errors`, `cicd.worker.count`, `cicd.system.errors`) so trend analysis can
  feed any metrics backend, not just the TUI.
- **Where spec and receiver diverge**, prefer spec names; optionally dual-emit
  receiver-style names behind a compat flag.

### 3. A frugal, resilient GitHub API strategy

The sampling features are bounded by API budget, so the fetch layer is a feature:

- **GraphQL batching for commit sweeps** — aliased `repository.object(oid:)` blocks with
  nested `checkSuites { workflowRun, checkRuns { startedAt completedAt } }` fetch ~100
  commits per query at ~1–12 rate-limit points; a 1,000-commit sample costs ~10–120 of the
  5,000/hr GraphQL budget versus ~1,010 REST requests. Alternatively
  `ref.target.history(since:, until:)` walks the window and embeds check suites in one
  paginated query. Caveat: GraphQL `WorkflowRun` has no duration field — timing comes from
  `CheckRun.startedAt/completedAt`.
- **Timestamps, never `/timing`** — the per-run timing and billing endpoints are closing
  down. Durations come from `run_started_at`→`updated_at` and per-job timestamps.
- **ETag conditional requests** — 304s are free against the primary REST limit, so cached
  re-sampling of mostly-immutable per-commit run lists is nearly cost-free.
- **Secondary-limit aware pacing** — ≤900 REST points/min, low concurrency, honor
  `retry-after` and `x-ratelimit-reset`, and self-throttle GraphQL via
  `rateLimit { cost remaining resetAt }`.
- **Model `run_attempt` explicitly** — head_sha-filtered results include re-runs, which
  skew duration distributions unless deduplicated or modeled as retries.

## Roadmap sketch

| Horizon | Work |
| --- | --- |
| Shipped (2026-06) | Typical Run view; 57-finding correctness wave; per-workflow calibrated sampling (`cmd/sample-eval` methodology); 8-way concurrent fetches; `ote sync` SQLite store with exact store-backed trends; Hourly Patterns; exemplar links on percentiles; run-vs-typical diff; `scripts/e2e-smoke.sh` reality matrix |
| Next | GraphQL batch fetcher for commit sweeps; same-SHA flake flag + transition-count score; queue prefix segments on bars; TUI interactive polish |
| Later | Waste metrics (re-run burn, idle staircase gaps); faceting by branch/event/runner; critical-path highlighting with slack; OTel metrics emission; MCP/JSON surface so agents can consume the same statistics the TUI renders |

## Non-goals

- A hosted service or persistent backend — `ote` stays a single binary over public APIs
  plus optional OTLP export.
- Replacing Perfetto/Honeycomb-class deep-dive UIs — `ote` exports to them instead.
- Stable-semconv guarantees while the CI/CD conventions are Development-stability; we
  track the spec, we don't freeze it.
