# ote (otel-explorer)

Interactive terminal tool that turns OpenTelemetry traces and CI/CD runs into navigable timelines.

## Project Structure

- `cmd/ote/` — main CLI entry point (TUI app)
- `cmd/record-demo/` — demo recording helper
- `cmd/sample-eval/` — sample evaluation tool
- `pkg/analyzer/` — trace analysis, diff, spans, trends, metrics, typical runs
- `pkg/core/` — pipeline types and core abstractions
- `pkg/enrichment/` — span enrichment (CI/CD, exceptions, feature flags, lint, genai, resources)
- `pkg/export/` — output rendering (HTML, JSON, Slack, XLSX, DOCX, OTel, Perfetto)
- `pkg/ingest/` — trace ingestion (OTLP file, polling, receiver, trace API, webhook, filter)
- `pkg/logparse/` — log parsing (Gradle, Bazel, setup-job timestamps)
- `pkg/output/` — terminal output formatting, diff views, trends rendering
- `pkg/perfetto/` — Perfetto trace export
- `pkg/store/` — trace storage and sync
- `pkg/tui/` — Bubble Tea TUI (timelines, diff view, results)
- `pkg/githubapi/` — GitHub Actions API client, cache, log fetching
- `pkg/utils/` — shared utilities
- `docs/samples/` — sample trace files for testing
- `scripts/` — verification and demo scripts

## Build & Run

```bash
go build -o /tmp/ote ./cmd/ote
go test ./pkg/... ./cmd/...
```

No Makefile. Build with `go build` or `bazel build //cmd/ote:ote`.

## Test & Verify

```bash
go test ./pkg/... ./cmd/...           # unit + fuzz tests
OTE=/tmp/ote ./scripts/verify-sources.sh  # golden source verification
scripts/check-specs.sh                # TLA+ model checks (skips without a JVM)
```

Known failing test: `pkg/ingest/filter` — 2 tests fail (span attrs precedence). Fix before making changes to that package.

## TLA+ Specs (specs/)

Model-checked specs for the concurrency/state-machine subsystems.
See `specs/README.md` (conventions) and `specs/FINDINGS.md` (campaign results).
CI runs them via the `tla-specs` job (`scripts/check-specs.sh`).

Rules for changes:

- Touching a modeled subsystem? Update its spec to match, rerun
  `scripts/check-specs.sh <spec>`. The spec models the code —
  drift makes it worthless.
- Modeled subsystems → spec dir:
  store sync/watermark → `sync-bounds`; TUI reload/log-fetch → `tui-reload`;
  githubapi limiter/retry → `rate-limit`; analyzer run/job lifecycle →
  `gha-lifecycle`; analyzer time clamping → `timing-clamp`;
  tree/dedup → `span-tree`; logparse groups → `log-groups`.
- New concurrent/stateful design (queues, retries, invalidation,
  cancellation, "stale"/"race"/"in-flight" anywhere in the design)?
  Spec FIRST — an incoherent design should die in the spec, not in code.
- Never weaken a spec or property to silence a violation — report it.
- Regression tests derived from TLC witness traces are named
  `Test<Spec>Spec_*` — keep that convention so the spec↔test link stays visible.

## Architecture Notes

- Go 1.25, module path `github.com/stefanpenner/otel-explorer`
- TUI built with Bubble Tea (`charmbracelet/bubbles`, `bubbletea`, `lipgloss`)
- Errors via `cockroachdb/errors`
- Dual build: `go build` and Bazel (BUILD.bazel files throughout)
- Release via goreleaser (`.goreleaser.yml`)
- 570 test functions, 26 fuzz functions — good coverage, use them

## Code Conventions

- Package structure follows domain boundaries (analyzer, enrichment, ingest, export, etc.)
- Test files live alongside source (`foo.go` → `foo_test.go`, `foo_fuzz_test.go`)
- Golden/diff tests render output and assert markers appear in the result
- Fuzz tests use `testing.F` — run with `go test -fuzz=FuzzParseToAnalysis`
- No golangci-lint in CI — run `go vet` manually if needed
- Binary name is `ote`, not `otel-explorer`