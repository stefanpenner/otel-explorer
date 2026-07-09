# ote (otel-explorer)

Interactive terminal tool that turns OpenTelemetry traces and CI/CD runs into navigable timelines.

## Project Structure

- `cmd/ote/` — main CLI entry point (TUI app)
- `cmd/record-demo/` — demo recording helper
- `cmd/sample-eval/` — sample evaluation tool
- `pkg/analyzer/` — trace analysis, diff, spans, trends, metrics, typical runs
- `pkg/core/` — pipeline types and core abstractions
- `pkg/enrichment/` — span enrichment (CI/CD, exceptions, feature flags, lint, genai, resources)
- `pkg/export/` — output rendering (HTML, JSON, Slack, XLSX, DOCX, OTel)
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

## Build & Run (Bazel is primary)

```bash
bazel run //:gazelle          # after Go import changes
bazel build //:ote
bazel run //:ote -- <args>
```

`go build` / `go test` are secondary (IDE / quick loop only).
Do not finish a change until `bazel build //:ote` succeeds.
No Makefile.

## Test & Verify

```bash
bazel build //:ote                    # required final gate (strict deps)
bazel test //...                      # when package tests are wired
go test ./pkg/... ./cmd/...           # fast iteration only — not sufficient alone
bazel build //:ote && OTE=./bazel-bin/cmd/ote/ote_/ote \
  ./scripts/verify-sources.sh         # golden source verification
scripts/check-specs.sh                # TLA+ model checks (skips without a JVM)
```

## TLA+ Specs (specs/)

Model-checked specs for the concurrency/state-machine subsystems.
See `specs/README.md`, `specs/DECISION_CORES.md`, and `specs/FINDINGS.md`.
CI runs them via the `tla-specs` job (`scripts/check-specs.sh`).

Two layers:

1. **Full TLC specs** (`specs/<name>/<Spec>.tla`) — design model-checkers
   (records/quantifiers OK). Bait + mutation configs required.
2. **Decision cores** (`specs/<name>/decision/`) — scalar SMs for
   `specgen` → `pkg/.../<name>spec/`. Never hand-edit generated code.
   Dual/conform tests pin load-bearing guards to production helpers.

Rules for changes:

- Touching a modeled subsystem? Update full spec **and** decision core
  (if present), rerun `scripts/check-specs.sh <spec>`, re-`specgen`.
- Modeled subsystems → spec dir:
  store sync/watermark → `sync-bounds`; TUI reload/log-fetch → `tui-reload`;
  githubapi limiter/retry → `rate-limit`; analyzer run/job lifecycle →
  `gha-lifecycle`; analyzer time clamping → `timing-clamp`;
  tree/dedup → `span-tree`; logparse groups → `log-groups`.
- New concurrent/stateful design (queues, retries, invalidation,
  cancellation, "stale"/"race"/"in-flight" anywhere in the design)?
  Spec FIRST — an incoherent design should die in the spec, not in code.
  Prefer a decision core + `specgen` for pure guards; keep full TLC for
  multi-object interleavings.
- Never weaken a spec or property to silence a violation — report it.
- Regression tests derived from TLC witness traces are named
  `Test<Spec>Spec_*` — keep that convention so the spec↔test link stays visible.

## Architecture Notes

- Go 1.25, module path `github.com/stefanpenner/otel-explorer`
- TUI built with Bubble Tea (`charmbracelet/bubbles`, `bubbletea`, `lipgloss`)
- Errors via `cockroachdb/errors`
- Dual build: Bazel primary (`BUILD.bazel` throughout); `go build` secondary only
- Release via goreleaser (`.goreleaser.yml`)
- 624 test functions, 26 fuzz functions — good coverage, use them

## Code Conventions

- Package structure follows domain boundaries (analyzer, enrichment, ingest, export, etc.)
- Test files live alongside source (`foo.go` → `foo_test.go`, `foo_fuzz_test.go`)
- Golden/diff tests render output and assert markers appear in the result
- Fuzz tests use `testing.F` — run with `go test -fuzz=FuzzParseToAnalysis`
- No golangci-lint in CI — run `go vet` manually if needed
- Binary name is `ote`, not `otel-explorer`