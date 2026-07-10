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
scripts/decision-check.sh             # lean TLA gate (wires + decision tags)
scripts/check-specs.sh                # full TLC (skips without a JVM)
```

## TLA+ Specs (specs/)

**Day-to-day SSOT:** `specs/GATES.md` → `specs/<core>/GATES.md` only.  
Full TLC / decision layout / findings: `specs/README.md`, `DECISION_CORES.md`, `FINDINGS.md`.  
CI: `tla-specs` job runs `scripts/check-specs.sh`.

```bash
scripts/decision-check.sh              # lean: wires + decision-tagged tests
bazel run  //tools/decision:update     # Decision.tla → Go *spec + Rust modules
bazel test //tools/decision:up_to_date # Go + Rust codegen freshness
cargo test -p decision_cores           # Rust gates + duals
```

Rules (short):

- Code change in a modeled area → load that core's **GATES**, cite **symbols**
  (not `file.go:line`). Prefer decision cores; full TLC only for race redesign.
- Never hand-edit generated `*spec/spec.go`. Never weaken a spec to silence a fail.
- New queues/retries/stale/races? Spec first (decision core + `specgen` for pure
  guards). After `.tla` change: `decision:update` then `decision-check.sh`.

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