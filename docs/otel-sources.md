# OpenTelemetry sources `ote` can visualize

`ote` is not just a GitHub Actions viewer — it renders **any** OpenTelemetry
trace as a navigable waterfall. It recognizes the major OTel semantic
conventions and surfaces the attribute that matters for each span type (the
HTTP route, the SQL statement, the LLM model and token count, the CI/CD task
result) inline in the timeline, instead of showing an anonymous bar.

This page catalogs the sources it understands, with runnable examples. Every
sample below lives in [`docs/samples/`](./samples) — point `ote` at one to
reproduce the screenshot:

```bash
ote docs/samples/rag-api.jsonl --no-tui
```

The enrichment chain tries the most specific convention first and falls back to
a generic OTel reader, so an unknown span still renders with whatever
semantic-convention detail it carries:

```
GitHub Actions  →  OTel CI/CD  →  GenAI (LLM)  →  Generic (HTTP/DB/RPC/messaging/FaaS)
```

---

## At a glance

| Source | Trigger attributes | Icon | Inline detail |
|---|---|---|---|
| GitHub Actions | `github.*` | `▶ ⚙ ↳` | conclusion, required, queue time |
| OTel CI/CD | `cicd.pipeline.*` | `🚀 🔨 🧪 ▶ ⚙` | task type, run result |
| GenAI — chat / completion | `gen_ai.*` | `🤖` | operation, model, `in→out tok`, `error.type` |
| GenAI — embeddings | `…operation.name=embeddings` | `🔢` | model, token count |
| GenAI — tool call | `…operation.name=execute_tool` | `🔧` | `gen_ai.tool.name` |
| GenAI — agent / workflow | `…=invoke_agent` / `create_agent` / `invoke_workflow` | `🧠` | `gen_ai.agent.name` |
| GenAI — retrieval | `…operation.name=retrieval` | `🔎` | provider |
| GraphQL | `graphql.operation.*` | `◆` | `type name` |
| HTTP | `http.request.method` | `⇄` | `METHOD route → host:port [status]` |
| Database | `db.system` | `⛁` | `system: statement` |
| RPC / gRPC | `rpc.system` | `⇌` | `system service/method` |
| Messaging | `messaging.system` | `✉` | `system destination (operation)` |
| FaaS | `faas.trigger` | `λ` | `function (trigger)` |
| Generic span | any | `● ⇣ ⇢ ⇡ ⇠` | span-kind aware |

Whatever the source, the full attribute set — including int/bool values such as
`gen_ai.usage.input_tokens` or `http.response.status_code` — is browsable in the
TUI inspector, grouped by dotted prefix, alongside span events (with exception
type/message/stacktrace), span links, resource attributes, and the
instrumentation scope.

---

## Hero example: a RAG API request (mixed sources in one trace)

A single request that fans out across HTTP, Postgres, an embeddings model, a
vector store, an LLM (with a failed-then-retried call), and a Kafka publish —
all distinct OTel sources, rendered in one waterfall:

```
╭────────────────────────────────────────────────────────────────────────────────────────╮
│ Trace Analyzer                                                                         │
│ Workflows: – • Jobs: 83.3%                                   1 runs • 6 jobs • 0 steps │
│ Wall: 4s • Compute: 4s                                                  Concurrency: 2 │
╰────────────────────────────────────────────────────────────────────────────────────────╯

  Pipeline Timelines
  ────────────────────
┌──────────────────────────────────────────────────────────────┐
│ Start: 10:00:00   End: 10:00:04   Duration: 4s               │
├──────────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████████  │ ⇄  POST /v1/answer  POST /v1/answer → rag.internal:8080 [200] (4s)
│ ██                                                           │   ⛁  SELECT users  postgresql: SELECT * FROM users WHERE id = $1 (200ms)
│   █████                                                      │   🔢  embeddings text-embedding-3-small  80 tok in (400ms)
│         █████                                                │   ⛁  vector query  pinecone: query (400ms)
│              ██                                              │   🤖  chat claude-opus-4  overloaded_error ❌ (200ms)
│                  ████████████████████████████████████████    │   🤖  chat claude-opus-4  chat claude-opus-4-20250101 · 4.2k→620 tok (3s)
│                                                          █   │   ✉  publish answers  kafka answers (publish) (100ms)
└──────────────────────────────────────────────────────────────┘
```

> The LLM call dominates the request (3s of 4s), the first attempt failed with
> `overloaded_error`, and the successful retry consumed 4.2k input / 620 output
> tokens — all readable without opening a single span.

`ote docs/samples/rag-api.jsonl`

---

## GenAI / LLM (`gen_ai.*`)

OTel's GenAI semantic conventions are emitted by the Anthropic and OpenAI SDKs,
OpenLLMetry, OpenInference, LangChain, LlamaIndex, and similar instrumentation.
`ote` surfaces the operation, the subject (the model actually served —
`gen_ai.response.model` preferred over `gen_ai.request.model` — or the tool/
agent name for tool and agent spans), token usage compacted as `in→out tok`,
and any `error.type`. Each `gen_ai.operation.name` gets a distinct icon:

| Operation | Icon | Subject shown |
|---|---|---|
| `chat`, `text_completion`, `generate_content` | `🤖` | model |
| `embeddings` | `🔢` | model |
| `execute_tool` | `🔧` | `gen_ai.tool.name` |
| `create_agent`, `invoke_agent`, `invoke_workflow` | `🧠` | `gen_ai.agent.name` |
| `retrieval` | `🔎` | provider |

An agent loop — invoke → tool call → retrieval → synthesize — where token usage
makes the expensive call obvious at a glance:

```
┌──────────────────────────────────────────────────────────────┐
│ Start: 10:00:00   End: 10:00:09   Duration: 9s               │
├──────────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████████  │ 🧠  invoke_agent research-agent (9s)
│ ██████████████████                                           │   🤖  chat claude-opus-4  1.8k→210 tok (2s)
│                    ███                                       │   ⇄  GET /search  GET /search [200] (500ms)
│                       ████████████████████████████████████   │   🤖  chat claude-opus-4  12.5k→1.4k tok (5s)
└──────────────────────────────────────────────────────────────┘
```

When a trace contains LLM calls, `ote` prints an **LLM Usage** summary above
the timeline so the total model cost of a request is visible without summing
spans by hand — call count, aggregate input→output tokens, and a per-model
breakdown (wrapper spans like `invoke_agent`/`execute_tool`, which carry no
model or tokens, are excluded from the call count):

```
  LLM Usage
  ───────────
  3 calls · 4.3k → 620 tokens
    claude-opus-4 ×1
    claude-opus-4-20250101 ×1
    text-embedding-3-small ×1
```

Key attributes: `gen_ai.system`, `gen_ai.operation.name`,
`gen_ai.request.model`, `gen_ai.response.model`, `gen_ai.tool.name`,
`gen_ai.agent.name`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`,
`error.type`.

`ote docs/samples/llm-agent.jsonl`

---

## GraphQL (`graphql.operation.*`)

GraphQL server spans usually ride on an HTTP `POST /graphql`, which would
otherwise hide the actual operation behind a single route. `ote` recognizes the
GraphQL convention first (`◆`) and shows `type name`
(e.g. `query DashboardData`, `mutation createOrder`), so the operation — not the
endpoint — is what you read. The example below combines GraphQL with a nested
agent that calls a tool, a retrieval, and an LLM:

```
┌──────────────────────────────────────────────────────────────┐
│ Start: 10:00:00   End: 10:00:06   Duration: 6s               │
├──────────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████████  │ ◆  query DashboardData (6s)
│ ████                                                         │   ⛁  SELECT widgets  postgresql: SELECT * FROM widgets WHERE owner = $1 (400ms)
│     ███████████████████████████████████████████████████████  │   🧠  invoke_agent summarizer (5s)
│      ██████████                                              │     🔧  execute_tool web_search (1s)
│                █████                                         │     🔎  retrieval vector-store  retrieval langchain (500ms)
│                     ███████████████████████████████████████  │     🤖  chat claude-opus-4  8.8k→540 tok (3s)
└──────────────────────────────────────────────────────────────┘
```

Key attributes: `graphql.operation.type` (`query` / `mutation` /
`subscription`), `graphql.operation.name`, `graphql.document`.

`ote docs/samples/graphql-tools.jsonl`

---

## OTel CI/CD (`cicd.pipeline.*`)

The vendor-neutral CI/CD conventions (OTel v1.27+) emitted by Jenkins, GitLab
CI, Dagger, Buildkite, Gradle, and others. The pipeline is the root; tasks
become children, iconed by `cicd.pipeline.task.type` (`🔨` build, `🧪` test,
`🚀` deploy) and colored by `cicd.pipeline.task.run.result`.

```
┌──────────────────────────────────────────────────────────────┐
│ Start: 10:00:00   End: 10:06:00   Duration: 6m               │
├──────────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████████  │ 🚀  release (6m)
│████████████████████████                                      │   🔨  build (2m 25s)
│                         █████████████████████████            │   🧪  test ❌ (2m 30s)
│                                                  ██████████  │   🚀  deploy (1m)
└──────────────────────────────────────────────────────────────┘
```

Key attributes: `cicd.pipeline.name`, `cicd.pipeline.type`,
`cicd.pipeline.task.name`, `cicd.pipeline.task.type`,
`cicd.pipeline.task.run.result`, plus VCS context (`vcs.ref.head.name`,
`vcs.revision`) shown in the inspector.

`ote docs/samples/cicd-pipeline.jsonl`

---

## HTTP, Database, RPC, Messaging, FaaS (generic semconv)

The generic reader recognizes the common OTel span conventions and extracts a
human-readable detail string for each — visible inline in the hero trace above:

- **HTTP** (`http.request.method`): `POST /v1/answer → rag.internal:8080 [200]`;
  4xx/5xx status codes color the bar red.
- **Database** (`db.system`): `postgresql: SELECT * FROM users WHERE id = $1`
  (statement truncated to 80 chars), or `system: operation table`.
- **RPC** (`rpc.system`): `grpc UserService/GetUser`.
- **Messaging** (`messaging.system`): `kafka answers (publish)`.
- **FaaS** (`faas.trigger`): `my-function (http)`.

Spans with none of these still render, using `otel.span_kind` to vary the icon
(`⇣` server, `⇢` client, `⇡` producer, `⇠` consumer).

---

## Errors & exceptions (`exception` span events)

Per the OTel spec, recording an exception on a span does **not** change the
span's status — so a span that captured an error but left its status unset would
otherwise look fine in the waterfall. `ote` reads the `exception` span event and
folds it onto the span: the bar turns red (`❌`) and the `exception.type` is
shown inline, so failures surface in the timeline, not just deep in the
inspector. An explicit `OK` status is respected (a handled exception stays
green) but the type is still surfaced.

Here `charge-card` carried only an exception event with no error status, yet the
declined payment is now impossible to miss:

```
┌──────────────────────────────────────────────────────────────┐
│ Start: 10:00:00   End: 10:00:02   Duration: 2s               │
├──────────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████████  │ ⇄  POST /checkout (2s)
│   █████████                                                  │   ●  validate-cart (300ms)
│            ████████████████████████████████████              │   ●  charge-card  PaymentDeclined ❌ (1s)
│                                                ████████████  │   ✉  send-receipt  sqs receipts (publish) (400ms)
└──────────────────────────────────────────────────────────────┘
```

The full exception — type, message, and stacktrace (line by line) — remains
browsable under the span's **Events** in the TUI inspector.

`ote docs/samples/exceptions.jsonl`

---

## Ingestion: where traces can come from

The conventions above are recognized regardless of how the spans arrive:

- **Trace files** — OTLP JSON (newline-delimited or array), OTLP protobuf,
  Chrome trace JSON, Jaeger JSON, Zipkin JSON (`ote trace.json` or `--trace=`).
- **Live OTLP receiver** — `ote --listen` accepts OTLP/HTTP on `:4318`.
- **Trace backends** — Grafana Tempo (`--tempo`) and Jaeger v2 (`--jaeger`)
  with `--trace-id=`.
- **GitHub Actions** — a PR/commit URL is fetched and modeled as spans.

---

## Checking your own instrumentation

`ote --lint <trace>` flags spans that violate OTel semantic conventions, and
`--filter='service.name=checkout,http.status_code=5*'` / `--errors-only` narrow
a noisy trace down to what you care about before rendering.

`--filter` matches against span attributes, resource attributes, the span name
(`otel.span_name`), and the status (`otel.status_code=ERROR`); integer-valued
attributes such as `http.status_code` are matched by value, so `5*` catches all
5xx responses. `--errors-only` keeps any span with `ERROR` status **or** a
recorded `exception` event — including spans that captured an exception without
setting an error status:

```bash
ote docs/samples/exceptions.jsonl --errors-only --no-tui
# Filter: 4 → 1 spans   →   ●  charge-card  PaymentDeclined ❌ (1s)
```
