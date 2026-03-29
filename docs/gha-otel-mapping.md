# GitHub Actions → OpenTelemetry Mapping Spec

## Principle

Treat GitHub Actions as an **ingestion format**, not a tracing system. Normalize immediately to OTel CI/CD semantic conventions. Produce valid, OTel-native traces that any OTLP backend can consume.

## Trace Identity

| GitHub Concept | OTel Mapping |
|---|---|
| Workflow Run (ID + attempt) | `trace_id` — deterministic from `md5(runID-attempt)` |
| Workflow Run | Root span (`parent = none`) |
| Job | Child span of workflow |
| Step | Child span of job |

All spans in a single workflow run share the same `trace_id`. Span IDs are deterministic from GitHub entity IDs for idempotent re-ingestion.

## Span Hierarchy

```
Trace (trace_id = hash(run_id, attempt))
└── Workflow Run (root span)
    ├── Job A
    │   ├── ⏳ Queued (child of job — queue wait time)
    │   ├── Step 1
    │   └── Step 2
    ├── Job B
    │   └── Step 1
    └── ⏳ Workflow Queued (child of workflow — workflow queue time)
```

## Semantic Conventions

### Workflow Span (root)

| Attribute | Value | Source |
|---|---|---|
| `cicd.pipeline.name` | workflow name | `run.Name` |
| `cicd.pipeline.run.id` | run ID string | `run.ID` |
| `cicd.pipeline.run.url.full` | actions run URL | constructed |
| `cicd.pipeline.run.result` | success/failure/cancelled | `run.Conclusion` |
| `vcs.repository.url.full` | `https://github.com/owner/repo` | constructed |
| `vcs.ref.head.name` | branch name | from fetch |
| `vcs.revision` | head SHA | `run.HeadSHA` |
| `github.run_attempt` | attempt number | `run.RunAttempt` (only if >1) |

**Resource attributes** (set on all spans via InstrumentationScope resource):

| Attribute | Value |
|---|---|
| `service.name` | `github-actions` |
| `service.version` | ote version |
| `cicd.pipeline.name` | workflow name |
| `vcs.repository.url.full` | repo URL |

### Job Span

| Attribute | Value | Source |
|---|---|---|
| `cicd.pipeline.task.name` | job name | `job.Name` |
| `cicd.pipeline.task.type` | `build` | default |
| `cicd.pipeline.task.run.id` | job ID string | `job.ID` |
| `cicd.pipeline.task.run.url.full` | job URL | `job.HTMLURL` |
| `cicd.pipeline.task.run.result` | success/failure/skipped/cancelled | `job.Conclusion` |
| `github.runner_name` | runner name | `job.RunnerName` |
| `github.is_required` | true/false | branch protection check |

**Span events on job span** (annotations from failed check runs):

| Event Name | Attributes |
|---|---|
| annotation title | `annotation.path`, `annotation.line`, `annotation.level`, `annotation.message` |

### Step Span

| Attribute | Value | Source |
|---|---|---|
| `cicd.pipeline.task.name` | step name | `step.Name` |
| `cicd.pipeline.task.type` | `build` | default |
| `cicd.pipeline.task.run.result` | success/failure/skipped | `step.Conclusion` |
| `github.step_number` | step number | `step.Number` |
| `cicd.pipeline.task.run.url.full` | step URL | constructed |

### Queue Spans

Queue wait time is modeled as a child span with:

| Attribute | Value |
|---|---|
| `cicd.pipeline.task.name` | `queue` |
| `cicd.pipeline.task.type` | `queue` |
| `github.queue_time_ms` | duration in ms |

## Events (NOT fake spans)

Reviews, comments, merges, commits, and pushes become **span events on the workflow root span**, not separate marker spans.

| GitHub Event | OTel Event Name | Key Attributes |
|---|---|---|
| PR review (approved) | `github.review` | `github.review.state=APPROVED`, `github.user` |
| PR review (changes_requested) | `github.review` | `github.review.state=CHANGES_REQUESTED`, `github.user` |
| PR comment | `github.comment` | `github.user`, `github.url` |
| PR merged | `github.merge` | `github.user`, `github.pr_number` |
| Commit created | `github.commit` | `vcs.revision` |
| Commit pushed | `github.push` | `vcs.revision` |

**Breaking change**: These were previously emitted as zero-duration "marker" spans with `type=marker`. They are now span events on the root workflow span. The enricher chain still handles legacy marker spans from older trace files.

## Span Links

| Relationship | Link Direction |
|---|---|
| Retry (attempt > 1) | Link from new root span → previous attempt's root span |

## Span Status

| GitHub Conclusion | OTel Status Code | Description |
|---|---|---|
| `success` | `OK` | — |
| `failure` | `ERROR` | `"Job failed"` / `"Step failed"` |
| `cancelled` | `OK` | `"Cancelled"` |
| `skipped` | `OK` | `"Skipped"` |
| (in_progress) | `UNSET` | — |

## InstrumentationScope

All spans emitted by the GHA ingestion carry:

```
InstrumentationScope{
    Name:    "github.com/stefanpenner/otel-explorer/pkg/analyzer"
    Version: "" (could be build version)
}
```

## What stays GitHub-specific

These attributes have no OTel semconv equivalent:

- `github.run_attempt` — retry attempt number
- `github.runner_name` — runner machine name
- `github.is_required` — branch protection status
- `github.step_number` — step ordering
- `github.queue_time_ms` — queue duration
- `github.url_index` — multi-URL grouping (internal)
- `github.artifact_name` — artifact ingestion source
- `billable.*_ms` — GitHub billing data
