# Stacking the runner OTel export with actions-runner-controller (ARC)

This example wires three layers into one observable trace:

```
ARC schedule/scale-up  →  runner pod  →  job  →  step  →  in-job tool
   (k8s + scaling metrics)   (k8s.*)    (SERVER) (INTERNAL) (propagated)
```

Each layer is standard OpenTelemetry, so the data is fully useful in **any**
backend (Jaeger, Grafana Tempo, Honeycomb). otel-explorer just adds a nicer view
on top (pod/source provenance, the ARC link, scaling context).

## Pieces

| File | What it does |
|---|---|
| `gha-runner-scale-set-values.yaml` | Helm values: turn on the runner's OTLP export + attach `k8s.*` via the Downward API. Zero chart changes. |
| `otel-collector.yaml` | Collector: receive runner OTLP, scrape ARC's `gha_*` Prometheus metrics → OTLP, enrich with `k8sattributes`. |

## How the layers connect

1. **Runner → OTLP.** Setting `ACTIONS_RUNNER_OTLP_ENDPOINT` opts the runner into
   native export (traces + logs + metrics). `ACTIONS_RUNNER_OTLP_PROPAGATE=true`
   injects `TRACEPARENT` into each step so in-job tools nest.
2. **k8s context.** The runner honors the standard `OTEL_RESOURCE_ATTRIBUTES`
   env var; the Downward API fills it with `k8s.pod.name` / `k8s.namespace.name`
   / `k8s.node.name`. No runner-specific env names.
3. **ARC scaling metrics.** ARC only emits Prometheus (`gha_desired_runners`,
   `gha_running_jobs`, `gha_job_startup_duration_seconds`, …). The Collector's
   `prometheusreceiver` converts them to OTLP, so they sit alongside the traces.
4. **ARC → job link (optional).** If ARC injects a schedule-span context into the
   runner pod (`ACTIONS_RUNNER_PARENT_TRACEPARENT`), the runner's job span gets a
   **span link** to it — rendered as "Scheduled by controller". This needs an
   upstream ARC change (issue #1386); everything else works without it.

## View it

```sh
# traces from Tempo + the ARC/runner metrics
ote --tempo=http://tempo:3200 --trace-id=<run-trace-id> --metrics=<otlp-metrics-source>
```

In `ote` you'll see: the job under its **Pod** (inspector → Kubernetes section),
the in-job tool tagged with its source (`← jest`), and — once ARC injects the
context — a "Scheduled by controller" link on the job.

## Why no custom contract

- Runner spans are identified by `service.name` + instrumentation scope, not a
  bespoke attribute.
- IDs are deterministic but never advertised as W3C-random; the runner is the
  single source of truth to a backend (otel-explorer's API-reconstruction merge
  is local only).
- ARC metrics arrive as OTLP via the Collector — otel-explorer has no `gha_*`
  knowledge baked in.
