# Native-OTel runner on ARC (kind) — WORKING ✅

The PR [actions/runner#4366](https://github.com/actions/runner/pull/4366) runner
(native OpenTelemetry export) runs as the ARC runner image in the local `kind`
cluster, and k8s job/step spans flow to the in-cluster Collector → Tempo.

## Verified end-to-end

A `[self-hosted, kind]` job runs on the ARC ephemeral runner (image
`otel-runner:dev`) and emits one trace to `otel-collector.observability.svc`:

```
github-actions-runner            (service.name)
  test-runner                    ← job span
    Set up job / Resolve actions/checkout@v4 / Checkout code /
    Display runner information / Test basic commands / Test git /
    Success message / Post Checkout code / Complete job   ← step spans
```

Spans carry CICD semconv (`cicd.pipeline.run.id`, `cicd.pipeline.task.*`,
`...result`, run URLs) **and** k8s context via the Downward API
(`k8s.pod.name`, `k8s.namespace.name`, `k8s.node.name`). Confirmed in the
in-cluster Tempo.

## The one real blocker (and the fix)

**Symptom:** runner pods spun up then were torn down in seconds; ARC went into a
permanent `phase: Outdated` delete→recreate loop; jobs stayed `queued`; no spans.

**Root cause:** the runner pod exited with **code 7** = `RunnerVersionDeprecated`.
The GitHub Actions service denies an ephemeral/JIT runner whose reported version
is below the current minimum (`AccessDeniedException` → exit 7 when ARC sets
`ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE`). ARC reads exit-7 as
"outdated" (`ephemeralrunner_controller.go`) and loops.

PR#4366 branches off runner **2.333.0**, which the service now treats as
deprecated in the JIT path. (A *persistent* PAT-registered runner at 2.333.0 still
works — only the ephemeral/JIT path enforces this.)

**Fix:** report a current version. `otel-runner.Dockerfile` sets
`ARG RUNNER_VERSION_OVERRIDE=2.335.1` (matches the official ARC image) before the
layout build. With that, the runner registers, runs jobs, and exports — phase
stays `Running`, no loop.

> Note: this corrects an earlier guess that a custom runner `template:` / the
> `command:` field caused the loop. It did not — every spawned runner was exiting
> 7 regardless of template shape. Once the version is current, the full template
> (custom image + Downward-API k8s attrs, no `command` override needed since the
> image bakes `CMD [run.sh]`) is stable.

## Deploy

```bash
# in the runner repo (PR#4366 checkout):
docker build -f otel-runner.Dockerfile -t otel-runner:dev .
kind load docker-image otel-runner:dev --name gha-runner

# here:
helm upgrade arc-runner-set \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --version 0.14.1 -n arc-runners -f otel-runner-upgrade-values.yaml
```

Then dispatch a `[self-hosted, kind]` workflow; the trace lands in the in-cluster
Tempo as `service.name=github-actions-runner`.

## Files
- `otel-runner.Dockerfile`, `.dockerignore` — in the runner repo (build recipe).
- `otel-runner-upgrade-values.yaml` — working Helm values (image + OTel + k8s attrs).
