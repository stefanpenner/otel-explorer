# Native-OTel runner on ARC (kind) — status & findings

Goal: run the PR [actions/runner#4366](https://github.com/actions/runner/pull/4366)
runner (native OpenTelemetry export) as the ARC runner image in the local `kind`
cluster, so k8s job/step spans flow to the in-cluster Collector → Tempo.

## What works ✅

- **Image build** — `otel-runner.Dockerfile` (in the runner repo) builds a
  linux-arm64 runner *from source* (PR#4366, v2.333.0, commit `60ab864`) and
  overlays it onto the official `ghcr.io/actions/actions-runner` image, keeping
  the official k8s hooks / entrypoint. `CMD [/home/runner/run.sh]` is baked in.
- **Runner binary verified** — standalone the image runs, reports the right
  version/commit, and correctly consumes the ARC JIT handoff
  (`Removing env var: ACTIONS_RUNNER_INPUT_JITCONFIG` → `Adding Command: run`).
- **Deploy mechanics** — `kind load docker-image otel-runner:dev` +
  `helm ... -f otel-runner-upgrade-values.yaml` swaps the runner image and sets
  `ACTIONS_RUNNER_OTLP_ENDPOINT=http://otel-collector.observability.svc:4318`.
- The **local (non-k8s) path is fully proven**: the same PR#4366 runner exports
  job+step spans end-to-end into Tempo + Jaeger (see otel-e2e-example Part 3).

## Blocker ❌ — ARC chart hash-reconcile loop on a custom runner template

On `gha-runner-scale-set` **0.14.1**, supplying a custom runner `template:`
(custom image/env) drives the AutoscalingRunnerSet into a permanent
`phase: Outdated` delete→recreate loop **as soon as a runner is instantiated**
(min`Runners>=1`, or when a job arrives). Symptoms:
- runner pods spin up then are torn down in seconds (controller deems the
  EphemeralRunnerSet "outdated" every reconcile),
- ARC runners never reach `online`, jobs stay `queued`,
- no spans reach the Collector.

Isolation done:
- `minRunners: 0` + custom template → phase stays `Running` (dormant; no runner
  created) but jobs still never run (loop fires when a job triggers a runner).
- adding `command: [/home/runner/run.sh]` in values → loop (hence baking CMD
  into the image instead).
- every `helm upgrade` re-triggers the loop; even a fresh `helm install` with the
  custom template goes `Outdated`.

This is an ARC/chart hash-stability issue with custom pod templates, **not** an
OTel or runner-binary problem (the binary + JIT both verified working).

## Next steps to try

1. Pin the runner build to the chart's expected runner version (2.335.x) — build
   from a PR#4366 rebase on that tag — in case a version skew aggravates the hash.
2. Try a newer `gha-runner-scale-set` chart (>0.14.1); the custom-template hash
   bug may be fixed upstream.
3. Or set the image via the chart's supported field/path rather than a full
   `template.spec.containers[0]` override, to avoid the hash flap.
4. Compare the rendered EphemeralRunner spec (desired vs stored) to find the exact
   field the API-server defaults differently.

## Files

- `otel-runner.Dockerfile`, `.dockerignore` — in the runner repo (build recipe).
- `otel-runner-upgrade-values.yaml` — full Helm values (image + k8s OTel attrs).
- Restore the cluster to stock with the original values (stock image, minRunners 0).
