# span-tree — gates (agent SSOT)

Load this for code changes. Full TLC for pipeline/cycles: `SpanTree.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Drop API twin | `dropAPIForRunnerTwin` | DedupChoose | `Inv_RunnerWins` |
| Package | `pkg/analyzer` | `decision/Decision.tla` | `spantreespec` |

**Rule:** when group is exactly {1 API, 1 runner}, drop the API span.

**Check:** `scripts/decision-check.sh`
