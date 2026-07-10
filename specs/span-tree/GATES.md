# span-tree — gates (agent SSOT)

Load this for code changes. Full TLC for pipeline/cycles: `SpanTree.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Drop API twin | `dropAPIForRunnerTwin` → **DedupChoose** | DedupChoose | `Inv_RunnerWins` |
| Package | `pkg/analyzer` | `decision/Decision.tla` | `spantreespec` |

**SSOT:** twin shape runs generated `DedupChoose`; drop API when `kept=="runner"`.  
**Rule:** when group is exactly {1 API, 1 runner}, drop the API span.

**Check:** `scripts/decision-check.sh`
