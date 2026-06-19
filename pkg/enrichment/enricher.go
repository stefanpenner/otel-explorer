package enrichment

// SpanHints contains presentation-relevant properties extracted from span attributes
// by an enricher. Lives on TreeNode and flows through to TreeItem and rendering.
type SpanHints struct {
	Category     string // "workflow", "job", "step", "marker", "operation", "task", etc.
	Outcome      string // normalized: "success", "failure", "skipped", "pending", ""
	Icon         string // display icon (emoji/char)
	BarChar      string // timeline bar character
	Color        string // "green", "red", "yellow", "blue", "gray"
	URL          string // clickable link
	User         string // associated user
	EventType    string // for markers: "merged", "approved", etc.
	IsRequired   bool
	IsMarker     bool   // zero-duration event
	IsRoot       bool   // top-level span (workflow, root operation)
	IsLeaf       bool   // leaf-level span (step, leaf operation)
	SortPriority int    // tie-breaking (markers get -1)
	DedupKey     string // non-empty → deduplicate
	GroupKey     string // non-empty → group under synthetic parent ("activity" for markers)
	// CI/CD extended context
	VCSBranch   string // vcs.ref.head.name — branch context
	VCSRevision string // vcs.ref.head.revision — commit SHA
	VCSBaseRef  string // vcs.ref.base.name — base branch (PRs)
	VCSChangeID string // vcs.change.id — pull/merge request number
	RunID       string // cicd.pipeline.run.id or cicd.pipeline.task.run.id
	// Generic span detail (extracted from semconv attributes)
	Detail string // e.g. "GET /api/users" for HTTP, "SELECT * FROM users" for DB
	// Service context (from resource attributes)
	ServiceName string // service.name from resource
	Environment string // deployment.environment from resource
}

// Enricher extracts SpanHints from span name and attributes.
type Enricher interface {
	Enrich(name string, attrs map[string]string, isZeroDuration bool) SpanHints
}

// ChainEnricher tries enrichers in order; first non-empty Category wins.
type ChainEnricher struct {
	enrichers []Enricher
}

// NewChainEnricher creates a ChainEnricher from the given enrichers.
func NewChainEnricher(enrichers ...Enricher) *ChainEnricher {
	return &ChainEnricher{enrichers: enrichers}
}

// Enrich tries each enricher in order, returning the first non-empty result.
func (c *ChainEnricher) Enrich(name string, attrs map[string]string, isZeroDuration bool) SpanHints {
	for _, e := range c.enrichers {
		hints := e.Enrich(name, attrs, isZeroDuration)
		if hints.Category != "" {
			return hints
		}
	}
	return SpanHints{}
}

// DefaultEnricher returns the default enricher chain. Specific conventions are
// tried before the generic catch-all: GHA, then OTel CI/CD, then GenAI (LLM),
// then Generic.
func DefaultEnricher() *ChainEnricher {
	return NewChainEnricher(&GHAEnricher{}, &CICDEnricher{}, &GenAIEnricher{}, &GenericEnricher{})
}
