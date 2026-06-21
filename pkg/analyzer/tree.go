package analyzer

import (
	"sort"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"go.opentelemetry.io/otel/sdk/trace"
)

// SpanEvent represents an event attached to a span (e.g., exception, log).
type SpanEvent struct {
	Name  string
	Time  time.Time
	Attrs map[string]string
}

// SpanLink represents a link to another span (cross-trace causality).
type SpanLink struct {
	TraceID string
	SpanID  string
	Attrs   map[string]string
}

// TreeNode represents a node in the workflow/job/step hierarchy.
// This is a shared data structure used by both TUI and CLI rendering.
type TreeNode struct {
	Attrs     map[string]string    // raw span attributes
	Hints     enrichment.SpanHints // enrichment output
	Name      string
	StartTime time.Time
	EndTime   time.Time
	URLIndex  int // index of the input URL this node belongs to
	Children  []*TreeNode
	// OTel metadata surfaced for display
	Events  []SpanEvent // span events (exceptions, logs)
	Links   []SpanLink  // span links (cross-trace references)
	SpanID  string      // span ID
	TraceID string      // trace ID
	// InstrumentationScope
	ScopeName    string // instrumentation library name
	ScopeVersion string // instrumentation library version
	// Resource attributes
	ResourceAttrs map[string]string
}

// Duration returns the duration of this node
func (n *TreeNode) Duration() time.Duration {
	return n.EndTime.Sub(n.StartTime)
}

// SourceLabel returns a short, human-readable provenance for this span: which
// emitter produced it. Derived from resource service.name, the source attribute,
// and the instrumentation scope.
//   - a tool/app the runner propagated into (e.g. "jest") -> that service name
//   - the GitHub Actions runner                          -> "runner"
//   - the otel-explorer GitHub-API reconstruction         -> "github-api"
func (n *TreeNode) SourceLabel() string {
	svc := n.Hints.ServiceName
	if svc == "" {
		svc = n.ResourceAttrs["service.name"]
	}
	switch {
	case svc != "" && svc != runnerServiceName:
		return svc
	case svc == runnerServiceName || n.ScopeName == runnerScopeName:
		return "runner"
	case strings.Contains(n.ScopeName, "otel-explorer"):
		return "github-api"
	case n.ScopeName != "":
		return n.ScopeName
	default:
		return "github-api"
	}
}

// Standard OTel signals that identify the runner as the emitter (used instead of
// a custom "source" attribute, so any backend distinguishes the same way).
const (
	runnerScopeName   = "github.actions.runner"
	runnerServiceName = "github-actions-runner"
)

// BuildTreeFromSpans constructs a hierarchy of TreeNodes from OTel spans.
// Spans are filtered and enriched using the provided enricher.
func BuildTreeFromSpans(spans []trace.ReadOnlySpan, globalEarliest, globalLatest time.Time, enricher enrichment.Enricher) []*TreeNode {
	if len(spans) == 0 {
		return nil
	}

	// Collapse API/runner duplicates (same job/step from both the GitHub API
	// reconstruction and the runner). Idempotent, so safe on every rebuild —
	// including TUI reloads and log-fetch appends that bypass the initial combine.
	spans = DedupeRunnerSpans(spans)

	type spanWithHints struct {
		span  trace.ReadOnlySpan
		attrs map[string]string
		hints enrichment.SpanHints
	}

	filtered := []spanWithHints{}
	seenDedup := make(map[string]struct{})

	for _, s := range spans {
		attrs := make(map[string]string)
		for _, a := range s.Attributes() {
			// Emit() (not AsString()) so int/bool/double attributes — e.g.
			// gen_ai.usage.input_tokens, http.response.status_code — stringify
			// instead of collapsing to "" and vanishing from enrichment and the
			// inspector.
			attrs[string(a.Key)] = a.Value.Emit()
		}

		isZeroDuration := s.EndTime().Before(s.StartTime()) || s.EndTime().Equal(s.StartTime())
		hints := enricher.Enrich(s.Name(), attrs, isZeroDuration)

		// Skip spans the enricher doesn't recognize
		if hints.Category == "" {
			continue
		}

		// Filter by time bounds if provided
		if !globalEarliest.IsZero() && s.EndTime().Before(globalEarliest) {
			continue
		}
		if !globalLatest.IsZero() && s.StartTime().After(globalLatest) {
			continue
		}

		// Deduplicate using DedupKey from hints
		if hints.DedupKey != "" {
			if _, seen := seenDedup[hints.DedupKey]; seen {
				continue
			}
			seenDedup[hints.DedupKey] = struct{}{}
		}

		filtered = append(filtered, spanWithHints{span: s, attrs: attrs, hints: hints})
	}

	if len(filtered) == 0 {
		return nil
	}

	// Build one node per span, plus a (traceID, spanID) → node mapping for
	// parent lookup. Per the OTel spec span IDs are only unique within a
	// trace, so keying by span ID alone would let spans from one trace
	// attach under a same-ID parent in another. Building a node per span
	// (instead of per map key) also keeps spans with colliding IDs — e.g.
	// two same-named steps in one job — from silently overwriting each other.
	nodes := make(map[string]*TreeNode)
	nodeList := make([]*TreeNode, len(filtered))

	for i, sh := range filtered {
		spanID := sh.span.SpanContext().SpanID().String()

		urlIndex := 0
		for _, a := range sh.span.Attributes() {
			if string(a.Key) == "github.url_index" {
				urlIndex = int(a.Value.AsInt64())
				break
			}
		}

		// Extract span events
		var events []SpanEvent
		for _, e := range sh.span.Events() {
			eventAttrs := make(map[string]string)
			for _, a := range e.Attributes {
				eventAttrs[string(a.Key)] = a.Value.Emit()
			}
			events = append(events, SpanEvent{
				Name:  e.Name,
				Time:  e.Time,
				Attrs: eventAttrs,
			})
		}

		// Extract span links
		var links []SpanLink
		for _, l := range sh.span.Links() {
			linkAttrs := make(map[string]string)
			for _, a := range l.Attributes {
				linkAttrs[string(a.Key)] = a.Value.Emit()
			}
			links = append(links, SpanLink{
				TraceID: l.SpanContext.TraceID().String(),
				SpanID:  l.SpanContext.SpanID().String(),
				Attrs:   linkAttrs,
			})
		}

		// Extract InstrumentationScope
		scope := sh.span.InstrumentationScope()

		// Extract resource attributes
		resourceAttrs := make(map[string]string)
		if sh.span.Resource() != nil {
			for _, a := range sh.span.Resource().Attributes() {
				resourceAttrs[string(a.Key)] = a.Value.Emit()
			}
		}

		// Enrich hints with resource context
		if sh.hints.ServiceName == "" {
			if svc, ok := resourceAttrs["service.name"]; ok {
				sh.hints.ServiceName = svc
			}
		}
		if sh.hints.Environment == "" {
			if env, ok := resourceAttrs["deployment.environment"]; ok {
				sh.hints.Environment = env
			}
		}

		// Surface recorded exceptions and feature-flag evaluations (span
		// events) onto the span itself, so they are visible in the timeline,
		// not only in the inspector.
		var flags []string
		exceptionApplied := false
		for _, ev := range events {
			if !exceptionApplied {
				if excType := enrichment.ExceptionTypeFromEvent(ev.Name, ev.Attrs); excType != "" {
					enrichment.ApplyException(&sh.hints, excType)
					exceptionApplied = true
				}
			}
			if f := enrichment.FeatureFlagFromEvent(ev.Name, ev.Attrs); f != "" {
				flags = append(flags, f)
			}
		}
		enrichment.ApplyFeatureFlags(&sh.hints, flags)

		node := &TreeNode{
			Attrs:         sh.attrs,
			Hints:         sh.hints,
			Name:          sh.span.Name(),
			StartTime:     sh.span.StartTime(),
			EndTime:       sh.span.EndTime(),
			URLIndex:      urlIndex,
			Children:      []*TreeNode{},
			Events:        events,
			Links:         links,
			SpanID:        spanID,
			TraceID:       sh.span.SpanContext().TraceID().String(),
			ScopeName:     scope.Name,
			ScopeVersion:  scope.Version,
			ResourceAttrs: resourceAttrs,
		}
		nodeList[i] = node
		key := node.TraceID + "/" + spanID
		if _, exists := nodes[key]; !exists {
			nodes[key] = node
		}
	}

	// Link children to parents
	var roots []*TreeNode
	for i, sh := range filtered {
		parentID := sh.span.Parent().SpanID().String()
		node := nodeList[i]

		if parentID == "0000000000000000" {
			roots = append(roots, node)
		} else if parent, ok := nodes[node.TraceID+"/"+parentID]; ok && parent != node {
			parent.Children = append(parent.Children, node)
		} else {
			// Parent not in this batch, treat as root
			roots = append(roots, node)
		}
	}

	// Sort all nodes by start time
	sortTreeNodes(roots)
	for _, node := range nodeList {
		sortTreeNodes(node.Children)
	}

	return roots
}

// sortTreeNodes sorts nodes by start time, using SortPriority for tie-breaking
func sortTreeNodes(nodes []*TreeNode) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].StartTime.Equal(nodes[j].StartTime) {
			// Tie-breaker: lower SortPriority first (markers have -1)
			if nodes[i].Hints.SortPriority != nodes[j].Hints.SortPriority {
				return nodes[i].Hints.SortPriority < nodes[j].Hints.SortPriority
			}
		}
		return nodes[i].StartTime.Before(nodes[j].StartTime)
	})
}

// FlattenTree flattens the tree into a list with depth information
type FlatNode struct {
	Node  *TreeNode
	Depth int
}

func FlattenTree(roots []*TreeNode) []FlatNode {
	var result []FlatNode
	var flatten func(nodes []*TreeNode, depth int)
	flatten = func(nodes []*TreeNode, depth int) {
		for _, node := range nodes {
			result = append(result, FlatNode{Node: node, Depth: depth})
			flatten(node.Children, depth+1)
		}
	}
	flatten(roots, 0)
	return result
}
