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

	// Idempotent, so a TUI reload or a log-fetch append may dedupe again.
	spans = DedupeRunnerSpans(spans)

	kept := filterSpans(spans, globalEarliest, globalLatest, enricher)
	if len(kept) == 0 {
		return nil
	}

	nodes, byID := nodesFromSpans(kept)
	roots, parentOf := linkParents(kept, nodes, byID)
	roots = detachParentCycles(roots, nodes, parentOf)
	sortTree(roots, nodes)
	return roots
}

// keptSpan is a span that survived filtering, with enrichment already computed.
type keptSpan struct {
	span  trace.ReadOnlySpan
	attrs map[string]string
	hints enrichment.SpanHints
}

func filterSpans(spans []trace.ReadOnlySpan, earliest, latest time.Time, enricher enrichment.Enricher) []keptSpan {
	kept := []keptSpan{}
	seen := make(map[string]struct{})
	for _, s := range spans {
		attrs := SpanEnrichmentAttrs(s)
		zero := s.EndTime().Before(s.StartTime()) || s.EndTime().Equal(s.StartTime())
		hints := enricher.Enrich(s.Name(), attrs, zero)
		if hints.Category == "" {
			continue
		}
		if !earliest.IsZero() && s.EndTime().Before(earliest) {
			continue
		}
		if !latest.IsZero() && s.StartTime().After(latest) {
			continue
		}
		if hints.DedupKey != "" {
			if _, ok := seen[hints.DedupKey]; ok {
				continue
			}
			seen[hints.DedupKey] = struct{}{}
		}
		kept = append(kept, keptSpan{span: s, attrs: attrs, hints: hints})
	}
	return kept
}

// nodesFromSpans builds one node per span. The map is keyed by traceID/spanID
// because span IDs are unique only within a trace. The first span keeps the
// key, so a later colliding ID (two same-named steps) is not overwritten.
func nodesFromSpans(kept []keptSpan) ([]*TreeNode, map[string]*TreeNode) {
	nodes := make([]*TreeNode, len(kept))
	byID := make(map[string]*TreeNode)
	for i := range kept {
		node := treeNode(&kept[i])
		nodes[i] = node
		key := node.TraceID + "/" + node.SpanID
		if _, exists := byID[key]; !exists {
			byID[key] = node
		}
	}
	return nodes, byID
}

func treeNode(sh *keptSpan) *TreeNode {
	events := spanEvents(sh.span)
	links := spanLinks(sh.span)
	scope := sh.span.InstrumentationScope()
	resourceAttrs := resourceAttrsOf(sh.span)

	fillResourceHints(&sh.hints, resourceAttrs)
	fillEventHints(&sh.hints, events)

	return &TreeNode{
		Attrs:         sh.attrs,
		Hints:         sh.hints,
		Name:          sh.span.Name(),
		StartTime:     sh.span.StartTime(),
		EndTime:       sh.span.EndTime(),
		URLIndex:      urlIndexOf(sh.span),
		Children:      []*TreeNode{},
		Events:        events,
		Links:         links,
		SpanID:        sh.span.SpanContext().SpanID().String(),
		TraceID:       sh.span.SpanContext().TraceID().String(),
		ScopeName:     scope.Name,
		ScopeVersion:  scope.Version,
		ResourceAttrs: resourceAttrs,
	}
}

func spanEvents(s trace.ReadOnlySpan) []SpanEvent {
	var events []SpanEvent
	for _, e := range s.Events() {
		attrs := make(map[string]string)
		for _, a := range e.Attributes {
			attrs[string(a.Key)] = a.Value.Emit()
		}
		events = append(events, SpanEvent{Name: e.Name, Time: e.Time, Attrs: attrs})
	}
	return events
}

func spanLinks(s trace.ReadOnlySpan) []SpanLink {
	var links []SpanLink
	for _, l := range s.Links() {
		attrs := make(map[string]string)
		for _, a := range l.Attributes {
			attrs[string(a.Key)] = a.Value.Emit()
		}
		links = append(links, SpanLink{
			TraceID: l.SpanContext.TraceID().String(),
			SpanID:  l.SpanContext.SpanID().String(),
			Attrs:   attrs,
		})
	}
	return links
}

func resourceAttrsOf(s trace.ReadOnlySpan) map[string]string {
	attrs := make(map[string]string)
	if s.Resource() != nil {
		for _, a := range s.Resource().Attributes() {
			attrs[string(a.Key)] = a.Value.Emit()
		}
	}
	return attrs
}

func urlIndexOf(s trace.ReadOnlySpan) int {
	for _, a := range s.Attributes() {
		if string(a.Key) == "github.url_index" {
			return int(a.Value.AsInt64())
		}
	}
	return 0
}

func fillResourceHints(hints *enrichment.SpanHints, resourceAttrs map[string]string) {
	if hints.ServiceName == "" {
		if svc, ok := resourceAttrs["service.name"]; ok {
			hints.ServiceName = svc
		}
	}
	if hints.Environment == "" {
		// Stable name first, then the legacy one.
		if env, ok := resourceAttrs["deployment.environment.name"]; ok {
			hints.Environment = env
		} else if env, ok := resourceAttrs["deployment.environment"]; ok {
			hints.Environment = env
		}
	}
}

// fillEventHints folds the first exception and any feature-flag evaluations
// onto the span, so the timeline shows them and not only the inspector.
func fillEventHints(hints *enrichment.SpanHints, events []SpanEvent) {
	var flags []string
	exceptionApplied := false
	for _, ev := range events {
		if !exceptionApplied {
			if excType := enrichment.ExceptionTypeFromEvent(ev.Name, ev.Attrs); excType != "" {
				enrichment.ApplyException(hints, excType)
				exceptionApplied = true
			}
		}
		if f := enrichment.FeatureFlagFromEvent(ev.Name, ev.Attrs); f != "" {
			flags = append(flags, f)
		}
	}
	enrichment.ApplyFeatureFlags(hints, flags)
}

// linkParents hangs each span under its parent in the same trace.
// The all-zero parent ID, or a parent missing from this batch, is a root.
func linkParents(kept []keptSpan, nodes []*TreeNode, byID map[string]*TreeNode) ([]*TreeNode, map[*TreeNode]*TreeNode) {
	var roots []*TreeNode
	parentOf := make(map[*TreeNode]*TreeNode)
	for i, sh := range kept {
		parentID := sh.span.Parent().SpanID().String()
		node := nodes[i]
		if parentID == "0000000000000000" {
			roots = append(roots, node)
		} else if parent, ok := byID[node.TraceID+"/"+parentID]; ok && parent != node {
			parent.Children = append(parent.Children, node)
			parentOf[node] = parent
		} else {
			roots = append(roots, node)
		}
	}
	return roots, parentOf
}

// detachParentCycles promotes one node per parent cycle to root so FlattenTree
// does not drop it. Choice is the smallest span ID, earliest position on ties.
// That node is detached from its parent and renders once; the rest of the
// cycle hangs under it. Pinned by TestTreeSpec_ParentCycleSpansReachable.
func detachParentCycles(roots []*TreeNode, nodes []*TreeNode, parentOf map[*TreeNode]*TreeNode) []*TreeNode {
	reachable := make(map[*TreeNode]bool, len(nodes))
	for _, r := range roots {
		markReachable(r, reachable)
	}
	for len(reachable) < len(nodes) {
		var promote *TreeNode
		for _, n := range nodes {
			if !reachable[n] && (promote == nil || n.SpanID < promote.SpanID) {
				promote = n
			}
		}
		if p := parentOf[promote]; p != nil {
			for i, c := range p.Children {
				if c == promote {
					p.Children = append(p.Children[:i], p.Children[i+1:]...)
					break
				}
			}
		}
		promote.Attrs["otel-explorer.parent_cycle"] = "detached: parent chain forms a cycle"
		roots = append(roots, promote)
		markReachable(promote, reachable)
	}
	return roots
}

func markReachable(n *TreeNode, reachable map[*TreeNode]bool) {
	if reachable[n] {
		return
	}
	reachable[n] = true
	for _, c := range n.Children {
		markReachable(c, reachable)
	}
}

func sortTree(roots []*TreeNode, nodes []*TreeNode) {
	sortTreeNodes(roots)
	for _, node := range nodes {
		sortTreeNodes(node.Children)
	}
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
