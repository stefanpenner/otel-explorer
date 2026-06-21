package results

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/x/ansi"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// InspectorNode represents a node in the collapsible inspector tree.
type InspectorNode struct {
	Label    string
	Value    string // empty for group/section nodes
	Children []*InspectorNode
	Expanded bool
	// Rendering hints
	IsSection bool      // top-level section header
	IsURL     bool      // value is a clickable hyperlink
	ChildItem *TreeItem // non-nil if this node is a navigable child span
}

// HasChildren returns true if the node has child nodes.
func (n *InspectorNode) HasChildren() bool {
	return len(n.Children) > 0
}

// FlatInspectorEntry is a flattened inspector node with computed depth.
type FlatInspectorEntry struct {
	Node  *InspectorNode
	Depth int
}

// FlattenInspectorNodes returns the visible (expanded) nodes in display order.
func FlattenInspectorNodes(nodes []*InspectorNode) []FlatInspectorEntry {
	var result []FlatInspectorEntry
	var walk func(nodes []*InspectorNode, depth int)
	walk = func(nodes []*InspectorNode, depth int) {
		for _, n := range nodes {
			result = append(result, FlatInspectorEntry{Node: n, Depth: depth})
			if n.Expanded && len(n.Children) > 0 {
				walk(n.Children, depth+1)
			}
		}
	}
	walk(nodes, 0)
	return result
}

// BuildInspectorTree builds the inspector tree from a TreeItem.
func BuildInspectorTree(item *TreeItem) []*InspectorNode {
	var sections []*InspectorNode

	// Core
	{
		s := &InspectorNode{Label: "Core", IsSection: true, Expanded: true}
		s.Children = append(s.Children, &InspectorNode{Label: "Name", Value: item.DisplayName})
		s.Children = append(s.Children, &InspectorNode{Label: "ID", Value: item.ID})
		if item.ParentID != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Parent ID", Value: item.ParentID})
		}
		s.Children = append(s.Children, &InspectorNode{Label: "Type", Value: item.ItemType.String()})
		sections = append(sections, s)
	}

	// Timing
	{
		s := &InspectorNode{Label: "Timing", IsSection: true, Expanded: true}
		if !item.StartTime.IsZero() {
			s.Children = append(s.Children, &InspectorNode{Label: "Start", Value: item.StartTime.Format("2006-01-02 15:04:05")})
		}
		if !item.EndTime.IsZero() {
			s.Children = append(s.Children, &InspectorNode{Label: "End", Value: item.EndTime.Format("2006-01-02 15:04:05")})
		}
		if !item.StartTime.IsZero() && !item.EndTime.IsZero() {
			dur := item.EndTime.Sub(item.StartTime).Seconds()
			if dur < 0 {
				dur = 0
			}
			s.Children = append(s.Children, &InspectorNode{Label: "Duration", Value: utils.HumanizeTime(dur)})
		}
		sections = append(sections, s)
	}

	// Status
	{
		s := &InspectorNode{Label: "Status", IsSection: true, Expanded: true}
		if item.Hints.Outcome != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Outcome", Value: item.Hints.Outcome})
		}
		reqVal := "No"
		if item.Hints.IsRequired {
			reqVal = "Yes"
		}
		s.Children = append(s.Children, &InspectorNode{Label: "Required", Value: reqVal})
		bnVal := "No"
		if item.IsBottleneck {
			bnVal = "Yes"
		}
		s.Children = append(s.Children, &InspectorNode{Label: "Bottleneck", Value: bnVal})
		sections = append(sections, s)
	}

	// URL Link
	if item.Hints.URL != "" {
		s := &InspectorNode{Label: "Links", IsSection: true, Expanded: true}
		s.Children = append(s.Children, &InspectorNode{Label: "URL", Value: item.Hints.URL, IsURL: true})
		sections = append(sections, s)
	}

	// Marker
	if item.Hints.IsMarker {
		s := &InspectorNode{Label: "Marker", IsSection: true, Expanded: true}
		if item.Hints.User != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "User", Value: item.Hints.User})
		}
		if item.Hints.EventType != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Event Type", Value: item.Hints.EventType})
		}
		sections = append(sections, s)
	}

	// Trace Identity
	if item.TraceID != "" || item.SpanID != "" {
		s := &InspectorNode{Label: "Trace Identity", IsSection: true, Expanded: true}
		// Provenance: which emitter produced this span (runner / github-api / a tool).
		if item.sourceNode != nil {
			if src := item.sourceNode.SourceLabel(); src != "" {
				s.Children = append(s.Children, &InspectorNode{Label: "Source", Value: src})
			}
		}
		if item.TraceID != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Trace ID", Value: item.TraceID})
		}
		if item.SpanID != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Span ID", Value: item.SpanID})
		}
		sections = append(sections, s)
	}

	// Context
	if item.Hints.ServiceName != "" || item.Hints.Environment != "" || item.Hints.Detail != "" {
		s := &InspectorNode{Label: "Context", IsSection: true, Expanded: true}
		if item.Hints.ServiceName != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Service", Value: item.Hints.ServiceName})
		}
		if item.Hints.Environment != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Environment", Value: item.Hints.Environment})
		}
		if item.Hints.Detail != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Detail", Value: item.Hints.Detail})
		}
		if item.Hints.VCSBranch != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Branch", Value: item.Hints.VCSBranch})
		}
		if item.Hints.VCSRevision != "" {
			rev := item.Hints.VCSRevision
			if len(rev) > 12 {
				rev = rev[:12]
			}
			s.Children = append(s.Children, &InspectorNode{Label: "Revision", Value: rev})
		}
		if item.Hints.RunID != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Run ID", Value: item.Hints.RunID})
		}
		sections = append(sections, s)
	}

	// Kubernetes — promote standard k8s.* resource attributes to a first-class
	// section (they otherwise live buried in the generic Resource dump). This is
	// where a job ran when the runner is deployed on K8s (e.g. via ARC).
	if item.ResourceAttrs != nil {
		k8s := &InspectorNode{Label: "Kubernetes", IsSection: true, Expanded: true}
		for _, f := range []struct{ label, key string }{
			{"Pod", "k8s.pod.name"},
			{"Namespace", "k8s.namespace.name"},
			{"Node", "k8s.node.name"},
			{"Deployment", "k8s.deployment.name"},
		} {
			if v := item.ResourceAttrs[f.key]; v != "" {
				k8s.Children = append(k8s.Children, &InspectorNode{Label: f.label, Value: v})
			}
		}
		if len(k8s.Children) > 0 {
			sections = append(sections, k8s)
		}
	}

	// Instrumentation
	if item.ScopeName != "" {
		s := &InspectorNode{Label: "Instrumentation", IsSection: true, Expanded: false}
		s.Children = append(s.Children, &InspectorNode{Label: "Library", Value: item.ScopeName})
		if item.ScopeVersion != "" {
			s.Children = append(s.Children, &InspectorNode{Label: "Version", Value: item.ScopeVersion})
		}
		sections = append(sections, s)
	}

	// Events
	if len(item.Events) > 0 {
		s := &InspectorNode{
			Label:     fmt.Sprintf("Events (%d)", len(item.Events)),
			IsSection: true,
			Expanded:  false,
		}
		for _, ev := range item.Events {
			timeStr := ""
			if !ev.Time.IsZero() {
				timeStr = " @ " + ev.Time.Format("15:04:05")
			}
			evNode := &InspectorNode{
				Label:    ev.Name + timeStr,
				Expanded: false,
			}
			// Exception details
			if exType := ev.Attrs["exception.type"]; exType != "" {
				evNode.Children = append(evNode.Children, &InspectorNode{Label: "Type", Value: exType})
			}
			if exMsg := ev.Attrs["exception.message"]; exMsg != "" {
				msg := ansi.Truncate(exMsg, 120, "...")
				evNode.Children = append(evNode.Children, &InspectorNode{Label: "Message", Value: msg})
			}
			if stack := ev.Attrs["exception.stacktrace"]; stack != "" {
				stackNode := &InspectorNode{
					Label:    fmt.Sprintf("Stacktrace (%d lines)", strings.Count(stack, "\n")+1),
					Expanded: false,
				}
				for _, sl := range strings.Split(stack, "\n") {
					if sl != "" {
						stackNode.Children = append(stackNode.Children, &InspectorNode{Value: sl})
					}
				}
				evNode.Children = append(evNode.Children, stackNode)
			}
			// Other event attrs
			for k, v := range ev.Attrs {
				if k != "exception.type" && k != "exception.message" && k != "exception.stacktrace" && v != "" {
					evNode.Children = append(evNode.Children, &InspectorNode{Label: k, Value: v})
				}
			}
			s.Children = append(s.Children, evNode)
		}
		sections = append(sections, s)
	}

	// Span Links
	if len(item.Links) > 0 {
		s := &InspectorNode{
			Label:     fmt.Sprintf("Span Links (%d)", len(item.Links)),
			IsSection: true,
			Expanded:  false,
		}
		for i, link := range item.Links {
			// A link to a controller's span (cicd.system.component=controller) is an
			// upstream scheduler — e.g. ARC scheduling this job. Label it as such.
			label := fmt.Sprintf("Link %d", i+1)
			if comp := link.Attrs["cicd.system.component"]; comp != "" {
				label = "Scheduled by " + comp
			}
			linkNode := &InspectorNode{
				Label:    label,
				Expanded: false,
			}
			linkNode.Children = append(linkNode.Children, &InspectorNode{Label: "Trace ID", Value: link.TraceID})
			linkNode.Children = append(linkNode.Children, &InspectorNode{Label: "Span ID", Value: link.SpanID})
			for k, v := range link.Attrs {
				if v != "" {
					linkNode.Children = append(linkNode.Children, &InspectorNode{Label: k, Value: v})
				}
			}
			s.Children = append(s.Children, linkNode)
		}
		sections = append(sections, s)
	}

	// Span Attributes (grouped by dotted prefix)
	if item.sourceNode != nil && len(item.sourceNode.Attrs) > 0 {
		s := &InspectorNode{
			Label:     fmt.Sprintf("Attributes (%d)", len(item.sourceNode.Attrs)),
			IsSection: true,
			Expanded:  false,
		}
		s.Children = GroupDottedAttrs(item.sourceNode.Attrs)
		sections = append(sections, s)
	}

	// Resource Attributes (grouped by dotted prefix)
	if len(item.ResourceAttrs) > 0 {
		s := &InspectorNode{
			Label:     fmt.Sprintf("Resource (%d)", len(item.ResourceAttrs)),
			IsSection: true,
			Expanded:  false,
		}
		s.Children = GroupDottedAttrs(item.ResourceAttrs)
		sections = append(sections, s)
	}

	// Tree Info with navigable children
	{
		label := "Tree"
		if item.HasChildren {
			label = fmt.Sprintf("Children (%d)", len(item.Children))
		}
		s := &InspectorNode{Label: label, IsSection: true, Expanded: false}
		s.Children = append(s.Children, &InspectorNode{Label: "Depth", Value: fmt.Sprintf("%d", item.Depth)})
		if item.HasChildren {
			for _, child := range item.Children {
				childNode := &InspectorNode{
					Label:     child.DisplayName,
					Value:     "→",
					ChildItem: child,
				}
				if child.Hints.Outcome != "" {
					childNode.Value = child.Hints.Outcome + " →"
				}
				s.Children = append(s.Children, childNode)
			}
		}
		sections = append(sections, s)
	}

	return sections
}

// trieNode is used internally to build a prefix trie for dotted attribute grouping.
type trieNode struct {
	segment  string
	value    string // non-empty only for leaf nodes
	hasValue bool
	children []*trieNode
}

// GroupDottedAttrs groups a flat map of dotted keys into a tree of InspectorNodes.
// Keys are split by "." and organized into a trie, with single-child chains collapsed
// (e.g., cicd.pipeline.run.url becomes "cicd.pipeline.run" > "url: value" if run has
// only one child, or stays nested if there are siblings).
func GroupDottedAttrs(attrs map[string]string) []*InspectorNode {
	if len(attrs) == 0 {
		return nil
	}

	// Build a trie
	root := &trieNode{}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := attrs[k]
		if v == "" {
			continue
		}
		segments := strings.Split(k, ".")
		cur := root
		for _, seg := range segments {
			found := false
			for _, ch := range cur.children {
				if ch.segment == seg {
					cur = ch
					found = true
					break
				}
			}
			if !found {
				child := &trieNode{segment: seg}
				cur.children = append(cur.children, child)
				cur = child
			}
		}
		cur.value = v
		cur.hasValue = true
	}

	// Convert trie to InspectorNodes, collapsing single-child chains
	return convertTrieChildren(root)
}

// convertTrieChildren converts all children of a trie node to InspectorNodes.
func convertTrieChildren(parent *trieNode) []*InspectorNode {
	var result []*InspectorNode
	for _, ch := range parent.children {
		result = append(result, convertTrieNode(ch, ch.segment))
	}
	return result
}

// convertTrieNode converts a single trie node to an InspectorNode,
// collapsing single-child chains into dotted labels.
func convertTrieNode(t *trieNode, prefix string) *InspectorNode {
	// Collapse: if this node has no value and exactly one child, merge with child
	if !t.hasValue && len(t.children) == 1 {
		child := t.children[0]
		return convertTrieNode(child, prefix+"."+child.segment)
	}

	node := &InspectorNode{Label: prefix}

	if t.hasValue && len(t.children) == 0 {
		// Pure leaf
		node.Value = t.value
		return node
	}

	if t.hasValue {
		// Node has both a value and children (rare but possible)
		node.Value = t.value
	}

	// Group node: default expanded if few children, collapsed if many
	node.Expanded = len(t.children) <= 5
	node.Children = convertTrieChildren(t)

	return node
}

// FlattenSingleSection flattens only the children of a single section node.
// The section header itself is not included; its children start at depth 0.
func FlattenSingleSection(section *InspectorNode) []FlatInspectorEntry {
	if section == nil {
		return nil
	}
	var result []FlatInspectorEntry
	var walk func(nodes []*InspectorNode, depth int)
	walk = func(nodes []*InspectorNode, depth int) {
		for _, n := range nodes {
			result = append(result, FlatInspectorEntry{Node: n, Depth: depth})
			if n.Expanded && len(n.Children) > 0 {
				walk(n.Children, depth+1)
			}
		}
	}
	walk(section.Children, 0)
	return result
}

// SearchInspectorNodes returns indices into flat where Label or Value matches
// the query (case-insensitive substring match).
func SearchInspectorNodes(flat []FlatInspectorEntry, query string) []int {
	if query == "" {
		return nil
	}
	q := strings.ToLower(query)
	var matches []int
	for i, entry := range flat {
		if strings.Contains(strings.ToLower(entry.Node.Label), q) ||
			strings.Contains(strings.ToLower(entry.Node.Value), q) {
			matches = append(matches, i)
		}
	}
	return matches
}

// SectionLabel returns a display label for a section node (used in sidebar).
func SectionLabel(section *InspectorNode) string {
	return section.Label
}

// IsURLValue returns true if the string looks like a URL.
func IsURLValue(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// FindParentIndex returns the index of the parent node for the entry at the given index,
// or -1 if it's a root-level node.
func FindParentIndex(flat []FlatInspectorEntry, index int) int {
	if index <= 0 || index >= len(flat) {
		return -1
	}
	targetDepth := flat[index].Depth - 1
	for i := index - 1; i >= 0; i-- {
		if flat[i].Depth == targetDepth {
			return i
		}
		if flat[i].Depth < targetDepth {
			return -1
		}
	}
	return -1
}

// buildInspectorEvent is a helper for creating event nodes (used by both
// BuildInspectorTree and tests).
func buildEventNodes(events []analyzer.SpanEvent) []*InspectorNode {
	var nodes []*InspectorNode
	for _, ev := range events {
		timeStr := ""
		if !ev.Time.IsZero() {
			timeStr = " @ " + ev.Time.Format("15:04:05")
		}
		evNode := &InspectorNode{
			Label:    ev.Name + timeStr,
			Expanded: false,
		}
		if exType := ev.Attrs["exception.type"]; exType != "" {
			evNode.Children = append(evNode.Children, &InspectorNode{Label: "Type", Value: exType})
		}
		if exMsg := ev.Attrs["exception.message"]; exMsg != "" {
			evNode.Children = append(evNode.Children, &InspectorNode{Label: "Message", Value: exMsg})
		}
		for k, v := range ev.Attrs {
			if k != "exception.type" && k != "exception.message" && k != "exception.stacktrace" && v != "" {
				evNode.Children = append(evNode.Children, &InspectorNode{Label: k, Value: v})
			}
		}
		nodes = append(nodes, evNode)
	}
	return nodes
}
