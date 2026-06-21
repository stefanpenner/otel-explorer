package results

import (
	"testing"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
)

func TestGroupDottedAttrs_Empty(t *testing.T) {
	result := GroupDottedAttrs(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
	result = GroupDottedAttrs(map[string]string{})
	if result != nil {
		t.Errorf("expected nil for empty map, got %v", result)
	}
}

func TestGroupDottedAttrs_SingleKey(t *testing.T) {
	result := GroupDottedAttrs(map[string]string{"foo": "bar"})
	if len(result) != 1 {
		t.Fatalf("expected 1 node, got %d", len(result))
	}
	if result[0].Label != "foo" || result[0].Value != "bar" {
		t.Errorf("expected foo=bar, got %s=%s", result[0].Label, result[0].Value)
	}
}

func TestGroupDottedAttrs_SingleChainCollapse(t *testing.T) {
	// cicd.pipeline.run.id should collapse to one node since it's a single chain
	result := GroupDottedAttrs(map[string]string{"cicd.pipeline.run.id": "123"})
	if len(result) != 1 {
		t.Fatalf("expected 1 node, got %d", len(result))
	}
	if result[0].Label != "cicd.pipeline.run.id" {
		t.Errorf("expected collapsed label 'cicd.pipeline.run.id', got '%s'", result[0].Label)
	}
	if result[0].Value != "123" {
		t.Errorf("expected value '123', got '%s'", result[0].Value)
	}
}

func TestGroupDottedAttrs_Grouping(t *testing.T) {
	attrs := map[string]string{
		"cicd.pipeline.id":   "abc",
		"cicd.pipeline.name": "build",
		"github.actor":       "user1",
		"github.repo":        "myrepo",
	}
	result := GroupDottedAttrs(attrs)
	if len(result) != 2 {
		t.Fatalf("expected 2 top-level groups, got %d", len(result))
	}

	// Find the cicd group
	var cicdNode, githubNode *InspectorNode
	for _, n := range result {
		if n.Label == "cicd.pipeline" {
			cicdNode = n
		}
		if n.Label == "github" {
			githubNode = n
		}
	}

	if cicdNode == nil {
		t.Fatal("missing cicd.pipeline group")
	}
	if len(cicdNode.Children) != 2 {
		t.Errorf("cicd.pipeline should have 2 children, got %d", len(cicdNode.Children))
	}

	if githubNode == nil {
		t.Fatal("missing github group")
	}
	if len(githubNode.Children) != 2 {
		t.Errorf("github should have 2 children, got %d", len(githubNode.Children))
	}
}

func TestGroupDottedAttrs_MixedDepth(t *testing.T) {
	attrs := map[string]string{
		"service.name":    "my-svc",
		"service.version": "1.0",
		"simple":          "value",
	}
	result := GroupDottedAttrs(attrs)

	// Should have "service" group and "simple" leaf
	if len(result) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(result))
	}

	var serviceNode, simpleNode *InspectorNode
	for _, n := range result {
		if n.Label == "service" {
			serviceNode = n
		}
		if n.Label == "simple" {
			simpleNode = n
		}
	}
	if serviceNode == nil || len(serviceNode.Children) != 2 {
		t.Error("expected service group with 2 children")
	}
	if simpleNode == nil || simpleNode.Value != "value" {
		t.Error("expected simple leaf with value")
	}
}

func TestGroupDottedAttrs_EmptyValues(t *testing.T) {
	attrs := map[string]string{
		"keep":  "yes",
		"empty": "",
	}
	result := GroupDottedAttrs(attrs)
	if len(result) != 1 {
		t.Fatalf("expected 1 node (empty skipped), got %d", len(result))
	}
	if result[0].Label != "keep" {
		t.Errorf("expected 'keep', got '%s'", result[0].Label)
	}
}

func TestFlattenInspectorNodes_Basic(t *testing.T) {
	nodes := []*InspectorNode{
		{
			Label:    "Section",
			Expanded: true,
			Children: []*InspectorNode{
				{Label: "key1", Value: "val1"},
				{Label: "key2", Value: "val2"},
			},
		},
	}
	flat := FlattenInspectorNodes(nodes)
	if len(flat) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(flat))
	}
	if flat[0].Depth != 0 || flat[1].Depth != 1 || flat[2].Depth != 1 {
		t.Error("unexpected depth values")
	}
}

func TestFlattenInspectorNodes_Collapsed(t *testing.T) {
	nodes := []*InspectorNode{
		{
			Label:    "Section",
			Expanded: false,
			Children: []*InspectorNode{
				{Label: "key1", Value: "val1"},
			},
		},
	}
	flat := FlattenInspectorNodes(nodes)
	if len(flat) != 1 {
		t.Fatalf("expected 1 entry (collapsed), got %d", len(flat))
	}
}

func TestFlattenInspectorNodes_NestedExpand(t *testing.T) {
	nodes := []*InspectorNode{
		{
			Label:    "Root",
			Expanded: true,
			Children: []*InspectorNode{
				{
					Label:    "Group",
					Expanded: true,
					Children: []*InspectorNode{
						{Label: "leaf", Value: "v"},
					},
				},
			},
		},
	}
	flat := FlattenInspectorNodes(nodes)
	if len(flat) != 3 {
		t.Fatalf("expected 3, got %d", len(flat))
	}
	if flat[2].Depth != 2 {
		t.Errorf("expected depth 2, got %d", flat[2].Depth)
	}
}

func TestFindParentIndex(t *testing.T) {
	flat := []FlatInspectorEntry{
		{Depth: 0},
		{Depth: 1},
		{Depth: 2},
		{Depth: 1},
	}

	if idx := FindParentIndex(flat, 1); idx != 0 {
		t.Errorf("parent of index 1 should be 0, got %d", idx)
	}
	if idx := FindParentIndex(flat, 2); idx != 1 {
		t.Errorf("parent of index 2 should be 1, got %d", idx)
	}
	if idx := FindParentIndex(flat, 3); idx != 0 {
		t.Errorf("parent of index 3 should be 0, got %d", idx)
	}
	if idx := FindParentIndex(flat, 0); idx != -1 {
		t.Errorf("parent of root should be -1, got %d", idx)
	}
}

func TestBuildInspectorTree_MinimalItem(t *testing.T) {
	item := &TreeItem{
		ID:          "test-id",
		DisplayName: "Test Span",
		ItemType:    ItemTypeLeaf,
		StartTime:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		EndTime:     time.Date(2024, 1, 1, 12, 0, 5, 0, time.UTC),
	}

	sections := BuildInspectorTree(item)
	if len(sections) < 3 {
		t.Fatalf("expected at least 3 sections (Core, Timing, Status), got %d", len(sections))
	}

	// Core section
	if sections[0].Label != "Core" || !sections[0].IsSection {
		t.Error("first section should be Core")
	}
	if !sections[0].Expanded {
		t.Error("Core should be expanded by default")
	}

	// Timing section
	if sections[1].Label != "Timing" || !sections[1].IsSection {
		t.Error("second section should be Timing")
	}
}

func TestBuildInspectorTree_WithAttributes(t *testing.T) {
	item := &TreeItem{
		ID:          "test-id",
		DisplayName: "Test",
		ItemType:    ItemTypeLeaf,
		sourceNode: &analyzer.TreeNode{
			Attrs: map[string]string{
				"cicd.pipeline.id":   "abc",
				"cicd.pipeline.name": "build",
				"github.actor":       "user1",
			},
		},
		ResourceAttrs: map[string]string{
			"service.name":    "my-svc",
			"service.version": "1.0",
		},
	}

	sections := BuildInspectorTree(item)

	// Find Attributes section
	var attrSection, resSection *InspectorNode
	for _, s := range sections {
		if s.Label == "Attributes (3)" {
			attrSection = s
		}
		if s.Label == "Resource (2)" {
			resSection = s
		}
	}

	if attrSection == nil {
		t.Fatal("missing Attributes section")
	}
	if !attrSection.IsSection {
		t.Error("Attributes should be a section")
	}
	if attrSection.Expanded {
		t.Error("Attributes should be collapsed by default")
	}

	if resSection == nil {
		t.Fatal("missing Resource section")
	}
}

func TestBuildInspectorTree_WithEvents(t *testing.T) {
	item := &TreeItem{
		ID:          "test-id",
		DisplayName: "Test",
		ItemType:    ItemTypeLeaf,
		Events: []analyzer.SpanEvent{
			{
				Name: "exception",
				Time: time.Date(2024, 1, 1, 15, 4, 5, 0, time.UTC),
				Attrs: map[string]string{
					"exception.type":    "NullPointerException",
					"exception.message": "oh no",
				},
			},
		},
	}

	sections := BuildInspectorTree(item)
	var evSection *InspectorNode
	for _, s := range sections {
		if s.Label == "Events (1)" {
			evSection = s
		}
	}
	if evSection == nil {
		t.Fatal("missing Events section")
	}
	if len(evSection.Children) != 1 {
		t.Fatalf("expected 1 event child, got %d", len(evSection.Children))
	}
	evNode := evSection.Children[0]
	if len(evNode.Children) < 2 {
		t.Errorf("expected at least 2 children (type, message), got %d", len(evNode.Children))
	}
}

func TestBuildInspectorTree_WithMarker(t *testing.T) {
	item := &TreeItem{
		ID:          "test-id",
		DisplayName: "Test",
		ItemType:    ItemTypeMarker,
		Hints: enrichment.SpanHints{
			IsMarker:  true,
			User:      "alice",
			EventType: "merged",
		},
	}

	sections := BuildInspectorTree(item)
	var markerSection *InspectorNode
	for _, s := range sections {
		if s.Label == "Marker" {
			markerSection = s
		}
	}
	if markerSection == nil {
		t.Fatal("missing Marker section")
	}
	if len(markerSection.Children) != 2 {
		t.Errorf("expected 2 marker children, got %d", len(markerSection.Children))
	}
}

func TestFlattenSingleSection(t *testing.T) {
	section := &InspectorNode{
		Label:    "Attrs",
		Expanded: true,
		Children: []*InspectorNode{
			{Label: "key1", Value: "val1"},
			{
				Label:    "group",
				Expanded: true,
				Children: []*InspectorNode{
					{Label: "nested", Value: "val2"},
				},
			},
		},
	}

	flat := FlattenSingleSection(section)
	if len(flat) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(flat))
	}
	// First child starts at depth 0
	if flat[0].Depth != 0 {
		t.Errorf("expected depth 0, got %d", flat[0].Depth)
	}
	if flat[2].Depth != 1 {
		t.Errorf("expected depth 1, got %d", flat[2].Depth)
	}
}

func TestFlattenSingleSection_Nil(t *testing.T) {
	result := FlattenSingleSection(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestSearchInspectorNodes(t *testing.T) {
	flat := []FlatInspectorEntry{
		{Node: &InspectorNode{Label: "Name", Value: "build-and-test"}},
		{Node: &InspectorNode{Label: "Duration", Value: "4m 32s"}},
		{Node: &InspectorNode{Label: "cicd.pipeline", Value: "CI Build"}},
		{Node: &InspectorNode{Label: "github.actor", Value: "alice"}},
	}

	// Empty query returns nil
	matches := SearchInspectorNodes(flat, "")
	if matches != nil {
		t.Errorf("expected nil for empty query, got %v", matches)
	}

	// Search by value
	matches = SearchInspectorNodes(flat, "build")
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches for 'build', got %d: %v", len(matches), matches)
	}
	if matches[0] != 0 || matches[1] != 2 {
		t.Errorf("expected matches [0,2], got %v", matches)
	}

	// Case-insensitive
	matches = SearchInspectorNodes(flat, "ALICE")
	if len(matches) != 1 || matches[0] != 3 {
		t.Errorf("expected match at index 3, got %v", matches)
	}

	// Search by label
	matches = SearchInspectorNodes(flat, "github")
	if len(matches) != 1 || matches[0] != 3 {
		t.Errorf("expected match at index 3, got %v", matches)
	}

	// No matches
	matches = SearchInspectorNodes(flat, "zzzzz")
	if len(matches) != 0 {
		t.Errorf("expected no matches, got %v", matches)
	}
}

func TestIsURLValue(t *testing.T) {
	if !IsURLValue("https://github.com/foo") {
		t.Error("expected https URL to be detected")
	}
	if !IsURLValue("http://example.com") {
		t.Error("expected http URL to be detected")
	}
	if IsURLValue("not a url") {
		t.Error("expected non-URL to not match")
	}
}

func TestBuildInspectorTree_WithTraceIdentity(t *testing.T) {
	item := &TreeItem{
		ID:          "test-id",
		DisplayName: "Test",
		ItemType:    ItemTypeLeaf,
		TraceID:     "abc123",
		SpanID:      "def456",
	}

	sections := BuildInspectorTree(item)
	var traceSection *InspectorNode
	for _, s := range sections {
		if s.Label == "Trace Identity" {
			traceSection = s
		}
	}
	if traceSection == nil {
		t.Fatal("missing Trace Identity section")
	}
	if len(traceSection.Children) != 2 {
		t.Errorf("expected 2 children (TraceID, SpanID), got %d", len(traceSection.Children))
	}
}

// k8s.* resource attrs surface as a first-class "Kubernetes" section, and a span
// link to a controller renders as "Scheduled by ..." (the ARC handoff).
func TestInspector_KubernetesAndScheduledByLink(t *testing.T) {
	item := &TreeItem{
		ID:          "x",
		DisplayName: "build",
		ResourceAttrs: map[string]string{
			"k8s.pod.name":       "runner-abc123",
			"k8s.namespace.name": "actions",
			"service.name":       "github-actions-runner",
		},
		Links: []analyzer.SpanLink{
			{TraceID: "0af7651916cd43dd8448eb211c80319c", SpanID: "b7ad6b7169203331",
				Attrs: map[string]string{"cicd.system.component": "controller"}},
		},
	}

	var k8s, links *InspectorNode
	for _, s := range BuildInspectorTree(item) {
		switch {
		case s.Label == "Kubernetes":
			k8s = s
		case len(s.Label) >= 10 && s.Label[:10] == "Span Links":
			links = s
		}
	}

	if k8s == nil {
		t.Fatal("expected a Kubernetes section")
	}
	if k8s.Children[0].Label != "Pod" || k8s.Children[0].Value != "runner-abc123" {
		t.Errorf("expected Pod=runner-abc123, got %+v", k8s.Children[0])
	}
	if links == nil || len(links.Children) == 0 {
		t.Fatal("expected a Span Links section")
	}
	if links.Children[0].Label != "Scheduled by controller" {
		t.Errorf("expected link labeled 'Scheduled by controller', got %q", links.Children[0].Label)
	}
}
