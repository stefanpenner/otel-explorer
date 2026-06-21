package results

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// SortMode defines how timeline items are sorted
type SortMode int

const (
	SortByStartTime    SortMode = iota // default: chronological
	SortByDurationDesc                 // longest first
	SortByDurationAsc                  // shortest first
)

// String returns a human-readable label for the sort mode
func (s SortMode) String() string {
	switch s {
	case SortByDurationDesc:
		return "duration↓"
	case SortByDurationAsc:
		return "duration↑"
	default:
		return "start"
	}
}

// Next cycles to the next sort mode
func (s SortMode) Next() SortMode {
	return (s + 1) % 3
}

// ItemType represents the type of tree item
type ItemType int

const (
	ItemTypeURLGroup ItemType = iota
	ItemTypeRoot
	ItemTypeIntermediate
	ItemTypeLeaf
	ItemTypeMarker
	ItemTypeActivityGroup
	ItemTypeInfo // synthetic metadata item (no timeline bar)
)

// String returns a human-readable string for the ItemType
func (t ItemType) String() string {
	switch t {
	case ItemTypeURLGroup:
		return "URLGroup"
	case ItemTypeRoot:
		return "Root"
	case ItemTypeIntermediate:
		return "Intermediate"
	case ItemTypeLeaf:
		return "Leaf"
	case ItemTypeMarker:
		return "Marker"
	case ItemTypeActivityGroup:
		return "ActivityGroup"
	case ItemTypeInfo:
		return "Info"
	default:
		return "Unknown"
	}
}

// TreeItem represents a single item in the tree view
type TreeItem struct {
	ID           string
	Name         string
	DisplayName  string
	StartTime    time.Time
	EndTime      time.Time
	IsBottleneck bool
	Depth        int
	HasChildren  bool
	IsExpanded   bool
	ItemType     ItemType
	ParentID     string
	Children     []*TreeItem
	Hints        enrichment.SpanHints // enrichment hints from source node
	SourceHint   string               // provenance label shown when source differs from parent (e.g. "runner", "jest")
	sourceNode   *analyzer.TreeNode
	// OTel metadata surfaced for display
	Events        []analyzer.SpanEvent
	Links         []analyzer.SpanLink
	SpanID        string
	TraceID       string
	ScopeName     string
	ScopeVersion  string
	ResourceAttrs map[string]string
}

// BuildTreeItems converts TreeNodes into TreeItems for the TUI.
// Each input URL becomes a top-level URL group node containing its workflows.
func BuildTreeItems(roots []*analyzer.TreeNode, expandedState map[string]bool, inputURLs []string) []*TreeItem {
	// Ensure expandedState is non-nil
	if expandedState == nil {
		expandedState = make(map[string]bool)
	}

	// Group roots by URLIndex
	grouped := make(map[int][]*analyzer.TreeNode)
	for _, root := range roots {
		grouped[root.URLIndex] = append(grouped[root.URLIndex], root)
	}

	// If no inputURLs provided, synthesize one group for all roots
	if len(inputURLs) == 0 {
		inputURLs = []string{""}
	}

	var items []*TreeItem
	for urlIdx, inputURL := range inputURLs {
		children := grouped[urlIdx]

		// Build display name from parsed URL or file path
		displayName := inputURL
		if parsed, err := utils.ParseGitHubURL(inputURL); err == nil {
			if parsed.Type == "pr" {
				displayName = fmt.Sprintf("PR #%s (%s/%s)", parsed.Identifier, parsed.Owner, parsed.Repo)
			} else {
				id := parsed.Identifier
				if len(id) > 8 {
					id = id[:8]
				}
				displayName = fmt.Sprintf("commit %s (%s/%s)", id, parsed.Owner, parsed.Repo)
			}
		} else if !strings.HasPrefix(inputURL, "http") && inputURL != "" {
			// Not a URL — treat as a trace file name
			displayName = fmt.Sprintf("◇ %s", inputURL)
		}

		groupID := fmt.Sprintf("url-group/%d", urlIdx)

		// Calculate time bounds from non-marker children only.
		// Activity markers (review/merge events) can happen hours after CI,
		// so including them would inflate the parent's duration.
		var earliest, latest time.Time
		for _, child := range children {
			if child.Hints.IsMarker || child.Hints.GroupKey == "activity" {
				continue
			}
			if !child.StartTime.IsZero() && (earliest.IsZero() || child.StartTime.Before(earliest)) {
				earliest = child.StartTime
			}
			if !child.EndTime.IsZero() && (latest.IsZero() || child.EndTime.After(latest)) {
				latest = child.EndTime
			}
		}

		// Compute aggregate outcome
		outcome := aggregateOutcome(children)

		// Default to expanded for single-URL case
		if len(inputURLs) == 1 {
			if _, explicit := expandedState[groupID]; !explicit {
				expandedState[groupID] = true
			}
		}

		groupItem := &TreeItem{
			ID:          groupID,
			Name:        displayName,
			DisplayName: displayName,
			StartTime:   earliest,
			EndTime:     latest,
			Depth:       0,
			HasChildren: len(children) > 0,
			IsExpanded:  expandedState[groupID],
			ItemType:    ItemTypeURLGroup,
			Children:    []*TreeItem{},
			Hints: enrichment.SpanHints{
				URL:     inputURL,
				Outcome: outcome,
			},
		}

		// Add VCS changed files info from the first workflow root (shared across all workflows)
		groupItem.Children = append(groupItem.Children, buildURLGroupInfoItems(children, groupID, 1)...)

		groupItem.Children = append(groupItem.Children, partitionAndGroup(children, groupID, 1, expandedState)...)

		items = append(items, groupItem)
	}

	// Sort URL groups by start time
	sort.Slice(items, func(i, j int) bool {
		return items[i].StartTime.Before(items[j].StartTime)
	})

	return items
}

// buildURLGroupInfoItems creates info items that belong at the URL group level
// (e.g. VCS changed files which are shared across all workflows in the group).
func buildURLGroupInfoItems(roots []*analyzer.TreeNode, parentID string, depth int) []*TreeItem {
	// Find VCS change stats from the first non-marker root
	for _, root := range roots {
		if root.Hints.IsMarker || root.Hints.GroupKey == "activity" {
			continue
		}
		c := root.Attrs["vcs.changes.count"]
		if c == "" || c == "0" {
			continue
		}
		add := root.Attrs["vcs.changes.additions"]
		del := root.Attrs["vcs.changes.deletions"]
		var filesURL string
		if repoURL := root.Attrs["vcs.repository.url.full"]; repoURL != "" {
			if sha := root.Attrs["vcs.revision"]; sha != "" {
				filesURL = fmt.Sprintf("%s/commit/%s", repoURL, sha)
			}
		}
		id := makeNodeID(parentID, "_info", 0)
		return []*TreeItem{{
			ID:          id,
			Name:        fmt.Sprintf("Files: %s changed (+%s / -%s)", c, add, del),
			DisplayName: fmt.Sprintf("Files: %s changed (+%s / -%s)", c, add, del),
			Depth:       depth,
			ItemType:    ItemTypeInfo,
			ParentID:    parentID,
			Hints:       enrichment.SpanHints{Icon: "  ", Category: "diff", URL: filesURL},
		}}
	}
	return nil
}

// aggregateOutcome returns an aggregate outcome from child nodes using Hints.
func aggregateOutcome(nodes []*analyzer.TreeNode) string {
	hasFailure := false
	hasSuccess := false
	for _, n := range nodes {
		switch n.Hints.Outcome {
		case "failure":
			hasFailure = true
		case "success":
			hasSuccess = true
		}
	}
	if hasFailure {
		return "failure"
	}
	if hasSuccess {
		return "success"
	}
	return ""
}

// partitionAndGroup splits roots into workflow items and marker items,
// grouping all markers under a synthetic "Activity" node.
func partitionAndGroup(roots []*analyzer.TreeNode, parentID string, depth int, expandedState map[string]bool) []*TreeItem {
	var workflows []*analyzer.TreeNode
	var markers []*analyzer.TreeNode
	for _, root := range roots {
		if root.Hints.GroupKey == "activity" || root.Hints.IsMarker {
			markers = append(markers, root)
		} else {
			workflows = append(workflows, root)
		}
	}

	var items []*TreeItem

	// Activity group goes first (collapsed by default)
	if len(markers) > 0 {
		groupID := makeNodeID(parentID, "Activity", 0)

		var earliest, latest time.Time
		var children []*TreeItem
		for i, m := range markers {
			child := convertNode(m, groupID, i, depth+1, expandedState, "")
			children = append(children, child)
			if !m.StartTime.IsZero() && (earliest.IsZero() || m.StartTime.Before(earliest)) {
				earliest = m.StartTime
			}
			if !m.EndTime.IsZero() && (latest.IsZero() || m.EndTime.After(latest)) {
				latest = m.EndTime
			}
		}

		activityGroup := &TreeItem{
			ID:          groupID,
			Name:        "Activity",
			DisplayName: "Activity",
			StartTime:   earliest,
			EndTime:     latest,
			Depth:       depth,
			HasChildren: true,
			IsExpanded:  expandedState[groupID],
			ItemType:    ItemTypeActivityGroup,
			ParentID:    parentID,
			Children:    children,
		}
		items = append(items, activityGroup)
	}

	for i, wf := range workflows {
		items = append(items, convertNode(wf, parentID, i, depth, expandedState, ""))
	}

	return items
}

func convertNode(node *analyzer.TreeNode, parentID string, index, depth int, expandedState map[string]bool, parentSource string) *TreeItem {
	id := makeNodeID(parentID, node.Name, index)

	itemType := itemTypeFromNode(node)

	displayName := node.Name
	// For workflow roots, append the definition file path to the name
	if itemType == ItemTypeRoot {
		if p := node.Attrs["cicd.pipeline.definition"]; p != "" {
			displayName = fmt.Sprintf("%s · %s", node.Name, p)
		}
	}

	// Provenance hint: when a sub-tree's source differs from its parent's, tag the
	// boundary node (e.g. "build ← runner", "unit tests ← jest", "Queued ← github-api").
	// Kept in a separate field (not baked into the name) so name-based lookups,
	// focus, and search stay clean; the renderer appends it dimly.
	source := node.SourceLabel()
	sourceHint := ""
	if source != "" && source != parentSource {
		sourceHint = source
	}

	item := &TreeItem{
		ID:            id,
		Name:          displayName,
		DisplayName:   displayName,
		SourceHint:    sourceHint,
		StartTime:     node.StartTime,
		EndTime:       node.EndTime,
		Depth:         depth,
		HasChildren:   len(node.Children) > 0,
		IsExpanded:    expandedState[id],
		ItemType:      itemType,
		ParentID:      parentID,
		Children:      []*TreeItem{},
		Hints:         node.Hints,
		sourceNode:    node,
		Events:        node.Events,
		Links:         node.Links,
		SpanID:        node.SpanID,
		TraceID:       node.TraceID,
		ScopeName:     node.ScopeName,
		ScopeVersion:  node.ScopeVersion,
		ResourceAttrs: node.ResourceAttrs,
	}

	// Partition children into regular and artifact groups.
	// Skip artifact sub-grouping if this node is already an artifact span —
	// all artifact descendants have GroupKey=="artifact" from the ingest tagging,
	// and re-grouping them would create spurious nested "Trace Artifacts" nodes.
	var regularChildren []*analyzer.TreeNode
	var artifactChildren []*analyzer.TreeNode
	for _, child := range node.Children {
		if child.Hints.GroupKey == "artifact" && node.Hints.GroupKey != "artifact" {
			artifactChildren = append(artifactChildren, child)
		} else {
			regularChildren = append(regularChildren, child)
		}
	}

	// Convert regular children
	for i, child := range regularChildren {
		childItem := convertNode(child, id, i, depth+1, expandedState, source)
		item.Children = append(item.Children, childItem)
	}

	// Build artifacts folder: contains all artifacts, with trace spans nested
	// under their source artifact.
	hasArtifactMetadata := node.Attrs["cicd.pipeline.artifacts.count"] != "" && node.Attrs["cicd.pipeline.artifacts.count"] != "0"
	if hasArtifactMetadata || len(artifactChildren) > 0 {
		folder := buildArtifactFolder(node, id, depth+1, artifactChildren, expandedState)
		item.Children = append(item.Children, folder)
		item.HasChildren = true
	}

	return item
}

// FlattenVisibleItems returns a flat list of visible items based on expanded state
func FlattenVisibleItems(items []*TreeItem, expandedState map[string]bool, sortMode SortMode) []TreeItem {
	var result []TreeItem

	var flatten func(items []*TreeItem)
	flatten = func(items []*TreeItem) {
		sorted := items
		if sortMode != SortByStartTime {
			// Sort a copy to avoid mutating the tree
			sorted = make([]*TreeItem, len(items))
			copy(sorted, items)
			sort.Slice(sorted, func(i, j int) bool {
				di := sorted[i].EndTime.Sub(sorted[i].StartTime)
				dj := sorted[j].EndTime.Sub(sorted[j].StartTime)
				if sortMode == SortByDurationDesc {
					return di > dj
				}
				return di < dj
			})
		}
		for _, item := range sorted {
			result = append(result, *item)
			if item.HasChildren && expandedState[item.ID] {
				flatten(item.Children)
			}
		}
	}

	flatten(items)
	return result
}

// FilterVisibleItems filters already-flattened visible items to only include
// items whose ID is in matchIDs or ancestorIDs.
func FilterVisibleItems(items []TreeItem, matchIDs, ancestorIDs map[string]bool) []TreeItem {
	var result []TreeItem
	for _, item := range items {
		if matchIDs[item.ID] || ancestorIDs[item.ID] {
			result = append(result, item)
		}
	}
	return result
}

// itemTypeFromNode derives ItemType from enrichment hints.
func itemTypeFromNode(node *analyzer.TreeNode) ItemType {
	hints := node.Hints
	if hints.IsMarker {
		return ItemTypeMarker
	}
	switch hints.Category {
	case "workflow", "pipeline":
		return ItemTypeRoot
	case "job", "task":
		return ItemTypeIntermediate
	case "step":
		return ItemTypeLeaf
	default:
		if hints.IsRoot {
			return ItemTypeRoot
		}
		if hints.IsLeaf || len(node.Children) == 0 {
			return ItemTypeLeaf
		}
		return ItemTypeIntermediate
	}
}

// SpanIndex provides O(1) lookups for tree items by various keys.
type SpanIndex struct {
	ByID       map[string]*TreeItem
	ByParentID map[string][]*TreeItem
}

// BuildSpanIndex creates a SpanIndex from a tree of items.
func BuildSpanIndex(items []*TreeItem) *SpanIndex {
	idx := &SpanIndex{
		ByID:       make(map[string]*TreeItem),
		ByParentID: make(map[string][]*TreeItem),
	}
	var walk func(items []*TreeItem)
	walk = func(items []*TreeItem) {
		for _, item := range items {
			idx.ByID[item.ID] = item
			if item.ParentID != "" {
				idx.ByParentID[item.ParentID] = append(idx.ByParentID[item.ParentID], item)
			}
			walk(item.Children)
		}
	}
	walk(items)
	return idx
}

// buildArtifactFolder creates a collapsible folder of artifacts, with trace spans
// nested under their source artifact.
func buildArtifactFolder(node *analyzer.TreeNode, parentID string, depth int, artifactChildren []*analyzer.TreeNode, expandedState map[string]bool) *TreeItem {
	folderID := makeNodeID(parentID, "Artifacts", 0)

	// Parse per-artifact metadata from indexed attributes
	type artifactMeta struct {
		Name string
		Size string
		URL  string
	}
	var allArtifacts []artifactMeta
	for i := 0; ; i++ {
		name := node.Attrs[fmt.Sprintf("cicd.pipeline.artifact.%d.name", i)]
		if name == "" {
			break
		}
		size := node.Attrs[fmt.Sprintf("cicd.pipeline.artifact.%d.size", i)]
		url := node.Attrs[fmt.Sprintf("cicd.pipeline.artifact.%d.url", i)]
		allArtifacts = append(allArtifacts, artifactMeta{Name: name, Size: size, URL: url})
	}

	// Group trace spans by their source artifact name
	traceByArtifact := make(map[string][]*analyzer.TreeNode)
	for _, ac := range artifactChildren {
		name := ac.Attrs["github.artifact_name"]
		if name == "" {
			name = "_unknown"
		}
		traceByArtifact[name] = append(traceByArtifact[name], ac)
	}

	// Build per-artifact children
	var children []*TreeItem
	var earliest, latest time.Time
	seenNames := make(map[string]bool)
	childIdx := 0

	for _, meta := range allArtifacts {
		seenNames[meta.Name] = true
		artifactID := makeNodeID(folderID, meta.Name, childIdx)
		childIdx++

		label := meta.Name
		if meta.Size != "" {
			label += " (" + meta.Size + ")"
		}

		traceSpans := traceByArtifact[meta.Name]
		artifactItem := &TreeItem{
			ID:          artifactID,
			Name:        label,
			DisplayName: label,
			Depth:       depth + 1,
			HasChildren: len(traceSpans) > 0,
			IsExpanded:  expandedState[artifactID],
			ItemType:    ItemTypeLeaf,
			ParentID:    folderID,
			Children:    []*TreeItem{},
			Hints: enrichment.SpanHints{
				Icon:     "  ",
				Color:    "blue",
				Category: "artifact",
				URL:      meta.URL,
			},
		}

		// If this artifact has trace data, nest the trace spans under it
		if len(traceSpans) > 0 {
			artifactItem.ItemType = ItemTypeIntermediate
			artifactItem.Hints.Icon = "◈ "
			for i, ac := range traceSpans {
				child := convertNode(ac, artifactID, i, depth+2, expandedState, "")
				artifactItem.Children = append(artifactItem.Children, child)
				if !ac.StartTime.IsZero() && (earliest.IsZero() || ac.StartTime.Before(earliest)) {
					earliest = ac.StartTime
				}
				if !ac.EndTime.IsZero() && (latest.IsZero() || ac.EndTime.After(latest)) {
					latest = ac.EndTime
				}
			}
		}
		children = append(children, artifactItem)
	}

	// Any trace spans from artifacts not in the metadata (shouldn't happen, but be safe)
	for name, spans := range traceByArtifact {
		if seenNames[name] {
			continue
		}
		artifactID := makeNodeID(folderID, name, childIdx)
		childIdx++
		artifactItem := &TreeItem{
			ID:          artifactID,
			Name:        name,
			DisplayName: name,
			Depth:       depth + 1,
			HasChildren: true,
			IsExpanded:  expandedState[artifactID],
			ItemType:    ItemTypeIntermediate,
			ParentID:    folderID,
			Children:    []*TreeItem{},
			Hints: enrichment.SpanHints{
				Icon:     "◈ ",
				Color:    "blue",
				Category: "artifact",
			},
		}
		for i, ac := range spans {
			child := convertNode(ac, artifactID, i, depth+2, expandedState, "")
			artifactItem.Children = append(artifactItem.Children, child)
			if !ac.StartTime.IsZero() && (earliest.IsZero() || ac.StartTime.Before(earliest)) {
				earliest = ac.StartTime
			}
			if !ac.EndTime.IsZero() && (latest.IsZero() || ac.EndTime.After(latest)) {
				latest = ac.EndTime
			}
		}
		children = append(children, artifactItem)
	}

	totalSize := node.Attrs["cicd.pipeline.artifacts.size"]
	count := len(children)
	var folderLabel string
	switch {
	case totalSize != "" && count > 0:
		folderLabel = fmt.Sprintf("Artifacts: %d (%s)", count, totalSize)
	case count > 0:
		folderLabel = fmt.Sprintf("Artifacts: %d", count)
	default:
		folderLabel = "Artifacts"
	}

	// Link to the workflow run page where artifacts are listed
	var folderURL string
	if runURL := node.Attrs["cicd.pipeline.run.url.full"]; runURL != "" {
		folderURL = runURL
	}

	return &TreeItem{
		ID:          folderID,
		Name:        folderLabel,
		DisplayName: folderLabel,
		StartTime:   earliest,
		EndTime:     latest,
		Depth:       depth,
		HasChildren: len(children) > 0,
		IsExpanded:  expandedState[folderID],
		ItemType:    ItemTypeActivityGroup,
		ParentID:    parentID,
		Children:    children,
		Hints: enrichment.SpanHints{
			Category: "artifact",
			Icon:     "📁",
			Color:    "blue",
			URL:      folderURL,
		},
	}
}

func makeNodeID(parentID, name string, index int) string {
	if parentID == "" {
		return fmt.Sprintf("%s/%d", name, index)
	}
	return fmt.Sprintf("%s/%s/%d", parentID, name, index)
}

// CountStats returns workflow and job counts from the tree
func CountStats(roots []*analyzer.TreeNode) (workflows, jobs int) {
	for _, root := range roots {
		if isRootNode(root) {
			workflows++
		}
		countJobs(root, &jobs)
	}
	return
}

// isRootNode checks if node is a root-level span.
func isRootNode(node *analyzer.TreeNode) bool {
	return node.Hints.IsRoot
}

// isJobNode checks if node is a job-level (mid-tier) span.
func isJobNode(node *analyzer.TreeNode) bool {
	return !node.Hints.IsRoot && !node.Hints.IsMarker && !node.Hints.IsLeaf
}

func countJobs(node *analyzer.TreeNode, count *int) {
	if isJobNode(node) {
		*count++
	}
	for _, child := range node.Children {
		countJobs(child, count)
	}
}
