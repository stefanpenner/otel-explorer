package results

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stefanpenner/otel-explorer/pkg/enrichment"
	"github.com/stefanpenner/otel-explorer/pkg/utils"
	"go.opentelemetry.io/otel/sdk/trace"
)

// ReloadResultMsg is sent when reload completes
type ReloadResultMsg struct {
	spans       []trace.ReadOnlySpan
	globalStart time.Time
	globalEnd   time.Time
	err         error
}

// clearCopyFeedbackMsg clears the "Copied!" message after a delay.
type clearCopyFeedbackMsg struct{}

// inspectorBreadcrumbEntry saves UI state when navigating into a child.
type inspectorBreadcrumbEntry struct {
	item        *TreeItem
	sidebarIdx  int
	cursor      int
	scroll      int
	focusLeft   bool
}

// LoadingProgressMsg updates loading progress display
type LoadingProgressMsg struct {
	Phase  string
	Detail string
	URL    string
}

// LoadingReporter reports loading progress
type LoadingReporter interface {
	SetPhase(phase string)
	SetDetail(detail string)
	SetURL(url string)
}

// Model represents the TUI state
type Model struct {
	enricher      enrichment.Enricher
	roots         []*analyzer.TreeNode
	treeItems     []*TreeItem
	visibleItems  []TreeItem
	expandedState map[string]bool
	hiddenState   map[string]bool // items hidden from chart
	cursor        int
	selectionStart int // start of multi-selection range (-1 if no range)
	width         int
	height        int
	globalStart   time.Time
	globalEnd     time.Time
	chartStart    time.Time // calculated from non-hidden items
	chartEnd      time.Time // calculated from non-hidden items
	keys          KeyMap
	// Statistics (full dataset)
	summary     analyzer.Summary
	wallTimeMs  int64
	computeMs   int64
	stepCount   int
	// Displayed statistics (only visible items)
	displayedSummary   analyzer.Summary
	displayedWallTimeMs int64
	displayedComputeMs  int64
	displayedStepCount  int
	// Input URLs from CLI
	inputURLs []string
	// Modal state
	showDetailModal  bool
	showHelpModal    bool
	modalItem        *TreeItem
	modalScroll      int
	inspectorNodes   []*InspectorNode
	inspectorFlat    []FlatInspectorEntry
	inspectorCursor  int
	// Two-pane inspector state
	inspectorSidebarIdx  int  // 0-indexed into inspectorNodes
	inspectorFocusLeft   bool // true = sidebar focused
	// Inspector search
	inspectorSearching   bool
	inspectorSearchQuery string
	inspectorSearchMatches []int // indices into inspectorFlat
	inspectorSearchIdx   int    // current match index
	// Inspector breadcrumb navigation (traverse into children)
	inspectorBreadcrumb      []inspectorBreadcrumbEntry // stack of parent states
	// Copy feedback
	inspectorCopyMsg     string // transient "Copied!" message
	// Reload state
	isLoading     bool
	reloadFunc    func(reporter LoadingReporter) ([]trace.ReadOnlySpan, time.Time, time.Time, error)
	spinner       spinner.Model
	loadingPhase  string
	loadingDetail string
	loadingURL    string
	progressCh    chan LoadingProgressMsg
	resultCh      chan ReloadResultMsg
	// Focus state
	isFocused           bool
	focusedIDs          map[string]bool // IDs of items in focus (for dimming non-focused)
	preFocusHiddenState map[string]bool
	// Spans for export
	spans []trace.ReadOnlySpan
	// Perfetto open function
	openPerfettoFunc func([]trace.ReadOnlySpan, bool)
	// Mouse mode state
	mouseEnabled bool
	// Vim-style two-key sequence state
	pendingG  bool // waiting for second 'g' in gg
	pendingGG bool // waiting for second 'G' in GG
	// Search/filter state
	isSearching    bool
	searchQuery    string
	searchMatchIDs map[string]bool // IDs of items matching the query (not ancestors)
	searchAncIDs   map[string]bool // IDs of ancestor items (for context)
	// Logical end marker
	logicalEndID   string    // ID of marked item ("" = no marker)
	logicalEndTime time.Time // EndTime of marked item
	// Sort mode
	sortMode SortMode
	// Resizable tree width
	treeWidth int
	// Reload error
	reloadError string
	// Tree connector characters for each visible item (precomputed)
	treeConnectors [][]rune
	// Total visible items before search filter (for N/M display)
	preFilterCount int
	// Span index for O(1) lookups
	spanIndex *SpanIndex
	// Original time bounds per item (set once at build time, used to restore after toggle)
	origTimes map[string][2]time.Time
	// VCS changed files (extracted from root span attributes)
	changedFilesCount int
	changedFilesAdd   int
	changedFilesDel   int
	// Uploaded artifacts (from root span attributes)
	artifactsCount int
	artifactsSize  string
	artifactNames  string
	// Workflow definition files
	workflowFiles []string
	// Log fetch state
	logFetchFunc      LogFetchFunc
	logFetchingJobID  int64            // non-zero while a log fetch is in progress
	logFetchedJobIDs  map[int64]bool   // job IDs that have already been fetched
	logFetchInline    *logFetchState   // inline progress for the item being fetched
}

// ReloadFunc is the function signature for reloading data
type ReloadFunc func(reporter LoadingReporter) ([]trace.ReadOnlySpan, time.Time, time.Time, error)

// LogFetchFunc fetches and parses step logs for a job, returning new sub-step spans.
// owner, repo, jobID identify the job whose logs to fetch.
// existingSpans provides context (the step spans to parent under).
type LogFetchFunc func(owner, repo string, jobID int64, existingSpans []trace.ReadOnlySpan) ([]trace.ReadOnlySpan, error)

// LogFetchResultMsg is sent when log fetch completes for a step.
type LogFetchResultMsg struct {
	newSpans []trace.ReadOnlySpan
	err      error
}

// OpenPerfettoFunc is the function signature for opening Perfetto.
// It receives the currently visible (non-hidden) spans and whether
// activity markers (reviews, merges, etc.) are hidden.
type OpenPerfettoFunc func(spans []trace.ReadOnlySpan, activityHidden bool)

// NewModel creates a new TUI model from OTel spans
func NewModel(spans []trace.ReadOnlySpan, globalStart, globalEnd time.Time, inputURLs []string, reloadFunc ReloadFunc, openPerfettoFunc OpenPerfettoFunc, enricher enrichment.Enricher, opts ...ModelOption) Model {
	s := spinner.New()
	s.Spinner = spinner.Dot

	m := Model{
		enricher:         enricher,
		expandedState:    make(map[string]bool),
		hiddenState:      make(map[string]bool),
		globalStart:      globalStart,
		globalEnd:        globalEnd,
		chartStart:       globalStart,
		chartEnd:         globalEnd,
		keys:             DefaultKeyMap(),
		width:            80,
		height:           24,
		inputURLs:        inputURLs,
		selectionStart:   -1, // no range selection initially
		reloadFunc:       reloadFunc,
		openPerfettoFunc: openPerfettoFunc,
		spinner:          s,
		spans:            spans,
		treeWidth:        defaultTreeWidth,
	}

	// Calculate summary statistics
	m.summary = analyzer.CalculateSummary(spans, m.enricher)
	m.wallTimeMs = globalEnd.Sub(globalStart).Milliseconds()
	if m.wallTimeMs < 0 {
		m.wallTimeMs = 0
	}
	m.computeMs, m.stepCount = calculateComputeAndSteps(spans, m.enricher)

	// Initialize displayed stats to match full stats
	m.displayedSummary = m.summary
	m.displayedWallTimeMs = m.wallTimeMs
	m.displayedComputeMs = m.computeMs
	m.displayedStepCount = m.stepCount

	// Build tree from spans
	m.roots = analyzer.BuildTreeFromSpans(spans, globalStart, globalEnd, m.enricher)

	// Extract VCS, artifact, and workflow file stats from root span attributes
	seenFiles := make(map[string]bool)
	for _, root := range m.roots {
		if n, _ := strconv.Atoi(root.Attrs["vcs.changes.count"]); n > 0 {
			m.changedFilesCount = n
			m.changedFilesAdd, _ = strconv.Atoi(root.Attrs["vcs.changes.additions"])
			m.changedFilesDel, _ = strconv.Atoi(root.Attrs["vcs.changes.deletions"])
		}
		if n, _ := strconv.Atoi(root.Attrs["cicd.pipeline.artifacts.count"]); n > 0 {
			m.artifactsCount += n
			m.artifactsSize = root.Attrs["cicd.pipeline.artifacts.size"]
			m.artifactNames = root.Attrs["cicd.pipeline.artifacts.names"]
		}
		if p := root.Attrs["cicd.pipeline.definition"]; p != "" && !seenFiles[p] {
			seenFiles[p] = true
			m.workflowFiles = append(m.workflowFiles, p)
		}
	}

	// Expand URL groups + workflows for multi-URL, just workflows for single
	if len(inputURLs) > 1 {
		m.expandAllToDepth(1)
	} else {
		m.expandAllToDepth(0)
	}

	for _, opt := range opts {
		opt(&m)
	}

	m.rebuildItems()
	m.hideActivityGroups()
	m.recalculateEffectiveTimes()
	m.recalculateChartBounds()
	return m
}

// ModelOption configures optional Model features.
type ModelOption func(*Model)

// WithLogFetchFunc sets the function used for on-demand step log fetching.
func WithLogFetchFunc(f LogFetchFunc) ModelOption {
	return func(m *Model) {
		m.logFetchFunc = f
	}
}

// calculateComputeAndSteps calculates total compute time and step count from spans
// using the enricher to classify spans by category instead of hardcoding GHA types.
func calculateComputeAndSteps(spans []trace.ReadOnlySpan, enricher enrichment.Enricher) (computeMs int64, stepCount int) {
	for _, s := range spans {
		attrs := make(map[string]string)
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsString()
		}
		isZeroDuration := s.EndTime().Before(s.StartTime()) || s.EndTime().Equal(s.StartTime())
		hints := enricher.Enrich(s.Name(), attrs, isZeroDuration)
		if hints.Category == "" || hints.IsMarker {
			continue
		}
		// Only direct jobs/tasks contribute to compute time,
		// not nested intermediate spans from embedded traces (e.g. Bazel).
		if hints.Category == "job" || hints.Category == "task" {
			duration := s.EndTime().Sub(s.StartTime()).Milliseconds()
			if duration > 0 {
				computeMs += duration
			}
		}
		if hints.IsLeaf {
			stepCount++
		}
	}
	return
}

// expandAllToDepth expands all items up to the given depth
func (m *Model) expandAllToDepth(maxDepth int) {
	if len(m.inputURLs) > 1 {
		// Multi-URL: URL groups are at depth 0, tree roots at depth 1
		// Group roots by URLIndex (must match BuildTreeItems grouping)
		grouped := make(map[int][]*analyzer.TreeNode)
		for _, root := range m.roots {
			grouped[root.URLIndex] = append(grouped[root.URLIndex], root)
		}
		for urlIdx := range m.inputURLs {
			groupID := fmt.Sprintf("url-group/%d", urlIdx)
			if 0 <= maxDepth {
				m.expandedState[groupID] = true
			}
			var expand func(nodes []*analyzer.TreeNode, parentID string, depth int)
			expand = func(nodes []*analyzer.TreeNode, parentID string, depth int) {
				for i, node := range nodes {
					if depth <= maxDepth {
						id := makeNodeID(parentID, node.Name, i)
						m.expandedState[id] = true
						expand(node.Children, id, depth+1)
					}
				}
			}
			if 1 <= maxDepth {
				expand(grouped[urlIdx], groupID, 1)
			}
		}
	} else {
		var expand func(nodes []*analyzer.TreeNode, parentID string, depth int)
		expand = func(nodes []*analyzer.TreeNode, parentID string, depth int) {
			for i, node := range nodes {
				if depth <= maxDepth {
					id := makeNodeID(parentID, node.Name, i)
					m.expandedState[id] = true
					expand(node.Children, id, depth+1)
				}
			}
		}
		expand(m.roots, "", 0)
	}
}

// Init implements tea.Model
func (m Model) Init() tea.Cmd {
	return tea.WindowSize()
}

// Update implements tea.Model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case LogFetchResultMsg:
		if m.logFetchedJobIDs == nil {
			m.logFetchedJobIDs = make(map[int64]bool)
		}
		m.logFetchedJobIDs[m.logFetchingJobID] = true
		m.logFetchingJobID = 0
		m.logFetchInline = nil
		if msg.err != nil {
			m.reloadError = fmt.Sprintf("Log fetch failed: %v", msg.err)
			return m, nil
		}
		m.reloadError = ""
		if len(msg.newSpans) > 0 {
			m.spans = append(m.spans, msg.newSpans...)
			m.roots = analyzer.BuildTreeFromSpans(m.spans, m.globalStart, m.globalEnd, m.enricher)
			m.rebuildItems()
			m.recalculateEffectiveTimes()
			m.recalculateChartBounds()
		}
		return m, nil

	case ReloadResultMsg:
		m.isLoading = false
		m.progressCh = nil
		m.resultCh = nil
		m.loadingPhase = ""
		m.loadingDetail = ""
		m.loadingURL = ""
		if msg.err != nil {
			m.reloadError = msg.err.Error()
			return m, nil
		}
		m.reloadError = "" // clear previous error on success
		// Update model with new data
		m.globalStart = msg.globalStart
		m.globalEnd = msg.globalEnd
		m.chartStart = msg.globalStart
		m.chartEnd = msg.globalEnd
		m.summary = analyzer.CalculateSummary(msg.spans, m.enricher)
		m.wallTimeMs = msg.globalEnd.Sub(msg.globalStart).Milliseconds()
		if m.wallTimeMs < 0 {
			m.wallTimeMs = 0
		}
		m.computeMs, m.stepCount = calculateComputeAndSteps(msg.spans, m.enricher)
		m.roots = analyzer.BuildTreeFromSpans(msg.spans, msg.globalStart, msg.globalEnd, m.enricher)
		m.expandedState = make(map[string]bool)
		m.hiddenState = make(map[string]bool)
		if len(m.inputURLs) > 1 {
			m.expandAllToDepth(1)
		} else {
			m.expandAllToDepth(0)
		}
		m.rebuildItems()
		m.hideActivityGroups()
		m.recalculateEffectiveTimes()
		m.recalculateChartBounds()
		m.cursor = 0
		m.selectionStart = -1
		m.logicalEndID = ""
		m.logicalEndTime = time.Time{}
		return m, nil

	case spinner.TickMsg:
		if m.isLoading || m.logFetchInline != nil {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil

	case LoadingProgressMsg:
		if msg.Phase != "" {
			m.loadingPhase = msg.Phase
		}
		if msg.Detail != "" {
			m.loadingDetail = msg.Detail
		}
		if msg.URL != "" {
			m.loadingURL = msg.URL
		}
		// Continue listening for more progress updates
		return m, m.listenForProgress()

	case clearCopyFeedbackMsg:
		m.inspectorCopyMsg = ""
		return m, nil

	case tea.KeyMsg:
		// Ignore keys while loading (except quit)
		if m.isLoading {
			if key.Matches(msg, m.keys.Quit) {
				return m, tea.Quit
			}
			return m, nil
		}

		// Dismiss error bar on Esc
		if m.reloadError != "" && msg.Type == tea.KeyEsc {
			m.reloadError = ""
			return m, nil
		}

		// Handle help modal first
		if m.showHelpModal {
			switch msg.String() {
			case "esc", "enter", "?", "q":
				m.showHelpModal = false
				return m, nil
			}
			return m, nil
		}

		// Handle detail modal
		if m.showDetailModal {
			// Inspector search input mode
			if m.inspectorSearching {
				switch msg.Type {
				case tea.KeyEsc:
					m.inspectorSearching = false
					m.inspectorSearchQuery = ""
					m.inspectorSearchMatches = nil
					m.inspectorSearchIdx = -1
					return m, nil
				case tea.KeyEnter:
					m.inspectorSearching = false
					if len(m.inspectorSearchMatches) > 0 {
						m.inspectorSearchIdx = 0
						m.inspectorJumpToMatch()
					}
					return m, nil
				case tea.KeyBackspace:
					if len(m.inspectorSearchQuery) > 0 {
						m.inspectorSearchQuery = m.inspectorSearchQuery[:len(m.inspectorSearchQuery)-1]
						m.updateInspectorSearch()
					}
					return m, nil
				default:
					if msg.Type == tea.KeyRunes {
						m.inspectorSearchQuery += string(msg.Runes)
						m.updateInspectorSearch()
						// Auto-jump to first match
						if len(m.inspectorSearchMatches) > 0 {
							m.inspectorSearchIdx = 0
							m.inspectorJumpToMatch()
						}
					}
					return m, nil
				}
			}

			// Normal modal keys
			switch msg.String() {
			case "esc":
				if m.inspectorSearchQuery != "" {
					// Clear search
					m.inspectorSearchQuery = ""
					m.inspectorSearchMatches = nil
					m.inspectorSearchIdx = -1
					return m, nil
				}
				if m.inspectorNavigateBack() {
					return m, nil
				}
				m.resetInspectorModal()
				return m, nil
			case "i", "q":
				m.resetInspectorModal()
				return m, nil
			case "tab":
				// Switch between sidebar and tree pane
				m.inspectorFocusLeft = !m.inspectorFocusLeft
				return m, nil
			case "up", "k":
				if m.inspectorFocusLeft {
					if m.inspectorSidebarIdx > 0 {
						m.inspectorSidebarIdx--
						m.rebuildInspectorFlat()
					}
				} else {
					if m.inspectorCursor > 0 {
						m.inspectorCursor--
					}
				}
				return m, nil
			case "down", "j":
				if m.inspectorFocusLeft {
					if m.inspectorSidebarIdx < len(m.inspectorNodes)-1 {
						m.inspectorSidebarIdx++
						m.rebuildInspectorFlat()
					}
				} else {
					if m.inspectorCursor < len(m.inspectorFlat)-1 {
						m.inspectorCursor++
					}
				}
				return m, nil
			case "left", "h":
				if m.inspectorFocusLeft {
					// No-op on sidebar
					return m, nil
				}
				// Collapse current node, or move to parent
				if m.inspectorCursor < len(m.inspectorFlat) {
					entry := m.inspectorFlat[m.inspectorCursor]
					if entry.Node.Expanded && len(entry.Node.Children) > 0 {
						entry.Node.Expanded = false
						m.rebuildInspectorFlat()
					} else {
						parentIdx := FindParentIndex(m.inspectorFlat, m.inspectorCursor)
						if parentIdx >= 0 {
							m.inspectorCursor = parentIdx
						} else {
							// At top level, switch to sidebar
							m.inspectorFocusLeft = true
						}
					}
				}
				return m, nil
			case "right", "l":
				if m.inspectorFocusLeft {
					// Jump into the tree pane
					m.inspectorFocusLeft = false
					return m, nil
				}
				if m.inspectorCursor < len(m.inspectorFlat) {
					entry := m.inspectorFlat[m.inspectorCursor]
					if !entry.Node.Expanded && len(entry.Node.Children) > 0 {
						entry.Node.Expanded = true
						m.rebuildInspectorFlat()
					}
				}
				return m, nil
			case " ", "enter":
				if m.inspectorFocusLeft {
					// Select section and jump to tree
					m.inspectorFocusLeft = false
					m.inspectorCursor = 0
					m.modalScroll = 0
					return m, nil
				}
				if m.inspectorCursor < len(m.inspectorFlat) {
					entry := m.inspectorFlat[m.inspectorCursor]
					// Navigate into child span
					if entry.Node.ChildItem != nil {
						m.inspectorNavigateIntoChild(entry.Node.ChildItem)
						return m, nil
					}
					if len(entry.Node.Children) > 0 {
						entry.Node.Expanded = !entry.Node.Expanded
						m.rebuildInspectorFlat()
					}
				}
				return m, nil
			case "]":
				// Navigate to next item in main tree
				if m.cursor < len(m.visibleItems)-1 {
					m.cursor++
					m.modalScroll = 0
					item := m.visibleItems[m.cursor]
					m.modalItem = &item
					m.inspectorNodes = BuildInspectorTree(m.modalItem)
					m.inspectorSidebarIdx = 0
					m.rebuildInspectorFlat()
					m.inspectorCursor = 0
					m.inspectorBreadcrumb = nil
				}
				return m, nil
			case "[":
				// Navigate to previous item in main tree
				if m.cursor > 0 {
					m.cursor--
					m.modalScroll = 0
					item := m.visibleItems[m.cursor]
					m.modalItem = &item
					m.inspectorNodes = BuildInspectorTree(m.modalItem)
					m.inspectorSidebarIdx = 0
					m.rebuildInspectorFlat()
					m.inspectorCursor = 0
					m.inspectorBreadcrumb = nil
				}
				return m, nil
			case "/":
				m.inspectorSearching = true
				m.inspectorSearchQuery = ""
				m.inspectorSearchMatches = nil
				m.inspectorSearchIdx = -1
				return m, nil
			case "n":
				// Next search match
				if len(m.inspectorSearchMatches) > 0 {
					m.inspectorSearchIdx = (m.inspectorSearchIdx + 1) % len(m.inspectorSearchMatches)
					m.inspectorJumpToMatch()
				}
				return m, nil
			case "N":
				// Previous search match
				if len(m.inspectorSearchMatches) > 0 {
					m.inspectorSearchIdx--
					if m.inspectorSearchIdx < 0 {
						m.inspectorSearchIdx = len(m.inspectorSearchMatches) - 1
					}
					m.inspectorJumpToMatch()
				}
				return m, nil
			case "c":
				cmd := m.inspectorCopyValue()
				return m, cmd
			case "o":
				m.inspectorOpenValue()
				return m, nil
			case "backspace":
				// Navigate back in breadcrumb
				if m.inspectorNavigateBack() {
					return m, nil
				}
				return m, nil
			case "r":
				m.resetInspectorModal()
				if m.reloadFunc != nil {
					m.isLoading = true
					return m, tea.Batch(m.spinner.Tick, m.doReload())
				}
				return m, nil
			case "p":
				if m.openPerfettoFunc != nil {
					m.openPerfettoFunc(m.visibleSpans(), m.isActivityHidden())
				}
				return m, nil
			case "g":
				if m.inspectorFocusLeft {
					m.inspectorSidebarIdx = 0
					m.rebuildInspectorFlat()
				} else {
					m.inspectorCursor = 0
				}
				return m, nil
			case "G":
				if m.inspectorFocusLeft {
					m.inspectorSidebarIdx = len(m.inspectorNodes) - 1
					m.rebuildInspectorFlat()
				} else {
					if len(m.inspectorFlat) > 0 {
						m.inspectorCursor = len(m.inspectorFlat) - 1
					}
				}
				return m, nil
			}

			// Handle Enter on a tree item to navigate into children (breadcrumb)
			// This is handled via "enter" key above for expand/collapse

			return m, nil
		}

		// Handle search input mode
		if m.isSearching {
			switch msg.Type {
			case tea.KeyEsc:
				m.isSearching = false
				m.searchQuery = ""
				m.searchMatchIDs = nil
				m.searchAncIDs = nil
				m.rebuildItems()
				m.recalculateChartBounds()
				return m, nil
			case tea.KeyEnter, tea.KeyDown, tea.KeyTab:
				// Exit search input but keep filter active
				m.isSearching = false
				return m, nil
			case tea.KeyBackspace:
				if len(m.searchQuery) > 0 {
					_, size := utf8.DecodeLastRuneInString(m.searchQuery)
					m.searchQuery = m.searchQuery[:len(m.searchQuery)-size]
				}
				m.applySearchFilter()
				m.rebuildItems()
				return m, nil
			default:
				if msg.Type == tea.KeyRunes {
					m.searchQuery += string(msg.Runes)
					m.applySearchFilter()
					m.rebuildItems()
				}
				return m, nil
			}
		}

		// Esc or Enter clears active search filter (when not in input mode).
		// Enter preserves cursor on the current item in the full tree;
		// Esc simply clears and resets.
		if m.searchQuery != "" && (msg.Type == tea.KeyEsc || msg.Type == tea.KeyEnter) {
			// Remember current item ID so we can find it after rebuild
			var curID string
			if m.cursor >= 0 && m.cursor < len(m.visibleItems) {
				curID = m.visibleItems[m.cursor].ID
			}
			m.searchQuery = ""
			m.searchMatchIDs = nil
			m.searchAncIDs = nil
			m.rebuildItems()
			m.recalculateChartBounds()
			// Restore cursor to the same item in the unfiltered list
			if curID != "" {
				for i, item := range m.visibleItems {
					if item.ID == curID {
						m.cursor = i
						break
					}
				}
			}
			return m, nil
		}

		// Handle vim-style two-key sequences (gg / GG)
		if key.Matches(msg, m.keys.GoTop) {
			if m.pendingG {
				m.pendingG = false
				m.selectionStart = -1
				m.cursor = 0
				return m, nil
			}
			m.pendingG = true
			m.pendingGG = false
			return m, nil
		}
		if key.Matches(msg, m.keys.GoBottom) {
			if m.pendingGG {
				m.pendingGG = false
				m.selectionStart = -1
				if len(m.visibleItems) > 0 {
					m.cursor = len(m.visibleItems) - 1
				}
				return m, nil
			}
			m.pendingGG = true
			m.pendingG = false
			return m, nil
		}
		// Any other key clears pending g/G state
		m.pendingG = false
		m.pendingGG = false

		switch {
		case key.Matches(msg, m.keys.Quit):
			return m, tea.Quit

		case key.Matches(msg, m.keys.Info):
			m.openDetailModal()
			return m, nil

		case key.Matches(msg, m.keys.Reload):
			if m.reloadFunc != nil {
				m.isLoading = true
				return m, tea.Batch(m.spinner.Tick, m.doReload())
			}
			return m, nil

		case key.Matches(msg, m.keys.Logs):
			if cmd := m.fetchLogsForCurrentItem(); cmd != nil {
				return m, tea.Batch(m.spinner.Tick, cmd)
			}
			return m, nil

		case key.Matches(msg, m.keys.Up):
			m.selectionStart = -1 // clear selection
			if m.cursor > 0 {
				m.cursor--
			}

		case key.Matches(msg, m.keys.Down):
			m.selectionStart = -1 // clear selection
			if m.cursor < len(m.visibleItems)-1 {
				m.cursor++
			}

		case key.Matches(msg, m.keys.ShiftUp):
			// Start or extend selection upward
			if m.selectionStart == -1 {
				m.selectionStart = m.cursor
			}
			if m.cursor > 0 {
				m.cursor--
			}

		case key.Matches(msg, m.keys.ShiftDown):
			// Start or extend selection downward
			if m.selectionStart == -1 {
				m.selectionStart = m.cursor
			}
			if m.cursor < len(m.visibleItems)-1 {
				m.cursor++
			}

		case key.Matches(msg, m.keys.Left):
			m.selectionStart = -1 // clear selection
			m.collapseOrGoToParent()

		case key.Matches(msg, m.keys.Right), key.Matches(msg, m.keys.Enter):
			m.selectionStart = -1 // clear selection
			m.expandOrToggle()

		case key.Matches(msg, m.keys.Space):
			m.toggleChartVisibility()
			// Keep selection so user can toggle again or see what was selected

		case key.Matches(msg, m.keys.Open):
			m.openCurrentItem()

		case key.Matches(msg, m.keys.Focus):
			m.toggleFocus()

		case key.Matches(msg, m.keys.ToggleExpandAll):
			m.toggleExpandAll()

		case key.Matches(msg, m.keys.Perfetto):
			if m.openPerfettoFunc != nil {
				m.openPerfettoFunc(m.visibleSpans(), m.isActivityHidden())
			}

		case key.Matches(msg, m.keys.Mouse):
			m.mouseEnabled = !m.mouseEnabled
			if m.mouseEnabled {
				return m, tea.EnableMouseCellMotion
			}
			return m, tea.DisableMouse

		case key.Matches(msg, m.keys.Search):
			m.isSearching = true
			m.searchQuery = ""
			m.searchMatchIDs = nil
			m.searchAncIDs = nil
			return m, nil

		case key.Matches(msg, m.keys.LogicalEnd):
			m.toggleLogicalEnd()
			return m, nil

		case key.Matches(msg, m.keys.Sort):
			m.sortMode = m.sortMode.Next()
			m.rebuildItems()
			return m, nil

		case key.Matches(msg, m.keys.ResizeLeft):
			if m.treeWidth-treeWidthStep >= minTreeWidth {
				m.treeWidth -= treeWidthStep
			}
			return m, nil

		case key.Matches(msg, m.keys.ResizeRight):
			if m.treeWidth+treeWidthStep <= maxTreeWidth {
				m.treeWidth += treeWidthStep
			}
			return m, nil

		case key.Matches(msg, m.keys.NextFailed):
			m.jumpToNext(func(item TreeItem) bool {
				return item.Hints.Outcome == "failure"
			})
			return m, nil

		case key.Matches(msg, m.keys.NextBottleneck):
			m.jumpToNext(func(item TreeItem) bool {
				return item.IsBottleneck
			})
			return m, nil

		case key.Matches(msg, m.keys.PageUp):
			m.selectionStart = -1
			halfPage := m.pageSize() / 2
			m.cursor -= halfPage
			if m.cursor < 0 {
				m.cursor = 0
			}
			return m, nil

		case key.Matches(msg, m.keys.PageDown):
			m.selectionStart = -1
			halfPage := m.pageSize() / 2
			m.cursor += halfPage
			if m.cursor >= len(m.visibleItems) {
				m.cursor = len(m.visibleItems) - 1
			}
			if m.cursor < 0 {
				m.cursor = 0
			}
			return m, nil

		case key.Matches(msg, m.keys.Help):
			m.showHelpModal = true
			return m, nil
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, tea.ClearScreen

	case tea.MouseMsg:
		// Ignore mouse while loading
		if m.isLoading {
			return m, nil
		}

		// Handle mouse in modal
		if m.showDetailModal {
			switch msg.Button {
			case tea.MouseButtonWheelUp:
				if m.modalScroll > 0 {
					m.modalScroll--
				}
			case tea.MouseButtonWheelDown:
				m.modalScroll++
			case tea.MouseButtonLeft:
				if msg.Action == tea.MouseActionRelease {
					// Click outside modal area could close it (optional)
				}
			}
			return m, nil
		}

		// Handle mouse in main view
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			// Scroll up
			m.selectionStart = -1
			if m.cursor > 0 {
				m.cursor--
			}
		case tea.MouseButtonWheelDown:
			// Scroll down
			m.selectionStart = -1
			if m.cursor < len(m.visibleItems)-1 {
				m.cursor++
			}
		case tea.MouseButtonLeft:
			if msg.Action == tea.MouseActionRelease {
				// Calculate which row was clicked
				headerLines := 8
				if m.hasEnrichmentLine() {
					headerLines++
				}
				clickedRow := msg.Y - headerLines

				// Calculate scroll offset
				availableHeight := m.height - headerLines - 4
				if availableHeight < 1 {
					availableHeight = 10
				}

				startIdx := 0
				if len(m.visibleItems) > availableHeight {
					halfHeight := availableHeight / 2
					startIdx = m.cursor - halfHeight
					if startIdx < 0 {
						startIdx = 0
					}
					if startIdx+availableHeight > len(m.visibleItems) {
						startIdx = len(m.visibleItems) - availableHeight
						if startIdx < 0 {
							startIdx = 0
						}
					}
				}

				// Convert click position to item index
				itemIdx := startIdx + clickedRow
				if itemIdx >= 0 && itemIdx < len(m.visibleItems) {
					m.selectionStart = -1
					m.cursor = itemIdx
				}
			}
		}
	}

	return m, nil
}

// View implements tea.Model
func (m Model) View() string {
	// Enforce minimum dimensions to prevent crashes
	width := m.width
	height := m.height
	if width < 40 {
		width = 40
	}
	if height < 10 {
		height = 10
	}

	// Show loading overlay if reloading
	if m.isLoading {
		loadingText := m.renderLoadingView()
		return placeModalCentered(ModalStyle.Render(loadingText), width, height)
	}

	var b strings.Builder

	// Calculate available height for items
	// Header: topBorder + statsLine1 + statsLine2 + timeAxis + blankLine = 5
	// Footer: breadcrumb + statusLine + bottomBorder = 3
	headerLines := 5
	footerLines := 3
	if m.hasEnrichmentLine() {
		headerLines++ // queue/retry/billable line
	}
	searchActive := m.isSearching || m.searchQuery != ""
	if searchActive {
		headerLines++ // search bar takes one line
	}
	if m.reloadError != "" {
		headerLines++ // error bar takes one line
	}
	availableHeight := height - headerLines - footerLines
	if availableHeight < 1 {
		availableHeight = 10
	}

	// Determine if scrolling is needed
	totalItems := len(m.visibleItems)
	needsScroll := totalItems > availableHeight

	// Header (includes time range info)
	b.WriteString(m.renderHeader())
	b.WriteString("\n")

	// Time axis row (shows start time, duration, end time aligned with timeline)
	b.WriteString(m.renderTimeAxis())
	b.WriteString("\n")

	// Blank line between time axis and content (just outer borders, no middle separator)
	totalWidth := width - horizontalPad*2
	if totalWidth < 1 {
		totalWidth = 80
	}
	contentWidth := totalWidth - 2 // space between left and right borders
	blankLine := BorderStyle.Render("│") + strings.Repeat(" ", contentWidth) + BorderStyle.Render("│")
	b.WriteString(blankLine)
	b.WriteString("\n")

	// Search bar (between blank line and content)
	if searchActive {
		b.WriteString(m.renderSearchBar(contentWidth))
		b.WriteString("\n")
	}

	// Error bar (shown after reload failure)
	if m.reloadError != "" {
		b.WriteString(m.renderErrorBar(contentWidth))
		b.WriteString("\n")
	}

	// Determine scroll window
	startIdx := 0
	endIdx := totalItems

	if needsScroll {
		// Center cursor in view
		halfHeight := availableHeight / 2
		startIdx = m.cursor - halfHeight
		if startIdx < 0 {
			startIdx = 0
		}
		endIdx = startIdx + availableHeight
		if endIdx > totalItems {
			endIdx = totalItems
			startIdx = endIdx - availableHeight
			if startIdx < 0 {
				startIdx = 0
			}
		}
	}

	// Calculate scrollbar dimensions (80% height, centered)
	trackHeight := availableHeight * 80 / 100
	if trackHeight < 3 {
		trackHeight = min(3, availableHeight)
	}
	trackTopPad := (availableHeight - trackHeight) / 2
	trackBottomPad := availableHeight - trackHeight - trackTopPad

	// Calculate thumb position within track
	thumbSize := 1
	thumbStart := 0
	if needsScroll && trackHeight > 0 {
		thumbSize = max(1, trackHeight*availableHeight/totalItems)
		if thumbSize > trackHeight {
			thumbSize = trackHeight
		}
		maxScroll := totalItems - availableHeight
		if maxScroll > 0 {
			thumbStart = startIdx * (trackHeight - thumbSize) / maxScroll
		}
	}
	thumbEnd := thumbStart + thumbSize

	// Scrollbar characters (use subtle separator color)
	scrollThumb := SeparatorStyle.Render("┃")
	scrollTrack := SeparatorStyle.Render("│")

	// Render visible items with scrollbar
	rowIdx := 0
	for i := startIdx; i < endIdx; i++ {
		item := m.visibleItems[i]
		isSelected := m.isInSelection(i)
		b.WriteString(m.renderItem(item, isSelected, i))

		// Add scrollbar character
		if needsScroll {
			trackIdx := rowIdx - trackTopPad
			if rowIdx < trackTopPad || rowIdx >= availableHeight-trackBottomPad {
				b.WriteString(" ")
			} else if trackIdx >= thumbStart && trackIdx < thumbEnd {
				b.WriteString(scrollThumb)
			} else {
				b.WriteString(scrollTrack)
			}
		}
		b.WriteString("\n")
		rowIdx++
	}

	// Pad if needed (with separator matching item rows)
	renderedItems := endIdx - startIdx
	for i := renderedItems; i < availableHeight; i++ {
		padTotalWidth := width - horizontalPad*2 // account for left/right padding
		if padTotalWidth < 1 {
			padTotalWidth = 80
		}
		// Match the structure: │ space tree │ timeline │
		treeW := m.treeWidth
		availableW := padTotalWidth - 4 // 3 border chars + 1 left padding
		timelineW := availableW - treeW
		if timelineW < 10 {
			timelineW = 10
		}
		timelinePad := strings.Repeat(" ", timelineW)
		endCol := m.logicalEndCol(timelineW)
		if endCol >= 0 {
			timelinePad = overlayLogicalEndLine(timelinePad, endCol, timelineW, false)
		}
		b.WriteString(BorderStyle.Render("│") + " " + strings.Repeat(" ", treeW) + SeparatorStyle.Render("│") + timelinePad + BorderStyle.Render("│"))

		// Add scrollbar character for empty rows
		if needsScroll {
			trackIdx := rowIdx - trackTopPad
			if rowIdx < trackTopPad || rowIdx >= availableHeight-trackBottomPad {
				b.WriteString(" ")
			} else if trackIdx >= thumbStart && trackIdx < thumbEnd {
				b.WriteString(scrollThumb)
			} else {
				b.WriteString(scrollTrack)
			}
		}
		b.WriteString("\n")
		rowIdx++
	}

	// Footer
	b.WriteString(m.renderFooter())

	// Overlay modal if showing
	if m.showHelpModal {
		modal := m.renderHelpModal()
		return placeModalCentered(modal, width, height)
	}

	if m.showDetailModal {
		modal, maxScroll := m.renderDetailModal(height-4, width-10)
		// Clamp scroll to valid range
		if m.modalScroll > maxScroll {
			m.modalScroll = maxScroll
		}
		return placeModalCentered(modal, width, height)
	}

	// Add horizontal padding to each line
	return addHorizontalPadding(b.String(), horizontalPad)
}

// addHorizontalPadding adds left padding to each line
func addHorizontalPadding(content string, pad int) string {
	lines := strings.Split(content, "\n")
	padStr := strings.Repeat(" ", pad)
	var result strings.Builder
	for i, line := range lines {
		result.WriteString(padStr)
		result.WriteString(line)
		if i < len(lines)-1 {
			result.WriteString("\n")
		}
	}
	return result.String()
}

// applySearchFilter computes searchMatchIDs and searchAncIDs based on searchQuery.
// It also auto-expands ancestors of matching items and zooms the timeline to match range.
func (m *Model) applySearchFilter() {
	if m.searchQuery == "" {
		m.searchMatchIDs = nil
		m.searchAncIDs = nil
		// Restore original chart bounds
		m.recalculateChartBounds()
		return
	}

	query := strings.ToLower(m.searchQuery)
	m.searchMatchIDs = make(map[string]bool)
	m.searchAncIDs = make(map[string]bool)

	// Walk tree items recursively looking for matches
	var earliest, latest time.Time
	var walk func(items []*TreeItem)
	walk = func(items []*TreeItem) {
		for _, item := range items {
			if strings.Contains(strings.ToLower(item.Name), query) {
				m.searchMatchIDs[item.ID] = true
				// Collect and expand ancestors
				m.addAncestors(item.ParentID)
				// Track time range of matched items for filter-zoom
				if !item.StartTime.IsZero() && (earliest.IsZero() || item.StartTime.Before(earliest)) {
					earliest = item.StartTime
				}
				if !item.EndTime.IsZero() && (latest.IsZero() || item.EndTime.After(latest)) {
					latest = item.EndTime
				}
			}
			walk(item.Children)
		}
	}
	walk(m.treeItems)

	// Zoom timeline to matched items' time range
	if !earliest.IsZero() && !latest.IsZero() {
		m.chartStart = earliest
		m.chartEnd = latest
	}
}

// addAncestors walks up the tree from parentID, adding each ancestor to searchAncIDs
// and expanding them so they become visible.
func (m *Model) addAncestors(parentID string) {
	if parentID == "" {
		return
	}
	if m.searchAncIDs[parentID] {
		return // already processed
	}
	m.searchAncIDs[parentID] = true
	m.expandedState[parentID] = true

	// Find the parent item to continue up
	var findParent func(items []*TreeItem) string
	findParent = func(items []*TreeItem) string {
		for _, item := range items {
			if item.ID == parentID {
				return item.ParentID
			}
			if found := findParent(item.Children); found != "" {
				return found
			}
		}
		return ""
	}
	grandparentID := findParent(m.treeItems)
	if grandparentID != "" {
		m.addAncestors(grandparentID)
	}
}

// toggleLogicalEnd toggles the logical end marker on the current cursor item.
// If the cursor item is already the marker, it clears it. Otherwise it sets it.
func (m *Model) toggleLogicalEnd() {
	if m.cursor >= len(m.visibleItems) {
		return
	}
	item := m.visibleItems[m.cursor]
	if item.ID == m.logicalEndID {
		// Clear the marker
		m.logicalEndID = ""
		m.logicalEndTime = time.Time{}
	} else {
		// Set the marker
		m.logicalEndID = item.ID
		m.logicalEndTime = item.EndTime
		// For zero-duration items (markers), use StartTime
		if item.EndTime.IsZero() || item.EndTime.Equal(item.StartTime) {
			m.logicalEndTime = item.StartTime
		}
	}
}

// jumpToNext moves the cursor to the next item matching the predicate, wrapping around.
func (m *Model) jumpToNext(pred func(TreeItem) bool) {
	if len(m.visibleItems) == 0 {
		return
	}
	// Search forward from cursor+1, then wrap
	for offset := 1; offset < len(m.visibleItems); offset++ {
		idx := (m.cursor + offset) % len(m.visibleItems)
		if pred(m.visibleItems[idx]) {
			m.selectionStart = -1
			m.cursor = idx
			return
		}
	}
}

// pageSize returns the number of visible rows in the viewport.
func (m *Model) pageSize() int {
	headerLines := 5
	footerLines := 3
	if m.hasEnrichmentLine() {
		headerLines++
	}
	if m.isSearching || m.searchQuery != "" {
		headerLines++
	}
	if m.reloadError != "" {
		headerLines++
	}
	available := m.height - headerLines - footerLines
	if available < 1 {
		available = 10
	}
	return available
}

// logicalEndCol returns the timeline column position for the logical end marker.
// Returns -1 if no marker is set or the position is outside the chart bounds.
func (m *Model) logicalEndCol(timelineW int) int {
	if m.logicalEndID == "" || m.logicalEndTime.IsZero() || m.chartStart.IsZero() || m.chartEnd.IsZero() {
		return -1
	}
	chartDuration := m.chartEnd.Sub(m.chartStart)
	if chartDuration <= 0 {
		return -1
	}
	endOffset := m.logicalEndTime.Sub(m.chartStart)
	col := int(float64(endOffset) / float64(chartDuration) * float64(timelineW))
	if col >= timelineW {
		col = timelineW - 1
	}
	if col < 0 {
		col = 0
	}
	return col
}

// isAfterLogicalEnd returns true when the logical end is set and the item
// starts strictly after the logical end time.
func (m *Model) isAfterLogicalEnd(item TreeItem) bool {
	if m.logicalEndID == "" {
		return false
	}
	return item.StartTime.After(m.logicalEndTime)
}

// rebuildVisibleItems re-flattens and re-filters the visible item list
// without rebuilding the tree from scratch.
func (m *Model) rebuildVisibleItems() {
	m.visibleItems = FlattenVisibleItems(m.treeItems, m.expandedState, m.sortMode)
	m.preFilterCount = len(m.visibleItems)

	// Apply search filter if active
	if m.searchQuery != "" && (m.searchMatchIDs != nil || m.searchAncIDs != nil) {
		m.visibleItems = FilterVisibleItems(m.visibleItems, m.searchMatchIDs, m.searchAncIDs)
	}

	// Precompute tree connector characters for the final visible items
	m.precomputeTreeConnectors()

	// Ensure cursor is valid
	if m.cursor >= len(m.visibleItems) {
		m.cursor = len(m.visibleItems) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
}

// precomputeTreeConnectors builds connector runes for each visible item.
// Each item gets a slice of runes (one per depth level) indicating what
// connector character to draw:
//
//	'├' = branch (more siblings below)
//	'└' = last branch (no more siblings)
//	'│' = vertical continuation (ancestor has more siblings)
//	' ' = blank (ancestor was the last child)
func (m *Model) precomputeTreeConnectors() {
	n := len(m.visibleItems)
	m.treeConnectors = make([][]rune, n)
	if n == 0 {
		return
	}

	// Find the last visible child index for each parentID
	lastChildOfParent := make(map[string]int)
	for i, item := range m.visibleItems {
		if item.ParentID != "" {
			lastChildOfParent[item.ParentID] = i
		}
	}

	// Find max depth for guide stack allocation
	maxDepth := 0
	for _, item := range m.visibleItems {
		if item.Depth > maxDepth {
			maxDepth = item.Depth
		}
	}

	// guideActive[d] = true means draw │ at depth d (ancestor has more siblings)
	guideActive := make([]bool, maxDepth+1)

	for i, item := range m.visibleItems {
		d := item.Depth
		isLast := item.ParentID == "" || lastChildOfParent[item.ParentID] == i

		// Build connector runes for this item
		connectors := make([]rune, d)
		for level := 0; level < d; level++ {
			if level == d-1 {
				// Item's own level: branch connector
				if isLast {
					connectors[level] = '└'
				} else {
					connectors[level] = '├'
				}
			} else {
				// Ancestor levels: continuation or blank
				if guideActive[level] {
					connectors[level] = '│'
				} else {
					connectors[level] = ' '
				}
			}
		}
		m.treeConnectors[i] = connectors

		// Update guide stack for subsequent items
		if d > 0 {
			guideActive[d-1] = !isLast
		}
		// Clear deeper levels (those scopes have ended)
		for level := d; level <= maxDepth; level++ {
			guideActive[level] = false
		}
	}
}

// snapshotOrigTimes records every tree item's original time bounds into origTimes.
func (m *Model) snapshotOrigTimes() {
	m.origTimes = make(map[string][2]time.Time)
	var walk func(items []*TreeItem)
	walk = func(items []*TreeItem) {
		for _, item := range items {
			m.origTimes[item.ID] = [2]time.Time{item.StartTime, item.EndTime}
			walk(item.Children)
		}
	}
	walk(m.treeItems)
}

// rebuildItems rebuilds the flattened item list based on expanded state
func (m *Model) rebuildItems() {
	m.treeItems = BuildTreeItems(m.roots, m.expandedState, m.inputURLs)
	m.spanIndex = BuildSpanIndex(m.treeItems)
	m.snapshotOrigTimes()
	m.rebuildVisibleItems()

	// Ensure cursor is valid
	if m.cursor >= len(m.visibleItems) {
		m.cursor = len(m.visibleItems) - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
}

// collapseOrGoToParent collapses the current item or moves to its parent
func (m *Model) collapseOrGoToParent() {
	if m.cursor >= len(m.visibleItems) {
		return
	}

	item := m.visibleItems[m.cursor]

	// If item is expanded, collapse it
	if item.HasChildren && m.expandedState[item.ID] {
		m.expandedState[item.ID] = false
		m.rebuildItems()
		return
	}

	// Otherwise, go to parent
	if item.ParentID != "" {
		for i, it := range m.visibleItems {
			if it.ID == item.ParentID {
				m.cursor = i
				break
			}
		}
	}
}

// expandOrToggle expands or toggles the current item
func (m *Model) expandOrToggle() {
	if m.cursor >= len(m.visibleItems) {
		return
	}

	item := m.visibleItems[m.cursor]
	if item.HasChildren {
		m.expandedState[item.ID] = !m.expandedState[item.ID]
		m.rebuildItems()
	}
}

// openCurrentItem opens the URL of the current item in a browser
func (m *Model) openCurrentItem() {
	if m.cursor >= len(m.visibleItems) {
		return
	}

	item := m.visibleItems[m.cursor]
	if item.Hints.URL != "" {
		_ = utils.OpenBrowser(item.Hints.URL)
	}
}

// renderSearchBar renders the search input bar
func (m Model) renderSearchBar(contentWidth int) string {
	// Format: │ / query█              N matches │
	prefix := SearchBarStyle.Render(" / ")
	query := SearchBarStyle.Render(m.searchQuery)
	cursor := ""
	if m.isSearching {
		cursor = SearchBarStyle.Render("█")
	}

	// Count matches as N/M (matching/total)
	matchCount := len(m.searchMatchIDs)
	totalCount := m.preFilterCount
	countStr := ""
	countPlain := ""
	if m.searchQuery != "" {
		countPlain = fmt.Sprintf("%d/%d ", matchCount, totalCount)
		countStr = SearchCountStyle.Render(countPlain)
	}

	// Calculate padding
	prefixWidth := 3 // " / "
	queryWidth := len(m.searchQuery)
	cursorWidth := 0
	if m.isSearching {
		cursorWidth = 1
	}
	countWidth := len(countPlain)

	padWidth := contentWidth - prefixWidth - queryWidth - cursorWidth - countWidth
	if padWidth < 1 {
		padWidth = 1
	}

	return BorderStyle.Render("│") + prefix + query + cursor + strings.Repeat(" ", padWidth) + countStr + BorderStyle.Render("│")
}

// renderErrorBar renders a dismissible error bar after a failed reload
func (m Model) renderErrorBar(contentWidth int) string {
	errMsg := m.reloadError
	prefix := " ✗ Reload failed: "
	maxMsg := contentWidth - len(prefix) - 2 // account for borders
	if len(errMsg) > maxMsg && maxMsg > 3 {
		errMsg = errMsg[:maxMsg-3] + "..."
	}
	text := prefix + errMsg
	textWidth := len(text)
	padWidth := contentWidth - textWidth
	if padWidth < 0 {
		padWidth = 0
	}
	return BorderStyle.Render("│") + FailureStyle.Render(text) + strings.Repeat(" ", padWidth) + BorderStyle.Render("│")
}

// renderLoadingView renders the loading progress display
func (m Model) renderLoadingView() string {
	var b strings.Builder

	// Header
	b.WriteString(ModalTitleStyle.Render("🚀 Reloading Data"))
	b.WriteString("\n\n")

	// URL being processed
	if m.loadingURL != "" {
		b.WriteString(m.spinner.View())
		b.WriteString(" ")
		b.WriteString(m.loadingURL)
		b.WriteString("\n")
	} else {
		b.WriteString(m.spinner.View())
		b.WriteString(" Loading...\n")
	}

	// Phase and detail
	if m.loadingPhase != "" {
		b.WriteString("  ↳ ")
		b.WriteString(m.loadingPhase)
		if m.loadingDetail != "" {
			b.WriteString(" (")
			b.WriteString(m.loadingDetail)
			b.WriteString(")")
		}
		b.WriteString("\n")
	}

	return b.String()
}

// openDetailModal opens the detail modal for the current item
func (m *Model) openDetailModal() {
	if m.cursor >= len(m.visibleItems) {
		return
	}

	item := m.visibleItems[m.cursor]
	m.modalItem = &item
	m.showDetailModal = true
	m.modalScroll = 0
	m.inspectorNodes = BuildInspectorTree(m.modalItem)
	m.inspectorSidebarIdx = 0
	m.inspectorCursor = 0
	m.rebuildInspectorFlat()
	m.inspectorFocusLeft = true
	m.inspectorSearching = false
	m.inspectorSearchQuery = ""
	m.inspectorSearchMatches = nil
	m.inspectorSearchIdx = -1
	m.inspectorBreadcrumb = nil
	m.inspectorCopyMsg = ""
}

// rebuildInspectorFlat rebuilds the flat list based on sidebar selection.
func (m *Model) rebuildInspectorFlat() {
	if m.inspectorSidebarIdx >= 0 && m.inspectorSidebarIdx < len(m.inspectorNodes) {
		m.inspectorFlat = FlattenSingleSection(m.inspectorNodes[m.inspectorSidebarIdx])
	} else {
		m.inspectorFlat = nil
	}
	if m.inspectorCursor >= len(m.inspectorFlat) {
		m.inspectorCursor = len(m.inspectorFlat) - 1
	}
	if m.inspectorCursor < 0 {
		m.inspectorCursor = 0
	}
	m.updateInspectorSearch()
}

// updateInspectorSearch recomputes search matches for the current flat list.
func (m *Model) updateInspectorSearch() {
	if m.inspectorSearchQuery == "" {
		m.inspectorSearchMatches = nil
		m.inspectorSearchIdx = -1
		return
	}
	m.inspectorSearchMatches = SearchInspectorNodes(m.inspectorFlat, m.inspectorSearchQuery)
	if len(m.inspectorSearchMatches) > 0 {
		if m.inspectorSearchIdx < 0 || m.inspectorSearchIdx >= len(m.inspectorSearchMatches) {
			m.inspectorSearchIdx = 0
		}
	} else {
		m.inspectorSearchIdx = -1
	}
}

// inspectorJumpToMatch jumps the cursor to the current search match.
func (m *Model) inspectorJumpToMatch() {
	if m.inspectorSearchIdx >= 0 && m.inspectorSearchIdx < len(m.inspectorSearchMatches) {
		m.inspectorCursor = m.inspectorSearchMatches[m.inspectorSearchIdx]
	}
}

// inspectorCopyValue copies the currently selected node's value to clipboard.
func (m *Model) inspectorCopyValue() tea.Cmd {
	if m.inspectorCursor >= len(m.inspectorFlat) {
		return nil
	}
	entry := m.inspectorFlat[m.inspectorCursor]
	text := entry.Node.Value
	if text == "" {
		text = entry.Node.Label
	}
	if text == "" {
		return nil
	}
	err := utils.CopyToClipboard(text)
	if err != nil {
		m.inspectorCopyMsg = "Copy failed"
	} else {
		m.inspectorCopyMsg = "Copied!"
	}
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg {
		return clearCopyFeedbackMsg{}
	})
}

// inspectorOpenValue opens the value as a URL in the browser.
func (m *Model) inspectorOpenValue() {
	if m.inspectorCursor >= len(m.inspectorFlat) {
		return
	}
	entry := m.inspectorFlat[m.inspectorCursor]
	val := entry.Node.Value
	if entry.Node.IsURL || IsURLValue(val) {
		_ = utils.OpenBrowser(val)
	}
}

// inspectorNavigateIntoChild traverses into a child TreeItem, saving current state.
func (m *Model) inspectorNavigateIntoChild(childItem *TreeItem) {
	m.inspectorBreadcrumb = append(m.inspectorBreadcrumb, inspectorBreadcrumbEntry{
		item:       m.modalItem,
		sidebarIdx: m.inspectorSidebarIdx,
		cursor:     m.inspectorCursor,
		scroll:     m.modalScroll,
		focusLeft:  m.inspectorFocusLeft,
	})
	m.modalItem = childItem
	m.inspectorNodes = BuildInspectorTree(m.modalItem)
	m.inspectorSidebarIdx = 0
	m.inspectorCursor = 0
	m.modalScroll = 0
	m.inspectorFocusLeft = true
	m.rebuildInspectorFlat()
}

// inspectorNavigateBack goes back to the parent, restoring saved state.
func (m *Model) inspectorNavigateBack() bool {
	if len(m.inspectorBreadcrumb) == 0 {
		return false
	}
	last := len(m.inspectorBreadcrumb) - 1
	entry := m.inspectorBreadcrumb[last]
	m.inspectorBreadcrumb = m.inspectorBreadcrumb[:last]
	m.modalItem = entry.item
	m.inspectorNodes = BuildInspectorTree(m.modalItem)
	m.inspectorSidebarIdx = entry.sidebarIdx
	m.rebuildInspectorFlat()
	m.inspectorCursor = entry.cursor
	m.modalScroll = entry.scroll
	m.inspectorFocusLeft = entry.focusLeft
	return true
}

// resetInspectorModal clears all inspector state.
func (m *Model) resetInspectorModal() {
	m.showDetailModal = false
	m.modalItem = nil
	m.modalScroll = 0
	m.inspectorNodes = nil
	m.inspectorFlat = nil
	m.inspectorCursor = 0
	m.inspectorSidebarIdx = 0
	m.inspectorFocusLeft = false
	m.inspectorSearching = false
	m.inspectorSearchQuery = ""
	m.inspectorSearchMatches = nil
	m.inspectorSearchIdx = -1
	m.inspectorBreadcrumb = nil
	m.inspectorCopyMsg = ""
}

// channelReporter implements LoadingReporter and sends updates via a channel
type channelReporter struct {
	ch chan<- LoadingProgressMsg
}

func (r *channelReporter) SetPhase(phase string) {
	select {
	case r.ch <- LoadingProgressMsg{Phase: phase}:
	default:
	}
}

func (r *channelReporter) SetDetail(detail string) {
	select {
	case r.ch <- LoadingProgressMsg{Detail: detail}:
	default:
	}
}

func (r *channelReporter) SetURL(url string) {
	select {
	case r.ch <- LoadingProgressMsg{URL: url}:
	default:
	}
}

// doReload returns a command that performs the reload with progress updates
func (m *Model) doReload() tea.Cmd {
	// Store channels in model for listenForProgress to access
	m.progressCh = make(chan LoadingProgressMsg, 10)
	m.resultCh = make(chan ReloadResultMsg, 1)

	reporter := &channelReporter{ch: m.progressCh}
	progressCh := m.progressCh
	resultCh := m.resultCh

	// Start the reload in a goroutine
	go func() {
		defer close(progressCh)
		spans, start, end, err := m.reloadFunc(reporter)
		resultCh <- ReloadResultMsg{
			spans:       spans,
			globalStart: start,
			globalEnd:   end,
			err:         err,
		}
	}()

	// Return a command that listens for progress and result
	return m.listenForProgress()
}

// listenForProgress returns a command that listens for progress updates or results
func (m *Model) listenForProgress() tea.Cmd {
	progressCh := m.progressCh
	resultCh := m.resultCh

	if progressCh == nil || resultCh == nil {
		return nil
	}

	return func() tea.Msg {
		select {
		case progress, ok := <-progressCh:
			if ok {
				return progress
			}
			// Channel closed, wait for result
			return <-resultCh
		case result := <-resultCh:
			return result
		}
	}
}

// expandAll expands all items
func (m *Model) expandAll() {
	var expandNodes func(nodes []*TreeItem)
	expandNodes = func(nodes []*TreeItem) {
		for _, item := range nodes {
			if item.HasChildren {
				m.expandedState[item.ID] = true
			}
			expandNodes(item.Children)
		}
	}
	expandNodes(m.treeItems)
	m.rebuildItems()
}

// collapseAll collapses all items
func (m *Model) collapseAll() {
	for id := range m.expandedState {
		m.expandedState[id] = false
	}
	m.rebuildItems()
}

// toggleExpandAll expands all if any are collapsed, or collapses all if all are expanded
func (m *Model) toggleExpandAll() {
	anyCollapsed := false
	var check func(nodes []*TreeItem)
	check = func(nodes []*TreeItem) {
		for _, item := range nodes {
			if item.HasChildren && !m.expandedState[item.ID] {
				anyCollapsed = true
				return
			}
			check(item.Children)
		}
	}
	check(m.treeItems)

	if anyCollapsed {
		m.expandAll()
	} else {
		m.collapseAll()
	}
}

// hideActivityGroups hides Activity groups (not artifact groups) from the chart by default
func (m *Model) hideActivityGroups() {
	var walk func(items []*TreeItem)
	walk = func(items []*TreeItem) {
		for _, item := range items {
			if item.ItemType == ItemTypeActivityGroup && item.Hints.Category != "artifact" {
				m.hiddenState[item.ID] = true
				m.toggleDescendants(item.Children, true)
			}
			walk(item.Children)
		}
	}
	walk(m.treeItems)
}

// toggleFocus focuses on the current selection, hiding everything else
func (m *Model) toggleFocus() {
	if m.isFocused {
		// Unfocus: restore the previous hidden state
		m.hiddenState = m.preFocusHiddenState
		m.preFocusHiddenState = nil
		m.focusedIDs = nil
		m.isFocused = false
	} else {
		// Focus: save current hidden state and hide everything except selection
		m.preFocusHiddenState = make(map[string]bool)
		for k, v := range m.hiddenState {
			m.preFocusHiddenState[k] = v
		}

		// Get selected items and their descendants (but not ancestors)
		start, end := m.getSelectionRange()
		selectedIDs := make(map[string]bool)
		for i := start; i <= end && i < len(m.visibleItems); i++ {
			item := m.visibleItems[i]
			selectedIDs[item.ID] = true
			// Include all descendants (children, grandchildren, etc.)
			m.collectDescendantIDs(item.ID, selectedIDs)
		}

		// Track focused IDs for dimming (items not in this set get grayed out)
		m.focusedIDs = selectedIDs

		// Hide non-focused items from the timeline chart
		var hideAll func(items []*TreeItem)
		hideAll = func(items []*TreeItem) {
			for _, item := range items {
				if !selectedIDs[item.ID] {
					m.hiddenState[item.ID] = true
				} else {
					m.hiddenState[item.ID] = false
				}
				hideAll(item.Children)
			}
		}
		hideAll(m.treeItems)
		m.isFocused = true
	}
	m.recalculateEffectiveTimes()
	m.rebuildVisibleItems()
	m.recalculateChartBounds()
}

// collectAncestorIDs adds all ancestor IDs to the set
func (m *Model) collectAncestorIDs(parentID string, ids map[string]bool) {
	if parentID == "" {
		return
	}
	ids[parentID] = true
	// Find the parent item to get its parent
	for _, item := range m.visibleItems {
		if item.ID == parentID {
			m.collectAncestorIDs(item.ParentID, ids)
			return
		}
	}
}

// collectDescendantIDs adds all descendant IDs to the set
func (m *Model) collectDescendantIDs(parentID string, ids map[string]bool) {
	var collect func(items []*TreeItem)
	collect = func(items []*TreeItem) {
		for _, item := range items {
			if item.ParentID == parentID || ids[item.ParentID] {
				ids[item.ID] = true
			}
			collect(item.Children)
		}
	}
	collect(m.treeItems)
}

// getSelectionRange returns the start and end indices of the current selection
func (m *Model) getSelectionRange() (start, end int) {
	if m.selectionStart == -1 {
		return m.cursor, m.cursor
	}
	if m.selectionStart < m.cursor {
		return m.selectionStart, m.cursor
	}
	return m.cursor, m.selectionStart
}

// isInSelection returns true if the given index is within the current selection
func (m *Model) isInSelection(idx int) bool {
	start, end := m.getSelectionRange()
	return idx >= start && idx <= end
}

// toggleChartVisibility toggles visibility for all items in the selection range
func (m *Model) toggleChartVisibility() {
	start, end := m.getSelectionRange()
	if start >= len(m.visibleItems) {
		return
	}
	if end >= len(m.visibleItems) {
		end = len(m.visibleItems) - 1
	}

	// Determine target state from first item in selection
	firstItem := m.visibleItems[start]
	targetHidden := !m.hiddenState[firstItem.ID]

	// Toggle all items in selection range
	for i := start; i <= end; i++ {
		item := m.visibleItems[i]
		m.hiddenState[item.ID] = targetHidden
		// Also toggle all children of this item
		m.toggleChildrenVisibility(item.ID, targetHidden)
	}

	// Recalculate parent time bounds from active children, then chart bounds
	m.recalculateEffectiveTimes()
	m.recalculateChartBounds()
	m.rebuildVisibleItems()
}

// toggleChildrenVisibility recursively sets visibility for all descendants in the tree
func (m *Model) toggleChildrenVisibility(parentID string, hidden bool) {
	// Find the item in the tree and toggle all its descendants
	var findAndToggle func(items []*TreeItem) bool
	findAndToggle = func(items []*TreeItem) bool {
		for _, item := range items {
			if item.ID == parentID {
				// Found the parent, toggle all its children
				m.toggleDescendants(item.Children, hidden)
				return true
			}
			if findAndToggle(item.Children) {
				return true
			}
		}
		return false
	}
	findAndToggle(m.treeItems)
}

// toggleDescendants recursively sets visibility for items and all their descendants
func (m *Model) toggleDescendants(items []*TreeItem, hidden bool) {
	for _, item := range items {
		m.hiddenState[item.ID] = hidden
		m.toggleDescendants(item.Children, hidden)
	}
}

// recalculateEffectiveTimes walks the tree bottom-up and recalculates each parent's
// time bounds from its active (non-hidden) children. Leaf items and items with no
// children restore their original span times. This ensures inactive nodes do not
// contribute to ancestor durations.
func (m *Model) recalculateEffectiveTimes() {
	var walk func(items []*TreeItem)
	walk = func(items []*TreeItem) {
		for _, item := range items {
			// Recurse first (bottom-up)
			walk(item.Children)

			// Restore original times
			if orig, ok := m.origTimes[item.ID]; ok {
				item.StartTime = orig[0]
				item.EndTime = orig[1]
			}

			// Items with children derive effective times from active children
			if len(item.Children) == 0 {
				continue
			}
			var earliest, latest time.Time
			for _, child := range item.Children {
				// Skip hidden children and info/marker items
				if m.hiddenState[child.ID] {
					continue
				}
				if child.ItemType == ItemTypeInfo {
					continue
				}
				if !child.StartTime.IsZero() && (earliest.IsZero() || child.StartTime.Before(earliest)) {
					earliest = child.StartTime
				}
				if !child.EndTime.IsZero() && (latest.IsZero() || child.EndTime.After(latest)) {
					latest = child.EndTime
				}
			}
			// If at least one active child, use derived bounds
			if !earliest.IsZero() {
				item.StartTime = earliest
			}
			if !latest.IsZero() {
				item.EndTime = latest
			}
			// If ALL children are hidden, zero out times (no bar)
			hasActiveChild := false
			for _, child := range item.Children {
				if !m.hiddenState[child.ID] && child.ItemType != ItemTypeInfo {
					hasActiveChild = true
					break
				}
			}
			if !hasActiveChild {
				item.StartTime = time.Time{}
				item.EndTime = time.Time{}
			}
		}
	}
	walk(m.treeItems)
}

// recalculateChartBounds recalculates the chart time window and stats based on visible items
func (m *Model) recalculateChartBounds() {
	var earliest, latest time.Time
	var totalRuns, successfulRuns int
	var totalJobs, failedJobs int
	var stepCount int
	var computeMs int64
	workflowsSeen := make(map[string]bool)

	var checkItems func(items []*TreeItem)
	checkItems = func(items []*TreeItem) {
		for _, item := range items {
			// Always recurse into children first (they may be visible even if parent is hidden)
			checkItems(item.Children)

			// Skip hidden items for bounds/stats calculation
			if m.hiddenState[item.ID] {
				continue
			}

			// Time bounds
			if !item.StartTime.IsZero() {
				if earliest.IsZero() || item.StartTime.Before(earliest) {
					earliest = item.StartTime
				}
			}
			if !item.EndTime.IsZero() {
				if latest.IsZero() || item.EndTime.After(latest) {
					latest = item.EndTime
				}
			}

			// Stats by item type
			switch item.ItemType {
			case ItemTypeRoot:
				if !workflowsSeen[item.Name] {
					workflowsSeen[item.Name] = true
					totalRuns++
					if item.Hints.Outcome == "success" {
						successfulRuns++
					}
				}
			case ItemTypeIntermediate:
				// Only count direct jobs (category: job/task) for stats and compute,
				// not nested intermediate spans from embedded traces (e.g. Bazel).
				cat := item.Hints.Category
				if cat == "job" || cat == "task" {
					totalJobs++
					if item.Hints.Outcome == "failure" {
						failedJobs++
					}
					// Compute time uses original span durations (not effective times
					// which may be narrowed by hidden children)
					if orig, ok := m.origTimes[item.ID]; ok {
						if !orig[0].IsZero() && !orig[1].IsZero() {
							duration := orig[1].Sub(orig[0]).Milliseconds()
							if duration > 0 {
								computeMs += duration
							}
						}
					} else if !item.StartTime.IsZero() && !item.EndTime.IsZero() {
						duration := item.EndTime.Sub(item.StartTime).Milliseconds()
						if duration > 0 {
							computeMs += duration
						}
					}
				}
			case ItemTypeLeaf:
				stepCount++
			}
		}
	}
	checkItems(m.treeItems)

	// If all items are hidden, use global bounds and full stats
	if earliest.IsZero() {
		earliest = m.globalStart
		m.displayedSummary = m.summary
		m.displayedWallTimeMs = m.wallTimeMs
		m.displayedComputeMs = m.computeMs
		m.displayedStepCount = m.stepCount
	} else {
		// Update displayed stats (preserve enrichment metrics from full summary)
		m.displayedSummary = analyzer.Summary{
			TotalRuns:      totalRuns,
			SuccessfulRuns: successfulRuns,
			TotalJobs:      totalJobs,
			FailedJobs:     failedJobs,
			MaxConcurrency: m.summary.MaxConcurrency,
			AvgQueueTimeMs: m.summary.AvgQueueTimeMs,
			MaxQueueTimeMs: m.summary.MaxQueueTimeMs,
			QueueCount:     m.summary.QueueCount,
			RetriedRuns:    m.summary.RetriedRuns,
			BillableMs:     m.summary.BillableMs,
		}
		m.displayedStepCount = stepCount
		m.displayedComputeMs = computeMs
	}
	if latest.IsZero() {
		latest = m.globalEnd
	}

	m.chartStart = earliest
	m.chartEnd = latest

	// Wall time is chart duration
	m.displayedWallTimeMs = latest.Sub(earliest).Milliseconds()
	if m.displayedWallTimeMs < 0 {
		m.displayedWallTimeMs = 0
	}
}

// hasEnrichmentLine returns true if the header should show the queue/retry/billable line.
func (m Model) hasEnrichmentLine() bool {
	if m.displayedSummary.QueueCount > 0 {
		return true
	}
	if m.displayedSummary.RetriedRuns > 0 {
		return true
	}
	for _, ms := range m.displayedSummary.BillableMs {
		if ms > 0 {
			return true
		}
	}
	return false
}

// IsHidden returns whether an item is hidden from the chart
func (m *Model) IsHidden(id string) bool {
	return m.hiddenState[id]
}

// isActivityHidden returns true if any activity group is hidden.
func (m *Model) isActivityHidden() bool {
	var walk func([]*TreeItem) bool
	walk = func(items []*TreeItem) bool {
		for _, item := range items {
			if item.ItemType == ItemTypeActivityGroup && item.Hints.Category != "artifact" && m.hiddenState[item.ID] {
				return true
			}
			if walk(item.Children) {
				return true
			}
		}
		return false
	}
	return walk(m.treeItems)
}

// visibleSpans returns the subset of spans not hidden in the TUI.
func (m *Model) visibleSpans() []trace.ReadOnlySpan {
	// Collect OTel span IDs of all hidden tree items
	hiddenSpanIDs := make(map[string]bool)
	var walk func([]*TreeItem)
	walk = func(items []*TreeItem) {
		for _, item := range items {
			if m.hiddenState[item.ID] && item.SpanID != "" {
				hiddenSpanIDs[item.SpanID] = true
			}
			walk(item.Children)
		}
	}
	walk(m.treeItems)

	if len(hiddenSpanIDs) == 0 {
		return m.spans
	}

	filtered := make([]trace.ReadOnlySpan, 0, len(m.spans))
	for _, s := range m.spans {
		if !hiddenSpanIDs[s.SpanContext().SpanID().String()] {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// Run starts the TUI
func Run(spans []trace.ReadOnlySpan, globalStart, globalEnd time.Time, inputURLs []string, reloadFunc ReloadFunc, openPerfettoFunc OpenPerfettoFunc, enricher enrichment.Enricher, opts ...ModelOption) error {
	m := NewModel(spans, globalStart, globalEnd, inputURLs, reloadFunc, openPerfettoFunc, enricher, opts...)
	// Mouse mode disabled by default to allow OSC 8 hyperlinks to work
	// Press 'm' to toggle mouse mode for scrolling
	p := tea.NewProgram(m, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("tea.Program.Run failed: %w", err)
	}
	_ = finalModel
	return nil
}

