package results

import (
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

// handleReloadResult applies a fresh span set from a reload. Resets all
// view state (cursor, expansion, focus, log-fetch tracking) so the new
// data starts from a clean slate.
func (m Model) handleReloadResult(msg ReloadResultMsg) (tea.Model, tea.Cmd) {
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
	// Reset log fetch state so previously fetched jobs can be re-fetched
	// against the fresh data, and any in-flight fetch result is ignored
	// (results are tagged with the generation they were started under).
	m.reloadGen++
	m.logFetchedJobIDs = nil
	m.logFetchingJobID = 0
	m.logFetchInline = nil
	// Update model with new data
	m.spans = msg.spans
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
	m.isFocused = false
	m.focusedIDs = nil
	m.preFocusHiddenState = nil
	return m, nil
}

// handleKeyMsg dispatches keyboard input. It routes keys to the active
// modal (help, detail/inspector), the search-input box, or the main list.
func (m Model) handleKeyMsg(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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

	// Detail modal: search the inspector, or move inside it.
	if m.showDetailModal {
		if m.inspectorSearching {
			return m.handleInspectorSearchKey(msg)
		}
		return m.handleInspectorKey(msg)
	}

	// Handle search input mode
	if m.isSearching {
		switch msg.Type {
		case tea.KeyCtrlC:
			// Quit must work even while typing a query.
			return m, tea.Quit
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
			// Only the filter changed; skip the full tree rebuild.
			m.rebuildVisibleItems()
			return m, nil
		default:
			if msg.Type == tea.KeyRunes {
				m.searchQuery += string(msg.Runes)
				m.applySearchFilter()
				// Only the filter changed; skip the full tree rebuild.
				m.rebuildVisibleItems()
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

	return m, nil
}

// handleInspectorSearchKey edits the inspector query.
// Ctrl+C still quits.
func (m Model) handleInspectorSearchKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.cancelInspectorSearch()
	case tea.KeyEnter:
		m.commitInspectorSearch()
	case tea.KeyBackspace:
		m.popInspectorSearch()
	default:
		if msg.Type == tea.KeyRunes {
			m.typeInspectorSearch(msg.Runes)
		}
	}
	return m, nil
}

// cancelInspectorSearch leaves search and drops the query.
func (m *Model) cancelInspectorSearch() {
	m.inspectorSearching = false
	m.clearInspectorSearch()
}

// commitInspectorSearch leaves search and jumps to the first match.
func (m *Model) commitInspectorSearch() {
	m.inspectorSearching = false
	if len(m.inspectorSearchMatches) == 0 {
		return
	}
	m.inspectorSearchIdx = 0
	m.inspectorJumpToMatch()
}

// popInspectorSearch deletes the last rune and refreshes matches.
func (m *Model) popInspectorSearch() {
	if len(m.inspectorSearchQuery) == 0 {
		return
	}
	_, size := utf8.DecodeLastRuneInString(m.inspectorSearchQuery)
	m.inspectorSearchQuery = m.inspectorSearchQuery[:len(m.inspectorSearchQuery)-size]
	m.updateInspectorSearch()
}

// typeInspectorSearch appends runes and jumps to the first match.
func (m *Model) typeInspectorSearch(runes []rune) {
	m.inspectorSearchQuery += string(runes)
	m.updateInspectorSearch()
	if len(m.inspectorSearchMatches) == 0 {
		return
	}
	m.inspectorSearchIdx = 0
	m.inspectorJumpToMatch()
}

// beginInspectorSearch starts an empty inspector query.
func (m *Model) beginInspectorSearch() {
	m.inspectorSearching = true
	m.clearInspectorSearch()
}

// clearInspectorSearch drops the query and its matches.
func (m *Model) clearInspectorSearch() {
	m.inspectorSearchQuery = ""
	m.inspectorSearchMatches = nil
	m.inspectorSearchIdx = -1
}

// handleInspectorKey moves inside the detail inspector.
func (m Model) handleInspectorKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.dismissInspector()
	case "i", "q":
		m.resetInspectorModal()
	case "tab":
		m.inspectorFocusLeft = !m.inspectorFocusLeft
	case "up", "k":
		m.moveInspectorUp()
	case "down", "j":
		m.moveInspectorDown()
	case "left", "h":
		m.collapseInspectorOrParent()
	case "right", "l":
		m.expandInspectorOrEnter()
	case " ", "enter":
		m.activateInspector()
	case "]":
		m.showNextInspectorItem()
	case "[":
		m.showPrevInspectorItem()
	case "/":
		m.beginInspectorSearch()
	case "n":
		m.nextInspectorMatch()
	case "N":
		m.prevInspectorMatch()
	case "c":
		cmd := m.inspectorCopyValue()
		return m, cmd
	case "o":
		m.inspectorOpenValue()
	case "backspace":
		m.inspectorNavigateBack()
	case "r":
		cmd := m.reloadFromInspector()
		return m, cmd
	case "p":
		if m.openPerfettoFunc != nil {
			m.openPerfettoFunc(m.visibleSpans(), m.isActivityHidden())
		}
	case "g":
		m.jumpInspectorHome()
	case "G":
		m.jumpInspectorEnd()
	}
	return m, nil
}

// dismissInspector clears a query, goes back, or closes the inspector.
func (m *Model) dismissInspector() {
	if m.inspectorSearchQuery != "" {
		m.clearInspectorSearch()
		return
	}
	if m.inspectorNavigateBack() {
		return
	}
	m.resetInspectorModal()
}

// moveInspectorUp steps the sidebar, or the tree, one row toward the start.
func (m *Model) moveInspectorUp() {
	if m.inspectorFocusLeft {
		if m.inspectorSidebarIdx > 0 {
			m.inspectorSidebarIdx--
			m.rebuildInspectorFlat()
		}
		return
	}
	if m.inspectorCursor > 0 {
		m.inspectorCursor--
	}
}

// moveInspectorDown steps the sidebar, or the tree, one row toward the end.
func (m *Model) moveInspectorDown() {
	if m.inspectorFocusLeft {
		if m.inspectorSidebarIdx < len(m.inspectorNodes)-1 {
			m.inspectorSidebarIdx++
			m.rebuildInspectorFlat()
		}
		return
	}
	if m.inspectorCursor < len(m.inspectorFlat)-1 {
		m.inspectorCursor++
	}
}

// collapseInspectorOrParent collapses the node, moves to its parent, or focuses the sidebar.
func (m *Model) collapseInspectorOrParent() {
	if m.inspectorFocusLeft || m.inspectorCursor >= len(m.inspectorFlat) {
		return
	}

	entry := m.inspectorFlat[m.inspectorCursor]
	if entry.Node.Expanded && len(entry.Node.Children) > 0 {
		entry.Node.Expanded = false
		m.rebuildInspectorFlat()
		return
	}

	parentIdx := FindParentIndex(m.inspectorFlat, m.inspectorCursor)
	if parentIdx >= 0 {
		m.inspectorCursor = parentIdx
		return
	}
	m.inspectorFocusLeft = true
}

// expandInspectorOrEnter enters the tree, or expands a collapsed node.
func (m *Model) expandInspectorOrEnter() {
	if m.inspectorFocusLeft {
		m.inspectorFocusLeft = false
		return
	}
	if m.inspectorCursor >= len(m.inspectorFlat) {
		return
	}

	entry := m.inspectorFlat[m.inspectorCursor]
	if !entry.Node.Expanded && len(entry.Node.Children) > 0 {
		entry.Node.Expanded = true
		m.rebuildInspectorFlat()
	}
}

// activateInspector enters the tree, opens a child span, or toggles a node.
func (m *Model) activateInspector() {
	if m.inspectorFocusLeft {
		m.inspectorFocusLeft = false
		m.inspectorCursor = 0
		m.modalScroll = 0
		return
	}
	if m.inspectorCursor >= len(m.inspectorFlat) {
		return
	}

	entry := m.inspectorFlat[m.inspectorCursor]
	if entry.Node.ChildItem != nil {
		m.inspectorNavigateIntoChild(entry.Node.ChildItem)
		return
	}
	if len(entry.Node.Children) > 0 {
		entry.Node.Expanded = !entry.Node.Expanded
		m.rebuildInspectorFlat()
	}
}

// showNextInspectorItem opens the next main-tree row in the inspector.
func (m *Model) showNextInspectorItem() {
	if m.cursor >= len(m.visibleItems)-1 {
		return
	}
	m.cursor++
	m.showInspectorItem()
}

// showPrevInspectorItem opens the previous main-tree row in the inspector.
func (m *Model) showPrevInspectorItem() {
	if m.cursor <= 0 {
		return
	}
	m.cursor--
	m.showInspectorItem()
}

// showInspectorItem loads the inspector for the current main-tree row.
func (m *Model) showInspectorItem() {
	m.modalScroll = 0
	item := m.visibleItems[m.cursor]
	m.modalItem = &item
	m.inspectorNodes = BuildInspectorTree(m.modalItem)
	m.inspectorSidebarIdx = 0
	m.rebuildInspectorFlat()
	m.inspectorCursor = 0
	m.inspectorBreadcrumb = nil
}

// nextInspectorMatch jumps to the following query match, wrapping at the end.
func (m *Model) nextInspectorMatch() {
	n := len(m.inspectorSearchMatches)
	if n == 0 {
		return
	}
	m.inspectorSearchIdx = (m.inspectorSearchIdx + 1) % n
	m.inspectorJumpToMatch()
}

// prevInspectorMatch jumps to the previous query match, wrapping at the start.
func (m *Model) prevInspectorMatch() {
	n := len(m.inspectorSearchMatches)
	if n == 0 {
		return
	}
	m.inspectorSearchIdx--
	if m.inspectorSearchIdx < 0 {
		m.inspectorSearchIdx = n - 1
	}
	m.inspectorJumpToMatch()
}

// reloadFromInspector closes the inspector and starts a reload when one is wired.
func (m *Model) reloadFromInspector() tea.Cmd {
	m.resetInspectorModal()
	if m.reloadFunc == nil || m.isLoading {
		return nil
	}
	m.isLoading = true
	return tea.Batch(m.spinner.Tick, m.doReload())
}

// jumpInspectorHome moves to the first sidebar section, or the first tree row.
func (m *Model) jumpInspectorHome() {
	if m.inspectorFocusLeft {
		m.inspectorSidebarIdx = 0
		m.rebuildInspectorFlat()
		return
	}
	m.inspectorCursor = 0
}

// jumpInspectorEnd moves to the last sidebar section, or the last tree row.
func (m *Model) jumpInspectorEnd() {
	if m.inspectorFocusLeft {
		m.inspectorSidebarIdx = len(m.inspectorNodes) - 1
		m.rebuildInspectorFlat()
		return
	}
	if len(m.inspectorFlat) > 0 {
		m.inspectorCursor = len(m.inspectorFlat) - 1
	}
}

// handleMouseMsg dispatches mouse input (wheel scroll, click selection).
func (m Model) handleMouseMsg(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
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
			// Clamp here (where the mutation persists) so overscroll
			// ticks don't accumulate past the end of the content.
			if m.modalScroll < m.modalMaxScroll() {
				m.modalScroll++
			}
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
			// Map the clicked row to an item using the same layout math
			// as View(). msg.Y is zero-based; items start right after
			// the header lines.
			clickedRow := msg.Y - m.headerLineCount()
			availableHeight := m.contentHeight()
			if clickedRow >= 0 && clickedRow < availableHeight {
				itemIdx := m.scrollWindowStart(availableHeight) + clickedRow
				if itemIdx < len(m.visibleItems) {
					m.selectionStart = -1
					m.cursor = itemIdx
				}
			}
		}
	}

	return m, nil
}
