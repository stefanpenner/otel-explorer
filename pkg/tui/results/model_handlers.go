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

	// Handle detail modal
	if m.showDetailModal {
		// Inspector search input mode
		if m.inspectorSearching {
			switch msg.Type {
			case tea.KeyCtrlC:
				// Quit must work even while typing a query.
				return m, tea.Quit
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
					_, size := utf8.DecodeLastRuneInString(m.inspectorSearchQuery)
					m.inspectorSearchQuery = m.inspectorSearchQuery[:len(m.inspectorSearchQuery)-size]
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
			if m.reloadFunc != nil && !m.isLoading {
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
