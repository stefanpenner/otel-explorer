package results

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"go.opentelemetry.io/otel/sdk/trace"
)

// logFetchState tracks inline progress for a log fetch on a specific item.
type logFetchState struct {
	itemID string // ID of the TreeItem being fetched
	phase  string // e.g. "Fetching logs..."
}

// fetchLogsForCurrentItem initiates an async log fetch for the currently selected
// job or step item. Returns a tea.Cmd if a fetch was started, nil otherwise.
func (m *Model) fetchLogsForCurrentItem() tea.Cmd {
	if m.logFetchFunc == nil || m.logFetchingJobID != 0 {
		return nil
	}

	if m.cursor < 0 || m.cursor >= len(m.visibleItems) {
		return nil
	}

	item := &m.visibleItems[m.cursor]
	if item.SpanID == "" {
		return nil
	}

	// Find the job ID by looking up spans with typed attributes.
	// Walk from the selected item up to find a job span with github.job_id.
	jobID, owner, repo, jobSpanID := m.resolveLogFetchParams(item)
	if jobID == 0 || owner == "" || repo == "" {
		return nil
	}

	// Skip if this job was already fetched
	if m.logFetchedJobIDs[jobID] {
		return nil
	}

	// Find the job TreeItem to attach the spinner to the job node
	spinnerItemID := item.ID
	for _, vi := range m.visibleItems {
		if vi.SpanID == jobSpanID {
			spinnerItemID = vi.ID
			break
		}
	}

	m.logFetchingJobID = jobID
	m.logFetchInline = &logFetchState{
		itemID: spinnerItemID,
		phase:  "Fetching logs...",
	}

	fetchFunc := m.logFetchFunc
	spans := m.spans

	return func() tea.Msg {
		newSpans, err := fetchFunc(owner, repo, jobID, spans)
		return LogFetchResultMsg{newSpans: newSpans, err: err}
	}
}

// resolveLogFetchParams finds the job ID, owner, repo, and the job span ID
// for a log fetch by looking up actual span attributes.
func (m *Model) resolveLogFetchParams(item *TreeItem) (jobID int64, owner, repo, jobSpanID string) {
	// Build a SpanID -> ReadOnlySpan index
	spansByID := make(map[string]trace.ReadOnlySpan, len(m.spans))
	for _, s := range m.spans {
		spansByID[s.SpanContext().SpanID().String()] = s
	}

	// Walk from this item's span upward through parents to find github.job_id
	currentSpanID := item.SpanID
	for currentSpanID != "" {
		s, ok := spansByID[currentSpanID]
		if !ok {
			break
		}
		for _, a := range s.Attributes() {
			if string(a.Key) == "github.job_id" {
				jobID = a.Value.AsInt64()
				jobSpanID = currentSpanID
				break
			}
		}
		if jobID != 0 {
			break
		}
		// Walk to parent span
		parentID := s.Parent().SpanID()
		if !parentID.IsValid() {
			break
		}
		currentSpanID = parentID.String()
	}

	// Extract owner/repo from root span vcs.repository.url.full
	for _, root := range m.roots {
		if u, ok := root.Attrs["vcs.repository.url.full"]; ok && u != "" {
			parts := strings.Split(strings.TrimSuffix(u, "/"), "/")
			if len(parts) >= 2 {
				owner = parts[len(parts)-2]
				repo = parts[len(parts)-1]
				break
			}
		}
	}

	// Fallback: try to find repo URL from span attributes directly
	if owner == "" || repo == "" {
		for _, s := range m.spans {
			for _, a := range s.Attributes() {
				if string(a.Key) == "vcs.repository.url.full" {
					u := a.Value.AsString()
					parts := strings.Split(strings.TrimSuffix(u, "/"), "/")
					if len(parts) >= 2 {
						owner = parts[len(parts)-2]
						repo = parts[len(parts)-1]
						return
					}
				}
			}
		}
	}

	return
}

// logFetchPhase returns the loading phase text for an item, or "" if not fetching.
func (m *Model) logFetchPhase(itemID string) string {
	if m.logFetchInline != nil && m.logFetchInline.itemID == itemID {
		return m.logFetchInline.phase
	}
	return ""
}

