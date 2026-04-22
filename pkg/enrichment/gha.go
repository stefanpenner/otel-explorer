package enrichment

// GHAEnricher detects GitHub Actions span attributes and produces hints
// matching the current hardcoded behavior.
type GHAEnricher struct{}

// Enrich produces SpanHints for GHA spans (type ∈ {workflow, job, step, marker}).
// Returns empty hints (Category=="") if the span is not a GHA span.
func (e *GHAEnricher) Enrich(name string, attrs map[string]string, isZeroDuration bool) SpanHints {
	spanType := attrs["type"]
	if spanType != "workflow" && spanType != "job" && spanType != "step" && spanType != "marker" && spanType != "log_span" &&
		spanType != "runner.queue" && spanType != "runner.startup" && spanType != "runner.execution" {
		return SpanHints{}
	}

	h := SpanHints{
		Category:   spanType,
		URL:        attrs["github.url"],
		User:       attrs["github.user"],
		EventType:  attrs["github.event_type"],
		IsRequired: attrs["github.is_required"] == "true",
	}

	// Outcome from conclusion/status
	conclusion := attrs["github.conclusion"]
	status := attrs["github.status"]
	switch {
	case status == "in_progress" || status == "queued" || status == "waiting":
		h.Outcome = "pending"
		h.Color = "blue"
	case conclusion == "success":
		h.Outcome = "success"
		h.Color = "green"
	case conclusion == "failure":
		h.Outcome = "failure"
		h.Color = "red"
	case conclusion == "skipped" || conclusion == "cancelled":
		h.Outcome = "skipped"
		h.Color = "gray"
	default:
		h.Color = "gray"
	}

	switch spanType {
	case "workflow":
		h.IsRoot = true
		h.Icon = "📋"
		h.BarChar = "█"
	case "job":
		h.Icon = "⚙️"
		h.BarChar = "█"
	case "step":
		h.IsLeaf = true
		h.Icon = "↳"
		h.BarChar = "▒"
	case "log_span":
		h.IsLeaf = true
		h.Icon = "░"
		h.BarChar = "░"
		// Log spans are derived from completed step logs; inherit success
		// appearance so tree connectors and bars match the parent step.
		if h.Color == "gray" && conclusion == "" {
			h.Color = "green"
			h.Outcome = "success"
		}
	case "marker":
		h.IsMarker = true
		h.SortPriority = -1
		h.GroupKey = "activity"
		h.enrichMarker(attrs)
	case "runner.queue":
		h.IsLeaf = true
		h.Icon = "⏳"
		h.BarChar = "░"
		h.Color = "yellow"
		h.Outcome = "pending"
	case "runner.startup":
		h.IsLeaf = true
		h.Icon = "🔄"
		h.BarChar = "░"
		h.Color = "blue"
		h.Outcome = "pending"
	case "runner.execution":
		h.IsLeaf = true
		h.Icon = "🏃"
		h.BarChar = "▒"
	}

	return h
}

// enrichMarker fills in marker-specific hints.
func (h *SpanHints) enrichMarker(attrs map[string]string) {
	eventType := attrs["github.event_type"]

	switch eventType {
	case "merged":
		h.Icon = "◆ "
		h.BarChar = "◆"
		h.Color = "green"
		h.Outcome = "success"
	case "approved":
		h.Icon = "▲ "
		h.BarChar = "✓"
		h.Color = "green"
		h.Outcome = "success"
	case "comment", "commented", "COMMENTED":
		h.Icon = "● "
		h.BarChar = "●"
		h.Color = "blue"
		h.Outcome = "pending"
	case "changes_requested":
		h.Icon = "❌"
		h.BarChar = "✗"
		h.Color = "red"
		h.Outcome = "failure"
	default:
		h.Icon = "▲ "
		h.BarChar = "▲"
		h.Color = "blue"
		h.Outcome = "pending"
	}

	// Build dedup key from multiple attributes
	eventID := attrs["github.event_id"]
	eventTime := attrs["github.event_time"]
	user := attrs["github.user"]
	key := eventType + "-" + user + "-" + eventTime
	if eventID != "" {
		key = eventID + "-" + key
	}
	h.DedupKey = key
}
