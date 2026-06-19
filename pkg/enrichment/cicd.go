package enrichment

// CICDEnricher recognizes OTel CI/CD semantic conventions (v1.27+).
// It matches spans with cicd.pipeline.* attributes from any CI system
// (Jenkins, GitLab CI, Dagger, Buildkite, Gradle, etc.).
type CICDEnricher struct{}

// Enrich produces SpanHints for CI/CD spans using OTel semantic conventions.
// Returns empty hints (Category=="") if no cicd.pipeline.* attributes are found.
func (e *CICDEnricher) Enrich(name string, attrs map[string]string, isZeroDuration bool) SpanHints {
	pipelineName := attrs["cicd.pipeline.name"]
	taskName := attrs["cicd.pipeline.task.name"]
	taskType := attrs["cicd.pipeline.task.type"]

	if pipelineName == "" && taskName == "" {
		return SpanHints{}
	}

	h := SpanHints{
		BarChar: "█",
	}

	// URL from CI/CD task URL or VCS attributes
	if url := attrs["cicd.pipeline.task.run.url.full"]; url != "" {
		h.URL = url
	} else if url := attrs["cicd.pipeline.run.url.full"]; url != "" {
		h.URL = url
	} else if url := attrs["vcs.repository.url.full"]; url != "" {
		h.URL = url
	}

	// Extract VCS context for display
	if branch := attrs["vcs.ref.head.name"]; branch != "" {
		h.VCSBranch = branch
	}
	// vcs.ref.head.revision is the semconv name; vcs.revision is an older alias.
	if rev := attrs["vcs.ref.head.revision"]; rev != "" {
		h.VCSRevision = rev
	} else if rev := attrs["vcs.revision"]; rev != "" {
		h.VCSRevision = rev
	}

	// Pipeline-level span (root)
	if pipelineName != "" && taskName == "" {
		h.Category = "pipeline"
		h.IsRoot = true
		h.Icon = "▶ "
		h.Color = "blue"

		// Distinguish pipeline types
		if pipelineType := attrs["cicd.pipeline.type"]; pipelineType != "" {
			h.Icon = cicdIconFromType(pipelineType, "▶ ")
		}

		// Extract run ID for correlation
		if runID := attrs["cicd.pipeline.run.id"]; runID != "" {
			h.RunID = runID
		}

		enrichCICDOutcome(&h, attrs)
		return h
	}

	// Task-level span (intermediate)
	if taskName != "" {
		h.Category = "task"
		h.Icon = cicdIconFromType(taskType, "⚙ ")
		h.Color = "blue"

		// Extract run ID for correlation
		if runID := attrs["cicd.pipeline.task.run.id"]; runID != "" {
			h.RunID = runID
		}

		enrichCICDOutcome(&h, attrs)
		return h
	}

	return SpanHints{}
}

// cicdIconFromType returns an icon based on the CI/CD pipeline or task type.
func cicdIconFromType(cicdType, fallback string) string {
	switch cicdType {
	case "deploy":
		return "🚀 "
	case "build":
		return "🔨 "
	case "test":
		return "🧪 "
	default:
		return fallback
	}
}

// enrichCICDOutcome maps cicd.pipeline.task.run.result to outcome/color.
func enrichCICDOutcome(h *SpanHints, attrs map[string]string) {
	result := attrs["cicd.pipeline.task.run.result"]
	if result == "" {
		result = attrs["cicd.pipeline.run.result"]
	}
	switch result {
	case "success":
		h.Outcome = "success"
		h.Color = "green"
	case "failure":
		h.Outcome = "failure"
		h.Color = "red"
	case "cancelled":
		h.Outcome = "skipped"
		h.Color = "gray"
	case "error":
		h.Outcome = "failure"
		h.Color = "red"
	}

	// Also check OTel status code as fallback
	if h.Outcome == "" {
		switch attrs["otel.status_code"] {
		case "OK":
			h.Outcome = "success"
			h.Color = "green"
		case "ERROR":
			h.Outcome = "failure"
			h.Color = "red"
		}
	}
}
