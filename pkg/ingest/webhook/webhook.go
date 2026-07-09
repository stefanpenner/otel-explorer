package webhook

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

type webhookPayload struct {
	Action      string       `json:"action"`
	WorkflowRun *workflowRun `json:"workflow_run"`
	WorkflowJob *workflowJob `json:"workflow_job"`
	Repository  *repository  `json:"repository"`
}

type workflowRun struct {
	HeadSHA string `json:"head_sha"`
}

type workflowJob struct {
	HeadSHA string `json:"head_sha"`
}

type repository struct {
	FullName string `json:"full_name"`
}

// maxWebhookBytes caps incoming webhook payloads to bound memory use.
const maxWebhookBytes = 10 * 1024 * 1024 // 10MB

// ParseWebhook reads a GitHub Actions webhook JSON payload and returns
// GitHub URLs to analyze. It supports workflow_run and workflow_job events.
func ParseWebhook(r io.Reader) (urls []string, err error) {
	defer utils.RecoverBoundary0(&err)
	data, err := io.ReadAll(io.LimitReader(r, maxWebhookBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read webhook payload: %w", err)
	}

	if len(data) > maxWebhookBytes {
		return nil, fmt.Errorf("webhook payload too large (max %d bytes)", maxWebhookBytes)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("empty webhook payload")
	}

	var payload webhookPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("failed to parse webhook JSON: %w", err)
	}

	if payload.Repository == nil {
		return nil, fmt.Errorf("webhook payload missing repository field")
	}

	if payload.Repository.FullName == "" {
		return nil, fmt.Errorf("webhook payload has empty repository full_name")
	}

	// Prefer workflow_run over workflow_job
	var sha string
	switch {
	case payload.WorkflowRun != nil:
		sha = payload.WorkflowRun.HeadSHA
	case payload.WorkflowJob != nil:
		sha = payload.WorkflowJob.HeadSHA
	default:
		return nil, fmt.Errorf("webhook payload has no recognized event (need workflow_run or workflow_job)")
	}

	if sha == "" {
		return nil, fmt.Errorf("webhook payload has empty head_sha")
	}

	url := fmt.Sprintf("%s/commit/%s", payload.Repository.FullName, sha)
	return []string{url}, nil
}
