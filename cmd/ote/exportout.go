package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/export"
)

// exportFormats are the structured/document output formats that render from the
// shared export.Report IR (as opposed to the legacy terminal/markdown/otel
// renderers).
var exportRenderers = map[string]func(io.Writer, *export.Report) error{
	"json":  export.RenderJSON,
	"html":  export.RenderHTML,
	"xlsx":  export.RenderXLSX,
	"doc":   export.RenderDOCX,
	"slack": export.RenderSlack,
}

// binaryFormats can't stream to a TTY, so they default to a file destination.
var binaryFormats = map[string]bool{"xlsx": true, "doc": true}

func isExportFormat(s string) bool { _, ok := exportRenderers[s]; return ok }

func validOutputFormat(s string) bool {
	switch s {
	case "stdout", "markdown", "otel":
		return true
	}
	return isExportFormat(s)
}

func validTrendsFormat(s string) bool {
	switch s {
	case "terminal", "json":
		return true
	}
	return isExportFormat(s)
}

func generatedAt() string { return time.Now().UTC().Format(time.RFC3339) }

// resolveSlackWebhook prefers the explicit flag, falling back to the
// SLACK_WEBHOOK_URL environment variable.
func resolveSlackWebhook(flag string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv("SLACK_WEBHOOK_URL")
}

// resolvePerfettoUI prefers the explicit flag, falling back to PERFETTO_UI_URL.
func resolvePerfettoUI(flag string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv("PERFETTO_UI_URL")
}

func fileExt(format string) string {
	if format == "doc" {
		return "docx"
	}
	return format
}

// deliverReport sends a report to its destination: a Slack webhook when one is
// configured for --output=slack, otherwise a file or stdout via emitReport.
func deliverReport(rep *export.Report, format, outFile, slackWebhook string) error {
	if format == "slack" && slackWebhook != "" && outFile == "" {
		return postSlackWebhook(rep, slackWebhook)
	}
	return emitReport(rep, format, outFile)
}

// postSlackWebhook renders the report as Slack Block Kit and POSTs it to the
// incoming-webhook URL. Providing the URL is the authorization to send.
func postSlackWebhook(rep *export.Report, webhookURL string) error {
	var buf bytes.Buffer
	if err := export.RenderSlack(&buf, rep); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("slack webhook returned %s: %s", resp.Status, string(body))
	}
	fmt.Fprintln(os.Stderr, "posted report to Slack")
	return nil
}

// emitReport renders rep in the given format. Text formats (json/html/slack) go
// to stdout unless --out is set; binary formats (xlsx/doc) go to --out or a
// default filename, and the path is reported on stderr (mirroring perfetto).
func emitReport(rep *export.Report, format, outFile string) error {
	render := exportRenderers[format]
	if render == nil {
		return fmt.Errorf("unknown export format %q", format)
	}
	if outFile == "" && binaryFormats[format] {
		outFile = "ote-report." + fileExt(format)
	}
	if outFile == "" {
		return render(os.Stdout, rep)
	}
	f, err := os.Create(outFile)
	if err != nil {
		return err
	}
	if err := render(f, rep); err != nil {
		f.Close()
		os.Remove(outFile)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", outFile)
	return nil
}
