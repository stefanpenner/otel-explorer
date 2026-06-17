package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/export"
)

// exportFormats are the structured/document output formats that render from the
// shared export.Report IR (as opposed to the legacy terminal/markdown/otel
// renderers).
var exportRenderers = map[string]func(io.Writer, *export.Report) error{
	"json": export.RenderJSON,
	"html": export.RenderHTML,
	"xlsx": export.RenderXLSX,
	"doc":  export.RenderDOCX,
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

func fileExt(format string) string {
	if format == "doc" {
		return "docx"
	}
	return format
}

// emitReport renders rep in the given format. Text formats (json/html) go to
// stdout unless --out is set; binary formats (xlsx/doc) go to --out or a
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
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", outFile)
	return nil
}
