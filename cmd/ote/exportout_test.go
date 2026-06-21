package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/export"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitReport_RemovesPartialFileOnRenderError(t *testing.T) {
	orig := exportRenderers["json"]
	exportRenderers["json"] = func(_ io.Writer, _ *export.Report) error {
		return fmt.Errorf("simulated render failure")
	}
	t.Cleanup(func() { exportRenderers["json"] = orig })

	outFile := filepath.Join(t.TempDir(), "out.json")
	err := emitReport(&export.Report{Kind: export.KindRunAnalysis}, "json", outFile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated render failure")

	_, statErr := os.Stat(outFile)
	assert.True(t, os.IsNotExist(statErr), "partial file must be removed on render error")
}

func TestEmitReport_WritesFileOnSuccess(t *testing.T) {
	orig := exportRenderers["json"]
	exportRenderers["json"] = func(w io.Writer, _ *export.Report) error {
		_, err := w.Write([]byte("{}"))
		return err
	}
	t.Cleanup(func() { exportRenderers["json"] = orig })

	outFile := filepath.Join(t.TempDir(), "out.json")
	require.NoError(t, emitReport(&export.Report{Kind: export.KindRunAnalysis}, "json", outFile))
	_, err := os.Stat(outFile)
	assert.NoError(t, err, "file must exist on success")
}
