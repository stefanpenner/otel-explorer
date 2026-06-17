package export

import (
	"encoding/json"
	"io"
)

// RenderJSON writes the report as indented JSON. Stable schema (see
// SchemaVersion); safe to pipe to jq.
func RenderJSON(w io.Writer, rep *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(rep)
}
