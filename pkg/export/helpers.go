package export

import (
	"fmt"
	"strconv"

	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

func itoa(n int) string                 { return strconv.Itoa(n) }
func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }

// humanSec formats a duration given in seconds as a human-readable string
// (e.g. "1m 30s"), reusing the shared humanizer for consistency with the
// terminal output.
func humanSec(sec float64) string {
	if sec <= 0 {
		return "0s"
	}
	return utils.HumanizeTime(sec)
}
