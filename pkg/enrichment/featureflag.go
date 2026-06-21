package enrichment

import "strings"

// FeatureFlagFromEvent returns a "key=variant" (or bare "key") summary when the
// given span event records a feature-flag evaluation — i.e. it is named
// "feature_flag" (the OpenFeature convention) or carries a feature_flag.key
// attribute. Returns "" otherwise. Both the stable feature_flag.result.variant
// and the legacy feature_flag.variant are accepted.
func FeatureFlagFromEvent(name string, attrs map[string]string) string {
	key := attrs["feature_flag.key"]
	if key == "" && name != "feature_flag" {
		return ""
	}
	if key == "" {
		key = "?"
	}
	variant := firstNonEmpty(attrs, "feature_flag.result.variant", "feature_flag.variant")
	if variant != "" {
		return key + "=" + variant
	}
	return key
}

// ApplyFeatureFlags folds feature-flag evaluations (collected from a span's
// events) into the hints' Detail, each marked with a 🚩, so the active flag
// variant is visible in the timeline rather than only in the inspector. Flags
// already present in Detail are not duplicated. This is informational and does
// not change the span's outcome.
func ApplyFeatureFlags(h *SpanHints, flags []string) {
	for _, f := range flags {
		if f == "" || strings.Contains(h.Detail, f) {
			continue
		}
		seg := "🚩 " + f
		if h.Detail == "" {
			h.Detail = seg
		} else {
			h.Detail += " · " + seg
		}
	}
}
