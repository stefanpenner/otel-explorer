package enrichment

import "testing"

func TestFeatureFlagFromEvent(t *testing.T) {
	cases := []struct {
		name  string
		attrs map[string]string
		want  string
	}{
		{"feature_flag", map[string]string{"feature_flag.key": "dark-mode", "feature_flag.result.variant": "on"}, "dark-mode=on"},
		{"feature_flag", map[string]string{"feature_flag.key": "beta", "feature_flag.variant": "true"}, "beta=true"},
		{"feature_flag", map[string]string{"feature_flag.key": "rollout"}, "rollout"},
		{"some-event", map[string]string{"feature_flag.key": "k"}, "k"},
		{"feature_flag", nil, "?"},
		{"log", map[string]string{"level": "info"}, ""},
	}
	for _, c := range cases {
		if got := FeatureFlagFromEvent(c.name, c.attrs); got != c.want {
			t.Errorf("FeatureFlagFromEvent(%q, %v) = %q, want %q", c.name, c.attrs, got, c.want)
		}
	}
}

func TestApplyFeatureFlags(t *testing.T) {
	h := SpanHints{Detail: "GET /home [200]"}
	ApplyFeatureFlags(&h, []string{"dark-mode=on", "beta=true"})
	want := "GET /home [200] · 🚩 dark-mode=on · 🚩 beta=true"
	if h.Detail != want {
		t.Errorf("Detail = %q, want %q", h.Detail, want)
	}
}

func TestApplyFeatureFlags_EmptyDetail(t *testing.T) {
	h := SpanHints{}
	ApplyFeatureFlags(&h, []string{"dark-mode=on"})
	if h.Detail != "🚩 dark-mode=on" {
		t.Errorf("Detail = %q", h.Detail)
	}
}

func TestApplyFeatureFlags_NoDuplicate(t *testing.T) {
	h := SpanHints{Detail: "🚩 dark-mode=on"}
	ApplyFeatureFlags(&h, []string{"dark-mode=on"})
	if h.Detail != "🚩 dark-mode=on" {
		t.Errorf("expected no duplicate, got %q", h.Detail)
	}
}
