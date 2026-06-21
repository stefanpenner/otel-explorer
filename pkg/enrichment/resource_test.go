package enrichment

import (
	"reflect"
	"testing"
)

func TestResourceSummary_PerService(t *testing.T) {
	r := NewResourceSummary()
	// Two spans of the same service contribute complementary context.
	r.Add(map[string]string{
		"service.name":           "rag-api",
		"deployment.environment": "production",
		"cloud.provider":         "aws",
		"cloud.region":           "us-east-1",
	})
	r.Add(map[string]string{
		"service.name":       "rag-api",
		"k8s.namespace.name": "rag-ns",
		"k8s.pod.name":       "rag-7d9f",
		"host.name":          "node-1",
		"service.version":    "1.4.0",
	})
	// A second service.
	r.Add(map[string]string{
		"service.name":           "worker",
		"deployment.environment": "production",
		"process.runtime.name":   "go",
	})
	// A span with no service.name is ignored.
	r.Add(map[string]string{"db.system": "postgresql"})

	if !r.HasData() {
		t.Fatal("expected data")
	}
	got := r.Lines()
	want := []string{
		"rag-api · production · v1.4.0 · aws/us-east-1 · k8s rag-ns/rag-7d9f · host node-1",
		"worker · production · go",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines mismatch:\n got %#v\nwant %#v", got, want)
	}
}

func TestResourceSummary_PartialCloud(t *testing.T) {
	r := NewResourceSummary()
	// Only a region, no provider — still surfaced.
	r.Add(map[string]string{
		"service.name": "svc",
		"cloud.region": "europe-west1",
	})
	got := r.Lines()
	want := []string{"svc · europe-west1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v", got, want)
	}
}

func TestResourceSummary_Empty(t *testing.T) {
	r := NewResourceSummary()
	r.Add(map[string]string{"http.request.method": "GET"})
	if r.HasData() {
		t.Error("expected no data without service.name")
	}
}

func TestResourceSummary_EnvironmentNameVariant(t *testing.T) {
	// The newer deployment.environment.name is preferred over the legacy key.
	r := NewResourceSummary()
	r.Add(map[string]string{
		"service.name":                "svc",
		"deployment.environment.name": "staging",
	})
	got := r.Lines()
	want := []string{"svc · staging"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v", got, want)
	}
}
