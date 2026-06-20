package enrichment

import "strings"

// ResourceSummary aggregates deployment / infrastructure context per service
// across a trace, drawn from OTel resource (and span) attributes — so "where
// did this run" (environment, cloud region, k8s pod, host) is visible at a
// glance instead of buried in a raw resource-attribute dump.
type ResourceSummary struct {
	services map[string]*serviceContext
	order    []string
}

type serviceContext struct {
	name        string
	environment string
	version     string
	cloud       string // "provider/region", or whichever is present
	k8s         string // "namespace/pod", or whichever is present
	host        string
	container   string
	runtime     string
}

// NewResourceSummary returns an empty, ready-to-accumulate ResourceSummary.
func NewResourceSummary() *ResourceSummary {
	return &ResourceSummary{services: map[string]*serviceContext{}}
}

// Add folds one span's attributes (which should include its resource
// attributes) into the per-service context. Spans without a service.name are
// ignored, since there is nothing to key the context on.
func (r *ResourceSummary) Add(attrs map[string]string) {
	svc := attrs["service.name"]
	if svc == "" {
		return
	}
	c := r.services[svc]
	if c == nil {
		c = &serviceContext{name: svc}
		r.services[svc] = c
		r.order = append(r.order, svc)
	}
	setIfEmpty(&c.environment, firstNonEmpty(attrs, "deployment.environment.name", "deployment.environment"))
	setIfEmpty(&c.version, attrs["service.version"])
	setIfEmpty(&c.cloud, joinNonEmpty("/", attrs["cloud.provider"], attrs["cloud.region"]))
	setIfEmpty(&c.k8s, joinNonEmpty("/", attrs["k8s.namespace.name"], attrs["k8s.pod.name"]))
	setIfEmpty(&c.host, attrs["host.name"])
	setIfEmpty(&c.container, attrs["container.name"])
	setIfEmpty(&c.runtime, firstNonEmpty(attrs, "process.runtime.name", "telemetry.sdk.language"))
}

// HasData reports whether any service context was accumulated.
func (r *ResourceSummary) HasData() bool {
	return len(r.order) > 0
}

// Lines returns one formatted line per service (in first-seen order), e.g.
// "rag-api · production · aws/us-east-1 · k8s rag-ns/rag-7d9f · host node-1 · go".
func (r *ResourceSummary) Lines() []string {
	lines := make([]string, 0, len(r.order))
	for _, svc := range r.order {
		c := r.services[svc]
		segs := []string{c.name}
		segs = appendNonEmpty(segs, c.environment)
		if c.version != "" {
			segs = append(segs, "v"+c.version)
		}
		segs = appendNonEmpty(segs, c.cloud)
		if c.k8s != "" {
			segs = append(segs, "k8s "+c.k8s)
		}
		if c.host != "" {
			segs = append(segs, "host "+c.host)
		}
		if c.container != "" {
			segs = append(segs, "container "+c.container)
		}
		segs = appendNonEmpty(segs, c.runtime)
		lines = append(lines, strings.Join(segs, " · "))
	}
	return lines
}

func setIfEmpty(dst *string, v string) {
	if *dst == "" && v != "" {
		*dst = v
	}
}

func appendNonEmpty(segs []string, v string) []string {
	if v != "" {
		return append(segs, v)
	}
	return segs
}

func firstNonEmpty(attrs map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := attrs[k]; v != "" {
			return v
		}
	}
	return ""
}

func joinNonEmpty(sep string, parts ...string) string {
	var nonEmpty []string
	for _, p := range parts {
		if p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, sep)
}
