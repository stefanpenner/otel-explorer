// Package receiver implements an OTLP/HTTP receiver that accepts spans
// via the standard /v1/traces endpoint and feeds them into the analyzer.
package receiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/ingest/otlpfile"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// maxRequestBodyBytes caps OTLP request bodies (compressed and decompressed)
// to bound memory use; oversized requests get 413.
const maxRequestBodyBytes = 128 * 1024 * 1024 // 128MB

// Receiver accepts OTLP/HTTP trace data and accumulates spans.
type Receiver struct {
	mu     sync.Mutex
	spans  []sdktrace.ReadOnlySpan
	server *http.Server
	addr   string
}

// New creates a new OTLP/HTTP receiver listening on the given address.
func New(addr string) *Receiver {
	return &Receiver{
		addr: addr,
	}
}

// Start begins listening for OTLP/HTTP traces.
// It blocks until Stop is called or an error occurs.
func (r *Receiver) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", r.handleTraces)
	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	r.server = &http.Server{
		Addr:              r.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		r.Stop()
	}()

	err := r.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Stop gracefully shuts down the receiver.
func (r *Receiver) Stop() {
	if r.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.server.Shutdown(ctx); err != nil {
			log.Printf("receiver shutdown error: %v", err)
		}
	}
}

// Spans returns all accumulated spans.
func (r *Receiver) Spans() []sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]sdktrace.ReadOnlySpan, len(r.spans))
	copy(result, r.spans)
	return result
}

// SpanCount returns the current number of accumulated spans.
func (r *Receiver) SpanCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.spans)
}

func (r *Receiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer req.Body.Close()
	req.Body = http.MaxBytesReader(w, req.Body, maxRequestBodyBytes)
	body, err := io.ReadAll(req.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Per the OTLP/HTTP spec the body may be gzip-compressed.
	if req.Header.Get("Content-Encoding") == "gzip" {
		gr, gzErr := gzip.NewReader(bytes.NewReader(body))
		if gzErr != nil {
			http.Error(w, "invalid gzip body", http.StatusBadRequest)
			return
		}
		body, gzErr = io.ReadAll(io.LimitReader(gr, maxRequestBodyBytes+1))
		gr.Close()
		if gzErr != nil {
			http.Error(w, "failed to decompress body", http.StatusBadRequest)
			return
		}
		if len(body) > maxRequestBodyBytes {
			http.Error(w, "decompressed body too large", http.StatusRequestEntityTooLarge)
			return
		}
	}

	contentType := req.Header.Get("Content-Type")

	var spans []sdktrace.ReadOnlySpan

	switch {
	case contentType == "application/x-protobuf":
		// OTLP/HTTP binary protobuf: the body is a bare serialized
		// ExportTraceServiceRequest with no length prefix (the length-prefixed
		// fileexporter format handled by ParseProtobuf does not apply here).
		spans, err = otlpfile.ParseProtobufRequest(body)
	default:
		// JSON format (OTLP JSON or stdouttrace)
		spans, err = otlpfile.Parse(bytes.NewReader(body))
	}

	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse traces: %v", err), http.StatusBadRequest)
		return
	}

	r.mu.Lock()
	r.spans = append(r.spans, spans...)
	r.mu.Unlock()

	// Return OTLP ExportTraceServiceResponse (empty JSON object)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{}); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}
