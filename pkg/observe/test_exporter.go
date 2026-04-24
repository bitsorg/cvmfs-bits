package observe

import (
	"context"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SpanRecorder is an in-memory span exporter for testing.
// It records all spans so tests can inspect trace data.
type SpanRecorder struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

// ExportSpans records spans in memory.
func (sr *SpanRecorder) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.spans = append(sr.spans, spans...)
	return nil
}

// Shutdown is a no-op for the test exporter.
func (sr *SpanRecorder) Shutdown(ctx context.Context) error {
	return nil
}

// Spans returns all recorded spans.
func (sr *SpanRecorder) Spans() []sdktrace.ReadOnlySpan {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan{}, sr.spans...)
}

// SpansByName returns all spans with a given name.
func (sr *SpanRecorder) SpansByName(name string) []sdktrace.ReadOnlySpan {
	var result []sdktrace.ReadOnlySpan
	for _, span := range sr.Spans() {
		if span.Name() == name {
			result = append(result, span)
		}
	}
	return result
}

// HasSpan checks if a span with the given name exists.
func (sr *SpanRecorder) HasSpan(name string) bool {
	return len(sr.SpansByName(name)) > 0
}

// Clear resets the recorded spans.
func (sr *SpanRecorder) Clear() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.spans = nil
}
