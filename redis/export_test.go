package redis

import "go.opentelemetry.io/otel/trace"

// WithTracerProvider lets tests record spans without touching the global
// tracer provider, which parallel tests would race on.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}
