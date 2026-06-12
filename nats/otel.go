package nats

import (
	"context"
	"math"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// clampUint64ToInt64 saturates v at math.MaxInt64 so OTel attributes never
// overflow on JetStream streams whose Sequence eventually exceeds 2^63.
func clampUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(v)
}

const (
	tracerName       = "github.com/sergeyslonimsky/core/nats"
	systemAttribute  = "nats"
	operationProcess = "process"
	operationPublish = "publish"
)

// processFn matches Processor.Process — used by the consume-side
// instrumentation wrappers so we can decorate the function value without
// exposing the Processor type to OTel internals.
type processFn[I any] func(ctx context.Context, msg Message[I]) error

// natsHeaderCarrier adapts nats.Header to the OpenTelemetry TextMapCarrier
// interface for trace context propagation across publish/consume.
type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string {
	return nats.Header(c).Get(key)
}

func (c natsHeaderCarrier) Set(key, value string) {
	nats.Header(c).Set(key, value)
}

func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}

	return keys
}

// injectTraceContextInto writes the current span's trace context into
// headers in place. Caller MUST own the map (it must not be shared with
// the user) — clone before calling if ownership is unclear.
func injectTraceContextInto(ctx context.Context, h nats.Header) {
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier(h))
}

// extractTraceContext returns a context decorated with the trace context
// carried by the given NATS headers. Returns ctx unchanged when h is nil
// or carries nothing.
func extractTraceContext(ctx context.Context, h nats.Header) context.Context {
	if h == nil {
		return ctx
	}

	return otel.GetTextMapPropagator().Extract(ctx, natsHeaderCarrier(h))
}

// publishSpanAttrs returns the standard messaging.* attributes for a
// publish-side span.
func publishSpanAttrs(subject string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", systemAttribute),
		attribute.String("messaging.destination.name", subject),
		attribute.String("messaging.operation", operationPublish),
	}
}

// startPublishSpan opens a SpanKindProducer span and returns the
// trace-context-decorated ctx and span. The caller MUST call span.End()
// (deferred is fine via endPublishSpan) and inject the trace context into
// owned headers via injectTraceContextInto using the returned ctx.
//
//nolint:spancheck // span is ended by the caller via endPublishSpan — spancheck cannot trace the indirection.
func startPublishSpan(ctx context.Context, subject string) (context.Context, trace.Span) {
	tracer := otel.Tracer(tracerName)

	spanCtx, span := tracer.Start(ctx,
		subject+" "+operationPublish,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(publishSpanAttrs(subject)...),
	)

	return spanCtx, span
}

// endPublishSpan records the publish result and ends the span. Always
// callable; no-op-safe on a nil error.
func endPublishSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
}

// instrumentProcessSubscriber wraps a Core NATS Process function with an
// OTel span. Extracts the upstream trace context from the message headers
// so the consume span chains to the producer span.
func instrumentProcessSubscriber[I any](next processFn[I]) processFn[I] {
	tracer := otel.Tracer(tracerName)

	return func(ctx context.Context, msg Message[I]) error {
		ctx = extractTraceContext(ctx, msg.Headers)

		spanCtx, span := tracer.Start(ctx,
			msg.Subject+" "+operationProcess,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", systemAttribute),
				attribute.String("messaging.destination.name", msg.Subject),
				attribute.String("messaging.operation", operationProcess),
			),
		)
		defer span.End()

		err := next(spanCtx, msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		return err
	}
}

// instrumentProcessJS wraps a JetStream Process function with an OTel span.
// Adds JetStream-specific attributes (stream sequence, delivery count) and
// chains to the producer span via the extracted trace context.
func instrumentProcessJS[I any](next processFn[I]) processFn[I] {
	tracer := otel.Tracer(tracerName)

	return func(ctx context.Context, msg Message[I]) error {
		ctx = extractTraceContext(ctx, msg.Headers)

		spanCtx, span := tracer.Start(ctx,
			msg.Subject+" "+operationProcess,
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", systemAttribute),
				attribute.String("messaging.destination.name", msg.Subject),
				attribute.String("messaging.operation", operationProcess),
				attribute.Int64("messaging.message.sequence", clampUint64ToInt64(msg.Sequence)),
				attribute.Int64("messaging.nats.delivery_attempt", clampUint64ToInt64(msg.NumDelivered)),
			),
		)
		defer span.End()

		err := next(spanCtx, msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		return err
	}
}

// Compile-time check that natsHeaderCarrier satisfies the carrier
// interface used by the TextMapPropagator.
var _ propagation.TextMapCarrier = natsHeaderCarrier(nil)
