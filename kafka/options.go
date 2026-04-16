package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

// ErrorAction selects what the Consumer should do after MessageProcessor.Process
// returns a non-nil error. Passed back from a caller-supplied ErrorHandler.
type ErrorAction int

const (
	// ErrorActionSkip logs the error and advances the consumer-group offset
	// past the failing record. Use for poison-pill tolerant pipelines where
	// DLQ is handled by the processor itself.
	//
	// This is the default when no ErrorHandler is configured, preserving the
	// pre-existing behavior.
	ErrorActionSkip ErrorAction = iota

	// ErrorActionRetry does NOT commit the record. The next PollFetches may
	// redeliver it (after a rebalance or client restart). For in-process
	// retry, the caller's ErrorHandler should sleep/backoff before returning.
	//
	// Note: kgo does not re-fetch uncommitted records within the same
	// session automatically — retry here effectively means "let the next
	// rebalance replay it". For tight retries the processor should loop
	// internally and only return Retry after exhausting its budget.
	ErrorActionRetry

	// ErrorActionStop terminates the Consumer: Run returns the original
	// processing error wrapped. The record is NOT committed. Use for fatal
	// conditions where the service should not advance past the failure
	// (e.g., dependency outage that must be surfaced to the operator).
	ErrorActionStop
)

// ErrorHandler is invoked when MessageProcessor.Process returns a non-nil
// error. It decides what the consumer should do with the failing record.
//
// Implementations typically: emit to a DLQ and return ErrorActionSkip;
// increment a metric and return ErrorActionSkip; or on fatal errors return
// ErrorActionStop.
type ErrorHandler func(ctx context.Context, msg Message, err error) ErrorAction

// Option configures any of the kafka constructors (NewClient, NewProducer,
// NewConsumer). Not every option applies to every constructor — irrelevant
// fields are silently ignored.
type Option func(*options)

type options struct {
	logger         *slog.Logger
	otelEnabled    bool
	extraKgo       []kgo.Opt
	produceTimeout time.Duration // applied by NewProducer; ignored elsewhere
	errorHandler   ErrorHandler  // applied by NewConsumer; ignored elsewhere
}

// WithLogger attaches a *slog.Logger used for lifecycle events. Defaults to
// slog.Default() when omitted.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithOtel enables OpenTelemetry instrumentation via the kotel plugin: one
// span per produce/consume operation plus per-broker meter metrics. Uses
// the global tracer/meter providers — call otel.Setup and register the
// provider with app.App before the kafka constructors so the providers are
// non-noop.
func WithOtel() Option {
	return func(o *options) { o.otelEnabled = true }
}

// WithKgoOpts appends raw kgo.Opt values to the underlying franz-go client.
// Escape hatch for tuning kgo features that don't have a typed wrapper
// here (e.g., custom partitioner, sasl, tls, etc).
func WithKgoOpts(opts ...kgo.Opt) Option {
	return func(o *options) { o.extraKgo = append(o.extraKgo, opts...) }
}

// WithErrorHandler configures how NewConsumer reacts to a MessageProcessor
// error. Without this option, the Consumer logs and skips failing records
// (ErrorActionSkip) — which silently advances the consumer-group offset.
//
// Typical usages:
//
//   - Return ErrorActionSkip after pushing the payload to a DLQ topic.
//   - Return ErrorActionStop for unrecoverable dependency outages so the
//     service fails loudly instead of losing messages.
//   - Return ErrorActionRetry to leave the record uncommitted (next
//     rebalance redelivers it).
//
// Ignored by NewClient and NewProducer.
func WithErrorHandler(h ErrorHandler) Option {
	return func(o *options) { o.errorHandler = h }
}

// applyOtel returns the additional kgo options needed to wire kotel hooks
// when OTel is enabled.
func (o *options) applyOtel() []kgo.Opt {
	if !o.otelEnabled {
		return nil
	}

	tracer := kotel.NewTracer(kotel.TracerProvider(otel.GetTracerProvider()))
	meter := kotel.NewMeter(kotel.MeterProvider(otel.GetMeterProvider()))
	k := kotel.NewKotel(kotel.WithTracer(tracer), kotel.WithMeter(meter))

	return []kgo.Opt{kgo.WithHooks(k.Hooks()...)}
}
