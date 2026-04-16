package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// Consumer is a kafka consumer-group host backed by a single franz-go
// client. Implements lifecycle.Runner — register with app.App.
//
// Uses manual commit (DisableAutoCommit + MarkCommitRecords +
// CommitMarkedOffsets) so the offset-advance decision is driven by the
// configured ErrorHandler rather than by franz-go's auto-commit.
//
// Default error policy (no WithErrorHandler): log and skip failing records.
// See WithErrorHandler + ErrorAction for fine-grained control (DLQ, retry,
// fail-fast).
type Consumer struct {
	client       *kgo.Client
	processor    MessageProcessor
	errorHandler ErrorHandler
	logger       *slog.Logger

	shutdownOnce sync.Once
}

// NewConsumer creates a consumer-group client.
//
// cfg.Brokers, cfg.Group, and cfg.Topics are required; cfg.Offset defaults
// to "newest" when empty (other valid value: "oldest").
func NewConsumer(
	cfg ConsumerConfig,
	processor MessageProcessor,
	opts ...Option,
) (*Consumer, error) {
	if processor == nil {
		return nil, ErrNilProcessor
	}

	if len(cfg.Brokers) == 0 {
		return nil, ErrEmptyBrokersList
	}

	if cfg.Group == "" {
		return nil, ErrNilConsumerGroup
	}

	if len(cfg.Topics) == 0 {
		return nil, ErrEmptyTopic
	}

	offset, err := parseOffset(cfg.Offset)
	if err != nil {
		return nil, fmt.Errorf("parse offset: %w", err)
	}

	o := &options{logger: slog.Default()} //nolint:exhaustruct
	for _, apply := range opts {
		apply(o)
	}

	// Base capacity: 5 typed kgo opts appended just below
	// (SeedBrokers, ConsumerGroup, ConsumeTopics, ConsumeResetOffset,
	// DisableAutoCommit) plus the caller's extra kgo opts.
	const baseKgoOptsCap = 5

	kgoOpts := make([]kgo.Opt, 0, baseKgoOptsCap+len(o.extraKgo))
	kgoOpts = append(kgoOpts,
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(offset),
		kgo.DisableAutoCommit(),
	)
	kgoOpts = append(kgoOpts, o.applyOtel()...)
	kgoOpts = append(kgoOpts, o.extraKgo...)

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("create franz-go consumer client: %w", err)
	}

	return &Consumer{ //nolint:exhaustruct // shutdownOnce has valid zero value
		client:       client,
		processor:    processor,
		errorHandler: o.errorHandler,
		logger:       o.logger,
	}, nil
}

// Run blocks polling for messages until ctx is cancelled (graceful) or the
// configured ErrorHandler returns ErrorActionStop (fatal).
//
// Returns nil on ctx cancellation. Returns a wrapping error when
// ErrorActionStop is selected by the handler.
//
// At-least-once semantics: on ErrorActionStop, records that were processed
// successfully earlier in the same fetch batch are NOT committed — they
// will be redelivered on restart. Processors must be idempotent to handle
// this (as they must for any Kafka consumer-group restart).
//
// Implements lifecycle.Runner.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		// Detect ctx termination (cancel OR deadline) at the top of the
		// loop. This covers both causes uniformly — relying on
		// errors.Is(err, context.Canceled) in fetches.Errors() would miss
		// context.DeadlineExceeded and spin forever on an expired timeout.
		select {
		case <-ctx.Done():
			c.logger.InfoContext(ctx, "kafka consumer context cancelled, stopping")

			return nil
		default:
		}

		fetches := c.client.PollFetches(ctx)

		// If ctx terminated while we were polling, exit before processing
		// stale data. The errors in fetches are just reflections of
		// cancellation and should not be logged as real fetch failures.
		if ctx.Err() != nil {
			continue
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.logger.ErrorContext(ctx, "kafka fetch error",
					slog.String("topic", err.Topic),
					slog.Any("err", err.Err),
				)
			}
		}

		if err := c.processFetches(ctx, fetches); err != nil {
			return err
		}

		if err := c.client.CommitMarkedOffsets(ctx); err != nil {
			if ctx.Err() != nil {
				continue // next iteration's select-default will exit
			}

			c.logger.ErrorContext(ctx, "kafka commit failed", slog.Any("err", err))
		}
	}
}

// Shutdown closes the underlying client, which unblocks PollFetches in Run
// and drains in-flight fetches. Safe to call multiple times. Implements
// lifecycle.Resource.
//
// The ctx is currently advisory — kgo.Client.Close does not honor it.
func (c *Consumer) Shutdown(_ context.Context) error {
	c.shutdownOnce.Do(func() {
		c.client.Close()
	})

	return nil
}

// processFetches iterates records in the fetch batch, invoking the
// processor for each and applying the ErrorHandler's decision. Returns a
// non-nil error only for ErrorActionStop; otherwise nil.
func (c *Consumer) processFetches(ctx context.Context, fetches kgo.Fetches) error {
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()

		msg := Message{
			Topic:   Topic(record.Topic),
			Key:     record.Key,
			Message: record.Value,
		}

		err := c.processor.Process(ctx, msg)
		if err == nil {
			c.client.MarkCommitRecords(record)

			continue
		}

		action := c.handleProcessError(ctx, msg, record, err)

		switch action {
		case ErrorActionSkip:
			c.client.MarkCommitRecords(record)
		case ErrorActionRetry:
			// Intentionally do NOT mark for commit. Record stays
			// uncommitted and is eligible for redelivery on the next
			// rebalance.
		case ErrorActionStop:
			return fmt.Errorf("kafka consumer stopped on processor error: %w", err)
		default:
			// Unknown action from a caller-supplied ErrorHandler — log and
			// treat as Skip to avoid stalling the consumer on an invalid
			// decision value.
			c.logger.ErrorContext(ctx, "kafka consumer: unknown ErrorAction, defaulting to Skip",
				slog.Int("action", int(action)),
				slog.String("topic", record.Topic),
				slog.Int64("offset", record.Offset),
			)
			c.client.MarkCommitRecords(record)
		}
	}

	return nil
}

// handleProcessError runs the caller-supplied ErrorHandler (or falls back
// to the default Skip behavior) and returns the decided action.
func (c *Consumer) handleProcessError(
	ctx context.Context,
	msg Message,
	record *kgo.Record,
	err error,
) ErrorAction {
	c.logger.ErrorContext(ctx, "kafka message processing failed",
		slog.String("topic", record.Topic),
		slog.Int64("offset", record.Offset),
		slog.Any("err", err),
	)

	if c.errorHandler == nil {
		return ErrorActionSkip
	}

	return c.errorHandler(ctx, msg, err)
}

// parseOffset converts the string offset value into kgo.Offset.
func parseOffset(offset string) (kgo.Offset, error) {
	switch strings.ToLower(offset) {
	case "", "newest":
		return kgo.NewOffset().AtEnd(), nil
	case "oldest":
		return kgo.NewOffset().AtStart(), nil
	default:
		return kgo.Offset{}, fmt.Errorf("%w: offset %s", ErrInvalidOffset, offset)
	}
}

// Compile-time assertions.
var (
	_ lifecycle.Runner   = (*Consumer)(nil)
	_ lifecycle.Resource = (*Consumer)(nil)
)
