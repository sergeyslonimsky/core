package v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrInvalidOffset    = errors.New("invalid offset")
	ErrNilProcessor     = errors.New("processor cannot be nil")
	ErrNilConsumerGroup = errors.New("consumer group cannot be empty")
	ErrEmptyTopic       = errors.New("topics list cannot be empty")
)

type ConsumerGroup struct {
	client    *kgo.Client
	topics    []string
	processor MessageProcessor
}

func NewConsumer(cfg ConsumerConfig, processor MessageProcessor) (*ConsumerGroup, error) {
	if processor == nil {
		return nil, ErrNilProcessor
	}

	offset, err := parseOffset(cfg.Offset)
	if err != nil {
		return nil, fmt.Errorf("parse offset: %w", err)
	}

	brokers := strings.Split(cfg.Brokers, ",")
	if len(brokers) == 0 || cfg.Brokers == "" {
		return nil, ErrEmptyBrokersString
	}

	if cfg.Group == "" {
		return nil, ErrNilConsumerGroup
	}

	if len(cfg.Topics) == 0 {
		return nil, ErrEmptyTopic
	}

	// Create franz-go client with consumer group configuration
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(offset),
		// Disable auto-commit, we'll commit manually after processing
		kgo.DisableAutoCommit(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create franz-go consumer client: %w", err)
	}

	return &ConsumerGroup{
		client:    client,
		topics:    cfg.Topics,
		processor: processor,
	}, nil
}

// Run starts the consumer loop. Blocks until the context is cancelled.
func (c *ConsumerGroup) Run(ctx context.Context) error { //nolint:cyclop
	defer c.client.Close()

	for {
		// Check context before each fetch
		select {
		case <-ctx.Done():
			slog.Info("consumer context cancelled, stopping")

			return fmt.Errorf("context error: %w", ctx.Err())
		default:
		}

		// Fetch a batch of messages
		fetches := c.client.PollFetches(ctx)

		// Check errors at the fetch level
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if errors.Is(err.Err, context.Canceled) {
					return context.Canceled
				}

				slog.Error("fetch error", slog.String("error", err.Err.Error()))
			}
			// Continue processing, there might be successful partitions
		}

		// Process each message
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			msg := Message{
				Topic:   Topic(record.Topic),
				Key:     record.Key,
				Message: record.Value,
			}

			// Process message with processor
			if err := c.processor.Process(ctx, msg); err != nil {
				slog.Error("failed to process message",
					slog.String("topic", record.Topic),
					slog.Int64("offset", record.Offset),
					slog.String("error", err.Error()),
				)
				// Continue processing next messages
				// (matches sarama implementation behavior)
			}

			// Mark offset for commit after processing
			// In franz-go commits are async, but we can mark after each message
			c.client.MarkCommitRecords(record)
		}

		// Commit all marked offsets
		if err := c.client.CommitMarkedOffsets(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return context.Canceled
			}

			slog.Error("failed to commit offsets", slog.String("error", err.Error()))
		}
	}
}

// parseOffset parses string offset value into kgo.Offset.
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
