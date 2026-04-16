package kafka

import "errors"

// Sentinel errors returned by kafka constructors and methods. Stable
// identity so callers can errors.Is them.
var (
	// ErrEmptyBrokersList is returned when ClientConfig/ConsumerConfig/
	// ProducerConfig is constructed without any seed brokers.
	ErrEmptyBrokersList = errors.New("brokers list cannot be empty")

	// ErrNoBrokersDiscovered is returned by Client.Healthcheck when the
	// underlying kgo.Client has not discovered any brokers yet (the seed
	// brokers were unreachable, or no metadata fetch has completed).
	ErrNoBrokersDiscovered = errors.New("no brokers discovered")

	// ErrInvalidOffset is returned by NewConsumer when ConsumerConfig.Offset
	// is set to a value other than "" / "newest" / "oldest".
	ErrInvalidOffset = errors.New("invalid offset")

	// ErrNilProcessor is returned by NewConsumer when the MessageProcessor
	// argument is nil.
	ErrNilProcessor = errors.New("processor cannot be nil")

	// ErrNilConsumerGroup is returned by NewConsumer when ConsumerConfig.Group
	// is empty.
	ErrNilConsumerGroup = errors.New("consumer group cannot be empty")

	// ErrEmptyTopic is returned by NewConsumer when ConsumerConfig.Topics
	// is empty.
	ErrEmptyTopic = errors.New("topics list cannot be empty")
)
