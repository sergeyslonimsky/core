package v2

// Topic is a type for topic names.
type Topic string

// Message is a structure for passing messages.
type Message struct {
	Topic   Topic
	Key     []byte
	Message []byte
}

// ConsumerConfig is the configuration for consumer.
type ConsumerConfig struct {
	Brokers string
	Group   string
	Offset  string
	Topics  []string
}

// ProducerConfig is the configuration for producer.
type ProducerConfig struct {
	Brokers string
	Topic   string
}

// Config is the common configuration.
type Config struct {
	Brokers   string
	Consumers map[string]ConsumerConfig
	Producers map[string]ProducerConfig
}

// GetProducerConfig returns producer configuration with fallback to common broker.
func (c Config) GetProducerConfig(producer string) ProducerConfig {
	cfg := c.Producers[producer]

	if cfg.Brokers == "" {
		cfg.Brokers = c.Brokers
	}

	return cfg
}

// GetConsumerConfig returns consumer configuration with fallback to common broker.
func (c Config) GetConsumerConfig(consumer string) ConsumerConfig {
	cfg := c.Consumers[consumer]

	if cfg.Brokers == "" {
		cfg.Brokers = c.Brokers
	}

	return cfg
}
