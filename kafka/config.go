package kafka

type ConsumerConfig struct {
	Brokers string
	Group   string
	Offset  string
	Topics  []string
}

type ProducerConfig struct {
	Brokers string
	Topic   string
}

type Config struct {
	Brokers   string
	Consumers map[string]ConsumerConfig
	Producers map[string]ProducerConfig
}

func (c Config) GetProducerConfig(producer string) ProducerConfig {
	cfg := c.Producers[producer]

	if cfg.Brokers == "" {
		cfg.Brokers = c.Brokers
	}

	return cfg
}

func (c Config) GetConsumerConfig(consumer string) ConsumerConfig {
	cfg := c.Consumers[consumer]

	if cfg.Brokers == "" {
		cfg.Brokers = c.Brokers
	}

	return cfg
}
