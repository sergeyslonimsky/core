package v2

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client is a basic wrapper around franz-go client.
type Client struct {
	client *kgo.Client
	config ClientConfig
}

// ClientConfig is the configuration for creating a client.
type ClientConfig struct {
	Brokers  []string
	ClientID string
}

// NewClient creates a new Kafka client with basic configuration.
func NewClient(_ context.Context, config ClientConfig) (*Client, error) {
	if len(config.Brokers) == 0 {
		return nil, ErrEmptyBrokersList
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
	}

	if config.ClientID != "" {
		opts = append(opts, kgo.ClientID(config.ClientID))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create franz-go client: %w", err)
	}

	return &Client{
		client: client,
		config: config,
	}, nil
}

// NewClientFromBrokerString creates a client from broker string (for compatibility with old API).
func NewClientFromBrokerString(ctx context.Context, brokers, clientID string) (*Client, error) {
	if brokers == "" {
		return nil, ErrEmptyBrokersString
	}

	config := ClientConfig{
		Brokers:  strings.Split(brokers, ","),
		ClientID: clientID,
	}

	return NewClient(ctx, config)
}

// GetKgoClient returns the internal franz-go client.
func (c *Client) GetKgoClient() *kgo.Client {
	return c.client
}

// Close closes the client.
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// Ping checks connection to brokers.
func (c *Client) Ping(_ context.Context) error {
	// Simple connection check via metadata retrieval
	brokers := c.client.DiscoveredBrokers()
	if len(brokers) == 0 {
		return ErrNoBrokersDiscovered
	}

	return nil
}
