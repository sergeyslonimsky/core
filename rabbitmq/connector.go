package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/furdarius/rabbitroutine"
)

const (
	defaultReconnectionAttempts uint = 3
	defaultConnectionWaitTime        = 2 * time.Second
)

type Connector interface {
	Connect(ctx context.Context) error
	GetConnector() *rabbitroutine.Connector
	StartConsumer(ctx context.Context, consumer rabbitroutine.Consumer) error
}

type connectorOpts struct {
	retriedListener      func(rabbitroutine.Retried)
	dialedListener       func(rabbitroutine.Dialed)
	amqpNotifiedListener func(rabbitroutine.AMQPNotified)
	rabbitRoutineConfig  rabbitroutine.Config
}

type ConnectorOpts func(config *connectorOpts)

func WithConnectorReconnectionOpts(reconnectAttempts uint, wait time.Duration) ConnectorOpts {
	return func(config *connectorOpts) {
		config.rabbitRoutineConfig.ReconnectAttempts = reconnectAttempts
		config.rabbitRoutineConfig.Wait = wait
	}
}

func WithRetriedListener(listener func(rabbitroutine.Retried)) ConnectorOpts {
	return func(config *connectorOpts) {
		config.retriedListener = listener
	}
}

func WithDialedListener(listener func(rabbitroutine.Dialed)) ConnectorOpts {
	return func(config *connectorOpts) {
		config.dialedListener = listener
	}
}

func WithAMQPNotifiedListener(listener func(rabbitroutine.AMQPNotified)) ConnectorOpts {
	return func(config *connectorOpts) {
		config.amqpNotifiedListener = listener
	}
}

type RabbitConnector struct {
	config config
	conn   *rabbitroutine.Connector
}

func NewRabbitConnector(cfg config, opts ...ConnectorOpts) *RabbitConnector {
	defaultOpts := connectorOpts{ //nolint:exhaustruct
		rabbitRoutineConfig: rabbitroutine.Config{
			ReconnectAttempts: defaultReconnectionAttempts,
			Wait:              defaultConnectionWaitTime,
		},
	}

	for _, opt := range opts {
		opt(&defaultOpts)
	}

	conn := rabbitroutine.NewConnector(defaultOpts.rabbitRoutineConfig)

	if defaultOpts.retriedListener != nil {
		conn.AddRetriedListener(defaultOpts.retriedListener)
	}

	if defaultOpts.dialedListener != nil {
		conn.AddDialedListener(defaultOpts.dialedListener)
	}

	if defaultOpts.amqpNotifiedListener != nil {
		conn.AddAMQPNotifiedListener(defaultOpts.amqpNotifiedListener)
	}

	return &RabbitConnector{
		config: cfg,
		conn:   conn,
	}
}

func (c *RabbitConnector) Connect(ctx context.Context) error {
	if err := c.conn.Dial(ctx, c.config.DSN()); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	return nil
}

func (c *RabbitConnector) GetConnector() *rabbitroutine.Connector {
	return c.conn
}

func (c *RabbitConnector) StartConsumer(ctx context.Context, consumer rabbitroutine.Consumer) error {
	if err := c.conn.StartConsumer(ctx, consumer); err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}

	return nil
}
