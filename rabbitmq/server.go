package rabbitmq

import (
	"context"
	"fmt"

	"github.com/furdarius/rabbitroutine"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conn      Connector
	consumers []rabbitroutine.Consumer
}

func NewServer(cfg Config) *Server {
	return NewServerWithConnector(NewRabbitConnector(cfg))
}

func NewServerWithConnector(conn Connector) *Server {
	return &Server{
		conn:      conn,
		consumers: make([]rabbitroutine.Consumer, 0),
	}
}

func (s *Server) AddConsumer(consumer rabbitroutine.Consumer) {
	s.consumers = append(s.consumers, consumer)
}

func (s *Server) Run(ctx context.Context) error {
	// If no consumers are defined, there's nothing to do.
	if len(s.consumers) == 0 {
		return nil
	}

	// Create an errgroup and use the passed-in context for proper cancellation handling.
	g, ctx := errgroup.WithContext(ctx)

	// Start the connection in a separate goroutine.
	g.Go(func() error {
		// Connect to RabbitMQ using the provided context.
		if err := s.conn.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
		}

		return nil
	})

	// Start each consumer in its own goroutine.
	for _, consumer := range s.consumers {
		g.Go(func() error {
			// Start the consumer using the provided context.
			if err := s.conn.StartConsumer(ctx, consumer); err != nil {
				return fmt.Errorf("failed to start consumer: %w", err)
			}

			return nil
		})
	}

	// Wait for all goroutines to finish and check for errors.
	if err := g.Wait(); err != nil {
		return fmt.Errorf("RabbitMQ server encountered an error: %w", err)
	}

	return nil
}
