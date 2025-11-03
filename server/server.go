package server

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

type runner interface {
	Run(ctx context.Context) error
}

type Server struct {
	runners []runner
}

func NewServer(runners ...runner) *Server {
	return &Server{
		runners: runners,
	}
}

func (s *Server) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, r := range s.runners {
		g.Go(func() error {
			return r.Run(ctx)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}
