package grpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

type Config struct {
	Port string
}

type Server struct {
	server *grpc.Server
	port   string
}

func NewServer(config Config) *Server {
	server := grpc.NewServer()

	return &Server{
		server: server,
		port:   config.Port,
	}
}

func (s *Server) Mount(mountFunc func(server *grpc.Server)) {
	mountFunc(s.server)
}

func (s *Server) Run(ctx context.Context) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+s.port) //nolint:exhaustruct
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	srvErr := make(chan error, 1)

	// Start the server in a goroutine
	go func() {
		srvErr <- s.server.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		// If context is done, gracefully shut down the server
		s.Shutdown()

		return nil
	case err := <-srvErr:
		// If server exits with an error (or nil), return the error
		return err
	}
}

func (s *Server) Shutdown() {
	s.server.GracefulStop()
}

func (s *Server) Address() string {
	return ":" + s.port
}
