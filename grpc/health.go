package grpc

import (
	"context"

	grpchealth "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sergeyslonimsky/core/lifecycle"
)

// healthService is a minimal grpc.health.v1 implementation driven by a
// core/lifecycle.Healthchecker. Check reports SERVING if the checker
// returns nil, NOT_SERVING otherwise. Watch is not supported (returns
// Unimplemented) — most infra (k8s/Envoy) only uses Check for gRPC health
// probes.
type healthService struct {
	grpchealth.UnimplementedHealthServer

	checker lifecycle.Healthchecker
}

// Check implements grpchealth.HealthServer.Check. Service-specific health
// (non-empty req.Service) is not modeled — the aggregated app-level
// Healthchecker is returned for every service.
func (h *healthService) Check(
	ctx context.Context,
	_ *grpchealth.HealthCheckRequest,
) (*grpchealth.HealthCheckResponse, error) {
	status := grpchealth.HealthCheckResponse_SERVING
	if err := h.checker.Healthcheck(ctx); err != nil {
		status = grpchealth.HealthCheckResponse_NOT_SERVING
	}

	return &grpchealth.HealthCheckResponse{Status: status}, nil
}

// WithHealthService registers a grpc.health.v1 service on the server that
// reports SERVING/NOT_SERVING based on the given Healthchecker (typically
// an *app.App).
//
// Works with Envoy / k8s gRPC health probes. For HTTP-style probes use
// http2.WithHealthcheckFrom on the HTTP server instead.
func WithHealthService(checker lifecycle.Healthchecker) Option {
	return func(o *options) {
		o.healthChecker = checker
	}
}
