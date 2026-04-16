package di

import (
	"context"
	"errors"
	"fmt"
)

// Container bundles a typed Config and a typed Services graph produced by
// the application. It is a thin convenience wrapper over the two-step
// bootstrap pattern: initConfig, then initServices.
//
// Container does NOT track cleanup for the happy path. Every component
// that holds resources must implement lifecycle.Resource (see
// github.com/sergeyslonimsky/core/lifecycle) and be registered with
// app.App — the container is only responsible for construction, not
// runtime teardown.
//
// Typical usage:
//
//	container, err := di.NewContainer(ctx,
//	    myapp.NewConfig,
//	    myapp.NewServices,
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	cfg, svc := container.Config, container.Services
//
//	a := app.New()
//	svc.RegisterResources(a)       // add Resources (db, redis, ...)
//	a.Add(svc.HTTPServer)           // add Runners
//	log.Fatal(a.Run())
type Container[C, S any] struct {
	Config   C
	Services S
}

// ServicesInit is the signature expected by NewContainer. It builds the
// Services graph and MAY return a cleanup closure that releases any
// resources constructed so far. If err is non-nil, NewContainer invokes
// the returned cleanup (if any) before propagating the error. On success
// the cleanup closure is ignored — runtime teardown is handled by app.App
// via lifecycle.Resource.
//
// Return a nil cleanup when the initializer has nothing to release on
// partial construction.
type ServicesInit[C, S any] func(
	ctx context.Context,
	cfg C,
) (services S, cleanup func(context.Context) error, err error)

// NewContainer invokes initConfig first, then threads the resulting config
// into initServices. Returns the populated container or the first error
// encountered.
//
// Failure semantics: if initServices returns an error alongside a non-nil
// cleanup, the cleanup is invoked before NewContainer returns. Cleanup
// errors are joined with the init error via errors.Join for visibility.
//
// Use ServicesInit for initServices when you construct resources (db,
// redis, otel providers) that need to be released on partial-failure
// startup paths.
func NewContainer[C, S any](
	ctx context.Context,
	initConfig func(context.Context) (C, error),
	initServices ServicesInit[C, S],
) (*Container[C, S], error) {
	cfg, err := initConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("init config: %w", err)
	}

	services, cleanup, err := initServices(ctx, cfg)
	if err != nil {
		initErr := fmt.Errorf("init services: %w", err)

		if cleanup != nil {
			if cleanupErr := cleanup(ctx); cleanupErr != nil {
				return nil, errors.Join(
					initErr,
					fmt.Errorf("cleanup after init failure: %w", cleanupErr),
				)
			}
		}

		return nil, initErr
	}

	return &Container[C, S]{Config: cfg, Services: services}, nil
}
