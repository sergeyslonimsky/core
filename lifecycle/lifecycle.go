// Package lifecycle defines the three core interfaces that every component
// registered with app.App must implement in some combination: Resource for
// stateful holders of connections or goroutines, Runner for components whose
// primary contract is long-running background work, and Healthchecker for
// readiness reporting.
//
// The package has no dependencies beyond the standard library and is designed
// as a leaf package so every other core/* package can depend on it without
// creating import cycles.
//
// # Resource vs Runner
//
// The distinction between Resource and Runner is about contract visibility,
// not about the presence of a background goroutine:
//
//   - Resource hides any background work as an implementation detail. The
//     caller uses the component by invoking its methods on demand. Examples:
//     redis client, sql DB, kafka producer, otel SDK, rabbitmq publisher.
//
//   - Runner exposes background work as its primary contract — Run blocks
//     until the context is cancelled. Callers register runners with app.App
//     and block on app.Run(). Examples: HTTP/2 server, gRPC server, kafka
//     consumer, rabbitmq consumer host.
//
// Runner embeds Resource, so every Runner is also a Resource. app.App uses a
// type switch on Runner first, then Resource, to dispatch components to the
// correct lifecycle pool without double-registration.
package lifecycle

import "context"

// Resource is a stateful holder of connections, pools, or background
// goroutines that does not block the caller. It participates in graceful
// shutdown via Shutdown.
//
// Implementations must make Shutdown idempotent and safe to call once — the
// app.App orchestrator calls it exactly once during shutdown. Shutdown must
// honor the provided context: if ctx is cancelled before the component
// finishes releasing resources, the implementation should force-close
// (drop connections, abandon in-flight work) and return ctx.Err().
//
// Shutdown MUST NOT be called concurrently with other methods on the same
// component; callers that need concurrent access must coordinate externally.
type Resource interface {
	// Shutdown releases any resources held by the component. It should be
	// called exactly once, typically by app.App during graceful shutdown.
	// The provided context bounds the total time allowed for cleanup — on
	// cancellation the implementation should abort gracefully and return
	// ctx.Err().
	Shutdown(ctx context.Context) error
}

// Runner is a Resource that additionally performs long-running blocking work
// via Run. Run blocks until ctx is cancelled or a fatal error occurs, then
// returns. Typical implementations: HTTP/gRPC servers, kafka/rabbitmq
// consumers.
//
// After Run returns, app.App will also call Shutdown to release any remaining
// resources (accept loops, listeners, client connections). Implementations
// must tolerate Shutdown being called after Run has already returned.
//
// Run and Shutdown may be called from different goroutines. Run is expected
// to unblock promptly when its context is cancelled — relying on Shutdown to
// force-stop is a sign of a design bug.
type Runner interface {
	Resource

	// Run blocks until ctx is cancelled (normal termination) or until a
	// fatal error occurs (in which case Run returns the error). Run must
	// NOT return a non-nil error on ctx cancellation — context.Canceled
	// is treated as success by app.App.
	Run(ctx context.Context) error
}

// Healthchecker is an optional interface a Resource or Runner may implement
// to report readiness. app.App.Healthcheck aggregates all registered
// components that implement this interface and returns errors.Join of any
// failures.
//
// Healthcheck is invoked on every /readyz request, so implementations should
// be cheap: a cached Ping result or a lightweight connection check. If the
// check requires network I/O, it MUST honor ctx deadlines — a slow readiness
// probe can turn a transient degradation into a cascading outage.
//
// Returning nil means the component is ready to serve. Returning a non-nil
// error means the component is not ready; the error will be surfaced in the
// /readyz response body.
type Healthchecker interface {
	// Healthcheck returns nil if the component is ready, or an error
	// describing the unhealthy state. Must respect ctx deadlines.
	Healthcheck(ctx context.Context) error
}
