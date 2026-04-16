package grpc

import (
	"context"

	"google.golang.org/grpc/stats"
)

// composeStatsHandlers returns a single stats.Handler that fans events out to
// every handler in the slice. Returns nil for an empty slice and the single
// handler for a one-element slice (no wrapping overhead for the common case).
func composeStatsHandlers(handlers []stats.Handler) stats.Handler {
	switch len(handlers) {
	case 0:
		return nil
	case 1:
		return handlers[0]
	default:
		cloned := make([]stats.Handler, len(handlers))
		copy(cloned, handlers)

		return compositeStatsHandler(cloned)
	}
}

// compositeStatsHandler fans every lifecycle callback out to all wrapped
// handlers. Context returned by TagConn/TagRPC is threaded sequentially so
// each handler can layer its own context values.
type compositeStatsHandler []stats.Handler

// Compile-time check.
var _ stats.Handler = compositeStatsHandler(nil)

//nolint:fatcontext // context threading across handlers is the whole point of composite stats.Handler
func (c compositeStatsHandler) TagConn(
	ctx context.Context,
	info *stats.ConnTagInfo,
) context.Context {
	for _, h := range c {
		ctx = h.TagConn(ctx, info)
	}

	return ctx
}

func (c compositeStatsHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	for _, h := range c {
		h.HandleConn(ctx, s)
	}
}

//nolint:fatcontext // context threading across handlers is the whole point of composite stats.Handler
func (c compositeStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	for _, h := range c {
		ctx = h.TagRPC(ctx, info)
	}

	return ctx
}

func (c compositeStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	for _, h := range c {
		h.HandleRPC(ctx, s)
	}
}
