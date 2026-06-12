//nolint:testpackage // white-box test: needs access to unexported test helpers.
package nats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeProcessor[I any] struct{}

func (fakeProcessor[I]) Process(_ context.Context, _ Message[I]) error { return nil }

func TestNewPublisher_emptyURL(t *testing.T) {
	t.Parallel()

	_, err := NewPublisher(t.Context(), Config{})
	require.ErrorIs(t, err, ErrEmptyURL)
}

func TestNewSubscriber_nilProcessor(t *testing.T) {
	t.Parallel()

	_, err := NewSubscriber[string](
		Config{URL: "nats://x"},
		"foo",
		nil,
		SubscriberHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrNilProcessor)
}

func TestNewSubscriber_emptySubjectFilter(t *testing.T) {
	t.Parallel()

	_, err := NewSubscriber[string](
		Config{URL: "nats://x"},
		"",
		fakeProcessor[string]{},
		SubscriberHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrEmptySubjectFilter)
}

func TestNewSubscriber_emptyURL(t *testing.T) {
	t.Parallel()

	_, err := NewSubscriber[string](
		Config{},
		"foo",
		fakeProcessor[string]{},
		SubscriberHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrEmptyURL)
}

func TestNewJetStreamConsumer_nilProcessor(t *testing.T) {
	t.Parallel()

	_, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: "nats://x"},
		"stream",
		"durable",
		nil,
		JSConsumerHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrNilProcessor)
}

func TestNewJetStreamConsumer_emptyStream(t *testing.T) {
	t.Parallel()

	_, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: "nats://x"},
		"",
		"durable",
		fakeProcessor[string]{},
		JSConsumerHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrEmptyStream)
}

func TestNewJetStreamConsumer_emptyDurable(t *testing.T) {
	t.Parallel()

	_, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{URL: "nats://x"},
		"stream",
		"",
		fakeProcessor[string]{},
		JSConsumerHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrEmptyDurable)
}

func TestNewJetStreamConsumer_emptyURL(t *testing.T) {
	t.Parallel()

	_, err := NewJetStreamConsumer[string](
		t.Context(),
		Config{},
		"stream",
		"durable",
		fakeProcessor[string]{},
		JSConsumerHandlers[string]{},
	)
	require.ErrorIs(t, err, ErrEmptyURL)
}

func TestSubscriber_ShutdownBeforeRun(t *testing.T) {
	t.Parallel()

	s := &Subscriber[string]{}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	assert.NoError(t, s.Shutdown(ctx))
}

func TestJetStreamConsumer_ShutdownBeforeRun(t *testing.T) {
	t.Parallel()

	c := &JetStreamConsumer[string]{}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	assert.NoError(t, c.Shutdown(ctx))
}
