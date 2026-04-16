package rabbitmq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergeyslonimsky/core/rabbitmq"
)

// TestPublishOpts is a sanity check on the PublishOpts struct fields.
//
// Constructor tests for Publisher require a live broker (NewPublisher
// dials in the constructor); see e2e_integration_test.go for those.
func TestPublishOpts(t *testing.T) {
	t.Parallel()

	opts := rabbitmq.PublishOpts{
		ContentType: "application/json",
		MessageID:   "test-id-123",
		AppID:       "test-app",
		UserID:      "test-user",
		Priority:    5,
		Type:        "test-type",
	}

	assert.Equal(t, "application/json", opts.ContentType)
	assert.Equal(t, "test-id-123", opts.MessageID)
	assert.Equal(t, "test-app", opts.AppID)
	assert.Equal(t, "test-user", opts.UserID)
	assert.Equal(t, uint8(5), opts.Priority)
	assert.Equal(t, "test-type", opts.Type)
}
