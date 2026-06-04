package rocketmq

import (
	"context"
	"time"
)

// RetryConfig defines retry configuration
type RetryConfig struct {
	MaxRetries  int
	BackoffTime time.Duration
	MaxBackoff  time.Duration
}

// RetryHandler handles retry logic
type RetryHandler struct {
	config RetryConfig
}

// NewRetryHandler creates a new RetryHandler
func NewRetryHandler(config RetryConfig) *RetryHandler {
	return &RetryHandler{
		config: config,
	}
}

// DoWithRetry executes operation with retry
func (r *RetryHandler) DoWithRetry(ctx context.Context, operation func() error) error {
	var lastErr error
	backoff := r.config.BackoffTime

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		// Last attempt: return the error without sleeping again.
		if attempt == r.config.MaxRetries {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Double the backoff each round, capped at MaxBackoff.
		backoff *= 2
		if backoff > r.config.MaxBackoff {
			backoff = r.config.MaxBackoff
		}
	}

	return WrapError(lastErr, "max retries exceeded")
}

// GetRetryConfig returns the retry configuration
func (r *RetryHandler) GetRetryConfig() RetryConfig {
	return r.config
}

// SetRetryConfig sets the retry configuration
func (r *RetryHandler) SetRetryConfig(config RetryConfig) {
	r.config = config
}
