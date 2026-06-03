package rocketmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-lynx/lynx-rocketmq/conf"
)

// buildTestConsumerCallback builds a consume callback exactly as SubscribeWith
// does, so we can unit-test panic recovery and error handling without a live broker.
func buildTestConsumerCallback(
	r *Client,
	consumerName string,
	handler MessageHandler,
) func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (result consumer.ConsumeResult, cbErr error) {
		for _, msg := range msgs {
			start := time.Now()

			func() {
				defer func() {
					if rec := recover(); rec != nil {
						r.metrics.IncrementConsumerMessagesFailed()
						result = consumer.ConsumeRetryLater
						cbErr = errors.New("handler panic")
					}
				}()

				if err := handler(ctx, msg); err != nil {
					r.metrics.IncrementConsumerMessagesFailed()
					result = consumer.ConsumeRetryLater
					cbErr = err
					return
				}

				r.metrics.RecordConsumerLatency(time.Since(start))
				r.metrics.IncrementConsumerMessagesReceived()
			}()

			if cbErr != nil {
				return result, cbErr
			}
		}
		return consumer.ConsumeSuccess, nil
	}
}

func TestConsumerCallbackPanicRecovery(t *testing.T) {
	client := NewRocketMQClient()
	client.metrics = newIsolatedMetrics()

	panicHandler := func(_ context.Context, _ *primitive.MessageExt) error {
		panic("simulated handler panic")
	}

	cb := buildTestConsumerCallback(client, "test", panicHandler)
	msg := &primitive.MessageExt{Message: primitive.Message{Topic: "test-topic"}}

	result, err := cb(context.Background(), msg)

	if result != consumer.ConsumeRetryLater {
		t.Fatalf("expected ConsumeRetryLater on panic, got %v", result)
	}
	if err == nil {
		t.Fatal("expected error returned on panic")
	}

	s := client.metrics.GetStats()
	if s.ConsumerFailed != 1 {
		t.Fatalf("expected 1 consumer failure metric, got %d", s.ConsumerFailed)
	}
	if s.ConsumerReceived != 0 {
		t.Fatalf("expected 0 consumer received metric on panic, got %d", s.ConsumerReceived)
	}
}

func TestConsumerCallbackHandlerError(t *testing.T) {
	client := NewRocketMQClient()
	client.metrics = newIsolatedMetrics()

	errHandler := func(_ context.Context, _ *primitive.MessageExt) error {
		return errors.New("processing error")
	}

	cb := buildTestConsumerCallback(client, "test", errHandler)
	msg := &primitive.MessageExt{Message: primitive.Message{Topic: "test-topic"}}

	result, err := cb(context.Background(), msg)

	if result != consumer.ConsumeRetryLater {
		t.Fatalf("expected ConsumeRetryLater on handler error, got %v", result)
	}
	if err == nil {
		t.Fatal("expected error returned on handler error")
	}

	s := client.metrics.GetStats()
	if s.ConsumerFailed != 1 {
		t.Fatalf("expected 1 consumer failure metric, got %d", s.ConsumerFailed)
	}
}

func TestConsumerCallbackSuccess(t *testing.T) {
	client := NewRocketMQClient()
	client.metrics = newIsolatedMetrics()

	okHandler := func(_ context.Context, _ *primitive.MessageExt) error {
		return nil
	}

	cb := buildTestConsumerCallback(client, "test", okHandler)
	msg := &primitive.MessageExt{Message: primitive.Message{Topic: "test-topic"}}

	result, err := cb(context.Background(), msg)

	if result != consumer.ConsumeSuccess {
		t.Fatalf("expected ConsumeSuccess, got %v", result)
	}
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	s := client.metrics.GetStats()
	if s.ConsumerReceived != 1 {
		t.Fatalf("expected 1 consumer received metric, got %d", s.ConsumerReceived)
	}
	if s.ConsumerFailed != 0 {
		t.Fatalf("expected 0 consumer failure metric, got %d", s.ConsumerFailed)
	}
}

func TestRetryHandlerExponentialBackoff(t *testing.T) {
	// Use very short durations so the test completes quickly.
	handler := NewRetryHandler(RetryConfig{
		MaxRetries:  3,
		BackoffTime: 1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	})

	calls := 0
	err := handler.DoWithRetry(context.Background(), func() error {
		calls++
		if calls < 3 {
			return errors.New("transient error")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryHandlerMaxRetriesExceeded(t *testing.T) {
	handler := NewRetryHandler(RetryConfig{
		MaxRetries:  2,
		BackoffTime: 1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	})

	calls := 0
	err := handler.DoWithRetry(context.Background(), func() error {
		calls++
		return errors.New("permanent error")
	})

	if err == nil {
		t.Fatal("expected error after max retries")
	}
	// attempts = MaxRetries + 1 (initial + retries)
	if calls != 3 {
		t.Fatalf("expected 3 calls (1 initial + 2 retries), got %d", calls)
	}
}

func TestRetryHandlerContextCancellation(t *testing.T) {
	handler := NewRetryHandler(RetryConfig{
		MaxRetries:  10,
		BackoffTime: 100 * time.Millisecond,
		MaxBackoff:  time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	// Cancel after the first attempt.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := handler.DoWithRetry(ctx, func() error {
		calls++
		return errors.New("transient")
	})

	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestGracefulShutdownNoInstances(t *testing.T) {
	client := NewRocketMQClient()
	client.conf = &conf.RocketMQ{}
	client.ensureLifecycleContext()

	// Shutdown must complete without hanging or panicking even with no
	// producers/consumers registered.
	done := make(chan error, 1)
	go func() {
		done <- client.shutdownTasksContext(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error on shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown timed out")
	}

	if client.ctx != nil || client.cancel != nil {
		t.Fatal("expected lifecycle context to be cleared after shutdown")
	}
}
