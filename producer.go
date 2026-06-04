package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-lynx/lynx/log"
)

// SendMessage sends a single message to the specified topic
func (r *Client) SendMessage(ctx context.Context, topic string, body []byte) error {
	return r.SendMessageWith(ctx, r.defaultProducer, topic, body)
}

// SendMessageSync sends a message synchronously
func (r *Client) SendMessageSync(ctx context.Context, topic string, body []byte) (*primitive.SendResult, error) {
	return r.SendMessageSyncWith(ctx, r.defaultProducer, topic, body)
}

// SendMessageAsync sends a message asynchronously
func (r *Client) SendMessageAsync(ctx context.Context, topic string, body []byte) error {
	return r.SendMessageAsyncWith(ctx, r.defaultProducer, topic, body)
}

// SendMessageWith sends a message by producer instance name
func (r *Client) SendMessageWith(ctx context.Context, producerName, topic string, body []byte) error {
	start := time.Now()
	defer func() {
		r.metrics.RecordProducerLatency(time.Since(start))
	}()

	if err := validateTopic(topic); err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		return WrapError(err, "invalid topic")
	}

	if len(body) == 0 {
		r.metrics.IncrementProducerMessagesFailed()
		return ErrEmptyMessage
	}

	producer, err := r.GetProducer(producerName)
	if err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		return err
	}

	msg := primitive.NewMessage(topic, body)

	// SendSync is retried with backoff; the broker also performs its own
	// internal retries up to the producer's configured MaxRetries.
	err = r.retryHandler.DoWithRetry(ctx, func() error {
		_, err := producer.SendSync(ctx, msg)
		return err
	})

	if err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		log.Error("Failed to send RocketMQ message", "producer", producerName, "topic", topic, "error", err)
		return WrapError(err, "failed to send message")
	}

	r.metrics.IncrementProducerMessagesSent()
	log.Debug("Sent RocketMQ message", "producer", producerName, "topic", topic)
	return nil
}

// SendMessageSyncWith sends a message synchronously by producer instance name
func (r *Client) SendMessageSyncWith(ctx context.Context, producerName, topic string, body []byte) (*primitive.SendResult, error) {
	start := time.Now()
	defer func() {
		r.metrics.RecordProducerLatency(time.Since(start))
	}()

	if err := validateTopic(topic); err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		return nil, WrapError(err, "invalid topic")
	}

	if len(body) == 0 {
		r.metrics.IncrementProducerMessagesFailed()
		return nil, ErrEmptyMessage
	}

	producer, err := r.GetProducer(producerName)
	if err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		return nil, err
	}

	msg := primitive.NewMessage(topic, body)

	var result *primitive.SendResult
	err = r.retryHandler.DoWithRetry(ctx, func() error {
		var sendErr error
		result, sendErr = producer.SendSync(ctx, msg)
		return sendErr
	})

	if err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		log.Error("Failed to send RocketMQ message sync", "producer", producerName, "topic", topic, "error", err)
		return nil, WrapError(err, "failed to send message")
	}

	r.metrics.IncrementProducerMessagesSent()
	log.Debug("Sent RocketMQ message sync", "producer", producerName, "topic", topic, "msgId", result.MsgID)
	return result, nil
}

// SendMessageAsyncWith sends a message asynchronously by producer instance name
func (r *Client) SendMessageAsyncWith(ctx context.Context, producerName, topic string, body []byte) error {
	start := time.Now()
	defer func() {
		r.metrics.RecordProducerLatency(time.Since(start))
	}()

	if err := validateTopic(topic); err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		return WrapError(err, "invalid topic")
	}

	if len(body) == 0 {
		r.metrics.IncrementProducerMessagesFailed()
		return ErrEmptyMessage
	}

	producer, err := r.GetProducer(producerName)
	if err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		return err
	}

	msg := primitive.NewMessage(topic, body)

	// Async send: success/failure is reported via the callback, not the return
	// value (which only surfaces submission errors).
	err = producer.SendAsync(ctx, func(ctx context.Context, result *primitive.SendResult, err error) {
		if err != nil {
			r.metrics.IncrementProducerMessagesFailed()
			log.Error("Failed to send RocketMQ message async", "producer", producerName, "topic", topic, "error", err)
		} else {
			r.metrics.IncrementProducerMessagesSent()
			log.Debug("Sent RocketMQ message async", "producer", producerName, "topic", topic, "msgId", result.MsgID)
		}
	}, msg)

	if err != nil {
		r.metrics.IncrementProducerMessagesFailed()
		log.Error("Failed to send RocketMQ message async", "producer", producerName, "topic", topic, "error", err)
		return WrapError(err, "failed to send message async")
	}

	return nil
}

// GetProducer gets the underlying producer client
func (r *Client) GetProducer(name string) (rocketmq.Producer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultProducer
	}

	producer, exists := r.producers[name]
	if !exists {
		return nil, WrapError(ErrProducerNotFound, "producer not found: "+name)
	}

	return producer, nil
}

// IsProducerReady checks if the producer is ready
func (r *Client) IsProducerReady(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if name == "" {
		name = r.defaultProducer
	}

	producer, exists := r.producers[name]
	if !exists {
		return false
	}

	// The RocketMQ client exposes no readiness probe, so a registered, non-nil
	// producer is treated as ready.
	return producer != nil
}
