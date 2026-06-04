package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-lynx/lynx/plugins"
)

// MessageHandler defines the message processing function
type MessageHandler func(ctx context.Context, msg *primitive.MessageExt) error

// Producer RocketMQ producer interface
type Producer interface {
	// SendMessage sends a single message to the specified topic
	SendMessage(ctx context.Context, topic string, body []byte) error

	// SendMessageSync sends a message synchronously
	SendMessageSync(ctx context.Context, topic string, body []byte) (*primitive.SendResult, error)

	// SendMessageAsync sends a message asynchronously
	SendMessageAsync(ctx context.Context, topic string, body []byte) error

	// SendMessageWith sends a message by producer instance name
	SendMessageWith(ctx context.Context, producerName, topic string, body []byte) error

	// GetProducer gets the underlying producer client
	GetProducer(name string) (rocketmq.Producer, error)

	// IsProducerReady checks if the producer is ready
	IsProducerReady(name string) bool
}

// Consumer RocketMQ consumer interface
type Consumer interface {
	// Subscribe subscribes to topics and sets message handler
	Subscribe(ctx context.Context, topics []string, handler MessageHandler) error

	// SubscribeWith subscribes by consumer instance name
	SubscribeWith(ctx context.Context, consumerName string, topics []string, handler MessageHandler) error

	// GetConsumer gets the underlying consumer client
	GetConsumer(name string) (rocketmq.PushConsumer, error)

	// IsConsumerReady checks if the consumer is ready
	IsConsumerReady(name string) bool
}

// ClientInterface is the full RocketMQ client contract: producer, consumer, and lifecycle.
type ClientInterface interface {
	Producer
	Consumer

	InitializeResources(rt plugins.Runtime) error
	StartupTasks() error
	ShutdownTasks() error
	GetMetrics() *Metrics
}

// MetricsProvider monitoring metrics provider interface
type MetricsProvider interface {
	// GetStats returns a point-in-time snapshot of all counters.
	GetStats() Stats

	// Reset zeroes all in-process counters.
	Reset()
}

// HealthCheckerInterface runs periodic connectivity probes and tracks error counts.
type HealthCheckerInterface interface {
	Start()
	Stop()
	IsHealthy() bool
	GetLastCheck() time.Time
	GetErrorCount() int
}

// ConnectionManagerInterface manages the active broker connection and owns a HealthChecker.
type ConnectionManagerInterface interface {
	Start()
	Stop()
	IsConnected() bool
	GetHealthChecker() HealthCheckerInterface

	// ForceReconnect forces reconnection
	ForceReconnect()
}

// RetryHandlerInterface retry handler interface
type RetryHandlerInterface interface {
	// DoWithRetry executes operation with retry
	DoWithRetry(ctx context.Context, operation func() error) error
}

// GoroutinePoolInterface goroutine pool interface
type GoroutinePoolInterface interface {
	// Submit submits task
	Submit(task func())

	// Wait waits for completion
	Wait()
}
