// Package rocketmq provides a production-grade Apache RocketMQ client plugin for the
// go-lynx framework. It supports multiple named producer and consumer instances,
// automatic reconnection, exponential-backoff retry, Prometheus metrics, panic
// recovery in message handlers, and graceful shutdown that waits for in-flight
// messages to complete before stopping.
package rocketmq

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics records RocketMQ operational counters and latency histograms and
// exposes them via a Prometheus registry.
type Metrics struct {
	mu sync.RWMutex

	// in-process counters (also reflected in Prometheus)
	producerMessagesSent   int64
	producerMessagesFailed int64
	producerLatency        int64 // latest sample, nanoseconds

	consumerMessagesReceived int64
	consumerMessagesFailed   int64
	consumerLatency          int64 // latest sample, nanoseconds

	connectionErrors  int64
	reconnectionCount int64
	lastReconnectTime time.Time

	healthCheckCount  int64
	healthCheckErrors int64
	lastHealthCheck   time.Time
	isHealthy         int32

	// Prometheus instruments
	promProducerSent     prometheus.Counter
	promProducerFailed   prometheus.Counter
	promProducerLatency  prometheus.Histogram
	promConsumerReceived prometheus.Counter
	promConsumerFailed   prometheus.Counter
	promConsumerLatency  prometheus.Histogram
	promConnErrors       prometheus.Counter
	promReconnections    prometheus.Counter
	promHealthErrors     prometheus.Counter
}

// NewMetrics creates a Metrics instance and registers Prometheus instruments
// under the "lynx_rocketmq" namespace. Duplicate registrations (e.g. when the
// plugin is instantiated multiple times in a test suite) are silently ignored:
// the already-registered collector is reused.
func NewMetrics() *Metrics {
	return newMetricsWithRegisterer(prometheus.DefaultRegisterer)
}

// newMetricsWithRegisterer is the internal constructor used by tests to supply
// an isolated registry and avoid duplicate-registration conflicts.
func newMetricsWithRegisterer(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		lastHealthCheck: time.Now(),
	}

	m.promProducerSent = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "producer",
		Name:      "messages_sent_total",
		Help:      "Total number of messages successfully sent by the producer.",
	}))
	m.promProducerFailed = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "producer",
		Name:      "messages_failed_total",
		Help:      "Total number of messages that failed to be sent by the producer.",
	}))
	m.promProducerLatency = mustOrExisting[prometheus.Histogram](reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "producer",
		Name:      "send_duration_seconds",
		Help:      "Histogram of producer send latency in seconds.",
		Buckets:   prometheus.DefBuckets,
	}))
	m.promConsumerReceived = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "consumer",
		Name:      "messages_received_total",
		Help:      "Total number of messages successfully processed by the consumer.",
	}))
	m.promConsumerFailed = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "consumer",
		Name:      "messages_failed_total",
		Help:      "Total number of messages that failed processing and will be retried.",
	}))
	m.promConsumerLatency = mustOrExisting[prometheus.Histogram](reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "consumer",
		Name:      "process_duration_seconds",
		Help:      "Histogram of consumer message-processing latency in seconds.",
		Buckets:   prometheus.DefBuckets,
	}))
	m.promConnErrors = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "connection",
		Name:      "errors_total",
		Help:      "Total number of connection errors encountered.",
	}))
	m.promReconnections = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "connection",
		Name:      "reconnections_total",
		Help:      "Total number of reconnection attempts.",
	}))
	m.promHealthErrors = mustOrExisting[prometheus.Counter](reg, prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_rocketmq",
		Subsystem: "health",
		Name:      "check_errors_total",
		Help:      "Total number of failed health checks.",
	}))

	return m
}

// mustOrExisting registers c with reg. If the metric is already registered the
// existing collector is returned instead of panicking.
func mustOrExisting[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
		}
		panic(err)
	}
	return c
}

// IncrementProducerMessagesSent increments the successful producer send counter.
func (m *Metrics) IncrementProducerMessagesSent() {
	atomic.AddInt64(&m.producerMessagesSent, 1)
	m.promProducerSent.Inc()
}

// IncrementProducerMessagesFailed increments the failed producer send counter.
func (m *Metrics) IncrementProducerMessagesFailed() {
	atomic.AddInt64(&m.producerMessagesFailed, 1)
	m.promProducerFailed.Inc()
}

// RecordProducerLatency records a producer send latency observation.
func (m *Metrics) RecordProducerLatency(duration time.Duration) {
	atomic.StoreInt64(&m.producerLatency, int64(duration))
	m.promProducerLatency.Observe(duration.Seconds())
}

// IncrementConsumerMessagesReceived increments the successful consumer receive counter.
func (m *Metrics) IncrementConsumerMessagesReceived() {
	atomic.AddInt64(&m.consumerMessagesReceived, 1)
	m.promConsumerReceived.Inc()
}

// IncrementConsumerMessagesFailed increments the failed consumer processing counter.
func (m *Metrics) IncrementConsumerMessagesFailed() {
	atomic.AddInt64(&m.consumerMessagesFailed, 1)
	m.promConsumerFailed.Inc()
}

// RecordConsumerLatency records a consumer message-processing latency observation.
func (m *Metrics) RecordConsumerLatency(duration time.Duration) {
	atomic.StoreInt64(&m.consumerLatency, int64(duration))
	m.promConsumerLatency.Observe(duration.Seconds())
}

// IncrementConnectionErrors increments the connection error counter.
func (m *Metrics) IncrementConnectionErrors() {
	atomic.AddInt64(&m.connectionErrors, 1)
	m.promConnErrors.Inc()
}

// IncrementReconnectionCount increments the reconnection counter and records the timestamp.
func (m *Metrics) IncrementReconnectionCount() {
	atomic.AddInt64(&m.reconnectionCount, 1)
	m.promReconnections.Inc()
	m.mu.Lock()
	m.lastReconnectTime = time.Now()
	m.mu.Unlock()
}

// IncrementHealthCheckCount increments the health-check invocation counter.
func (m *Metrics) IncrementHealthCheckCount() {
	atomic.AddInt64(&m.healthCheckCount, 1)
}

// IncrementHealthCheckErrors increments the health-check error counter.
func (m *Metrics) IncrementHealthCheckErrors() {
	atomic.AddInt64(&m.healthCheckErrors, 1)
	m.promHealthErrors.Inc()
}

// SetHealthy sets the binary health flag (1 = healthy, 0 = unhealthy).
func (m *Metrics) SetHealthy(healthy bool) {
	if healthy {
		atomic.StoreInt32(&m.isHealthy, 1)
	} else {
		atomic.StoreInt32(&m.isHealthy, 0)
	}
}

// UpdateLastHealthCheck records the current time as the last health-check timestamp.
func (m *Metrics) UpdateLastHealthCheck() {
	m.mu.Lock()
	m.lastHealthCheck = time.Now()
	m.mu.Unlock()
}

// Stats is a snapshot of the current metric values.
type Stats struct {
	ProducerSent      int64
	ProducerFailed    int64
	ProducerLatencyNs int64
	ConsumerReceived  int64
	ConsumerFailed    int64
	ConsumerLatencyNs int64
	ConnectionErrors  int64
	ReconnectionCount int64
	LastReconnectTime time.Time
	HealthCheckCount  int64
	HealthCheckErrors int64
	LastHealthCheck   time.Time
	IsHealthy         bool
}

// GetStats returns a point-in-time snapshot of all counters.
func (m *Metrics) GetStats() Stats {
	m.mu.RLock()
	lastReconnect := m.lastReconnectTime
	lastCheck := m.lastHealthCheck
	m.mu.RUnlock()

	return Stats{
		ProducerSent:      atomic.LoadInt64(&m.producerMessagesSent),
		ProducerFailed:    atomic.LoadInt64(&m.producerMessagesFailed),
		ProducerLatencyNs: atomic.LoadInt64(&m.producerLatency),
		ConsumerReceived:  atomic.LoadInt64(&m.consumerMessagesReceived),
		ConsumerFailed:    atomic.LoadInt64(&m.consumerMessagesFailed),
		ConsumerLatencyNs: atomic.LoadInt64(&m.consumerLatency),
		ConnectionErrors:  atomic.LoadInt64(&m.connectionErrors),
		ReconnectionCount: atomic.LoadInt64(&m.reconnectionCount),
		LastReconnectTime: lastReconnect,
		HealthCheckCount:  atomic.LoadInt64(&m.healthCheckCount),
		HealthCheckErrors: atomic.LoadInt64(&m.healthCheckErrors),
		LastHealthCheck:   lastCheck,
		IsHealthy:         atomic.LoadInt32(&m.isHealthy) == 1,
	}
}

// Reset zeroes all in-process counters. Prometheus counters are cumulative and
// are intentionally not reset.
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	atomic.StoreInt64(&m.producerMessagesSent, 0)
	atomic.StoreInt64(&m.producerMessagesFailed, 0)
	atomic.StoreInt64(&m.producerLatency, 0)
	atomic.StoreInt64(&m.consumerMessagesReceived, 0)
	atomic.StoreInt64(&m.consumerMessagesFailed, 0)
	atomic.StoreInt64(&m.consumerLatency, 0)
	atomic.StoreInt64(&m.connectionErrors, 0)
	atomic.StoreInt64(&m.reconnectionCount, 0)
	atomic.StoreInt64(&m.healthCheckCount, 0)
	atomic.StoreInt64(&m.healthCheckErrors, 0)
	atomic.StoreInt32(&m.isHealthy, 0)

	m.lastReconnectTime = time.Time{}
	m.lastHealthCheck = time.Now()
}
