package rocketmq

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// newIsolatedMetrics creates a Metrics instance backed by a fresh Prometheus
// registry to avoid duplicate-registration conflicts between test functions.
func newIsolatedMetrics() *Metrics {
	return newMetricsWithRegisterer(prometheus.NewRegistry())
}

func TestMetricsProducerCounters(t *testing.T) {
	m := newIsolatedMetrics()

	m.IncrementProducerMessagesSent()
	m.IncrementProducerMessagesSent()
	m.IncrementProducerMessagesFailed()

	s := m.GetStats()
	if s.ProducerSent != 2 {
		t.Fatalf("expected 2 producer sent, got %d", s.ProducerSent)
	}
	if s.ProducerFailed != 1 {
		t.Fatalf("expected 1 producer failed, got %d", s.ProducerFailed)
	}
}

func TestMetricsConsumerCounters(t *testing.T) {
	m := newIsolatedMetrics()

	m.IncrementConsumerMessagesReceived()
	m.IncrementConsumerMessagesFailed()
	m.IncrementConsumerMessagesFailed()

	s := m.GetStats()
	if s.ConsumerReceived != 1 {
		t.Fatalf("expected 1 consumer received, got %d", s.ConsumerReceived)
	}
	if s.ConsumerFailed != 2 {
		t.Fatalf("expected 2 consumer failed, got %d", s.ConsumerFailed)
	}
}

func TestMetricsLatencyRecording(t *testing.T) {
	m := newIsolatedMetrics()

	m.RecordProducerLatency(5 * time.Millisecond)
	m.RecordConsumerLatency(10 * time.Millisecond)

	s := m.GetStats()
	if s.ProducerLatencyNs != int64(5*time.Millisecond) {
		t.Fatalf("unexpected producer latency: %d ns", s.ProducerLatencyNs)
	}
	if s.ConsumerLatencyNs != int64(10*time.Millisecond) {
		t.Fatalf("unexpected consumer latency: %d ns", s.ConsumerLatencyNs)
	}
}

func TestMetricsConnectionAndHealth(t *testing.T) {
	m := newIsolatedMetrics()

	m.IncrementConnectionErrors()
	m.IncrementReconnectionCount()
	m.IncrementHealthCheckCount()
	m.IncrementHealthCheckErrors()
	m.SetHealthy(true)
	m.UpdateLastHealthCheck()

	s := m.GetStats()
	if s.ConnectionErrors != 1 {
		t.Fatalf("expected 1 connection error, got %d", s.ConnectionErrors)
	}
	if s.ReconnectionCount != 1 {
		t.Fatalf("expected 1 reconnection, got %d", s.ReconnectionCount)
	}
	if s.HealthCheckCount != 1 {
		t.Fatalf("expected 1 health check, got %d", s.HealthCheckCount)
	}
	if s.HealthCheckErrors != 1 {
		t.Fatalf("expected 1 health check error, got %d", s.HealthCheckErrors)
	}
	if !s.IsHealthy {
		t.Fatal("expected IsHealthy=true")
	}
	if s.LastReconnectTime.IsZero() {
		t.Fatal("expected LastReconnectTime to be set")
	}
	if s.LastHealthCheck.IsZero() {
		t.Fatal("expected LastHealthCheck to be set")
	}
}

func TestMetricsReset(t *testing.T) {
	m := newIsolatedMetrics()

	m.IncrementProducerMessagesSent()
	m.IncrementConsumerMessagesReceived()
	m.SetHealthy(true)
	m.Reset()

	s := m.GetStats()
	if s.ProducerSent != 0 {
		t.Fatalf("expected 0 after reset, got %d", s.ProducerSent)
	}
	if s.ConsumerReceived != 0 {
		t.Fatalf("expected 0 after reset, got %d", s.ConsumerReceived)
	}
	if s.IsHealthy {
		t.Fatal("expected IsHealthy=false after reset")
	}
}
