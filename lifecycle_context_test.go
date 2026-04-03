package rocketmq

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-lynx/lynx-rocketmq/conf"
	"github.com/go-lynx/lynx/plugins"
)

func TestConnectionManagerStartWithContextAndStop(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()

	cm := NewConnectionManager(NewMetrics(), []string{listener.Addr().String()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cm.StartWithContext(ctx); err != nil {
		t.Fatalf("StartWithContext failed: %v", err)
	}

	waitForCondition(t, time.Second, 10*time.Millisecond, func() bool {
		return cm.IsConnected() && cm.GetHealthChecker().IsHealthy()
	})

	cm.Stop()
	cm.Stop()

	select {
	case <-acceptDone:
	case <-time.After(time.Second):
		t.Fatal("listener accept did not complete")
	}
}

func TestConnectionManagerStartWithCanceledContext(t *testing.T) {
	cm := NewConnectionManager(NewMetrics(), []string{"127.0.0.1:9876"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := cm.StartWithContext(ctx); err == nil {
		t.Fatal("expected canceled context error")
	}
}

func TestClientContextLifecycleWithoutInstances(t *testing.T) {
	client := NewRocketMQClient()
	client.conf = &conf.RocketMQ{}

	if !client.IsContextAware() {
		t.Fatal("expected client to be context aware")
	}
	if !client.PluginProtocol().ContextLifecycle {
		t.Fatal("expected context lifecycle protocol to be enabled")
	}

	if err := client.StartContext(context.Background(), client); err != nil {
		t.Fatalf("StartContext failed: %v", err)
	}
	if got := client.Status(client); got != plugins.StatusActive {
		t.Fatalf("unexpected status after start: %v", got)
	}
	if client.ctx == nil || client.cancel == nil {
		t.Fatal("expected lifecycle context to be initialized")
	}

	if err := client.StopContext(context.Background(), client); err != nil {
		t.Fatalf("StopContext failed: %v", err)
	}
	if got := client.Status(client); got != plugins.StatusTerminated {
		t.Fatalf("unexpected status after stop: %v", got)
	}
	if client.ctx != nil || client.cancel != nil {
		t.Fatal("expected lifecycle context to be cleared")
	}
	if len(client.prodConnMgrs) != 0 || len(client.consConnMgrs) != 0 {
		t.Fatal("expected connection manager maps to be cleared")
	}
}

func TestClientShutdownTasksStopsConnectionManagers(t *testing.T) {
	client := NewRocketMQClient()
	client.conf = &conf.RocketMQ{}
	client.ensureLifecycleContext()

	producerMgr := NewConnectionManager(client.metrics, nil)
	consumerMgr := NewConnectionManager(client.metrics, nil)
	if err := producerMgr.StartWithContext(client.ctx); err != nil {
		t.Fatalf("producer manager start failed: %v", err)
	}
	if err := consumerMgr.StartWithContext(client.ctx); err != nil {
		t.Fatalf("consumer manager start failed: %v", err)
	}

	client.prodConnMgrs["producer"] = producerMgr
	client.consConnMgrs["consumer"] = consumerMgr
	client.defaultProducer = "producer"
	client.defaultConsumer = "consumer"

	if err := client.shutdownTasksContext(context.Background()); err != nil {
		t.Fatalf("shutdownTasksContext failed: %v", err)
	}
	if len(client.prodConnMgrs) != 0 || len(client.consConnMgrs) != 0 {
		t.Fatal("expected connection manager maps to be reset")
	}
	if client.defaultProducer != "" || client.defaultConsumer != "" {
		t.Fatal("expected default producer/consumer names to be reset")
	}
	if client.ctx != nil || client.cancel != nil {
		t.Fatal("expected lifecycle context to be cleared")
	}

	producerMgr.Stop()
	consumerMgr.Stop()
}

func waitForCondition(t *testing.T, timeout, interval time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}

	t.Fatal("condition was not satisfied before timeout")
}
