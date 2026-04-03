package rocketmq

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
)

const nameServerProbeTimeout = 3 * time.Second

// ConnectionManager manages connection health and reconnection
type ConnectionManager struct {
	metrics         *Metrics
	healthChecker   *HealthChecker
	nameServerAddrs []string
	mu              sync.RWMutex
	connected       bool
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NewConnectionManager creates a new connection manager.
// If nameServerAddrs is non-empty, checkConnection will probe RocketMQ by TCP dial to one of the NameServer addresses.
func NewConnectionManager(metrics *Metrics, nameServerAddrs []string) *ConnectionManager {
	cm := &ConnectionManager{
		metrics:         metrics,
		nameServerAddrs: nameServerAddrs,
	}
	cm.healthChecker = NewHealthChecker(metrics, cm)
	return cm
}

// Start starts the connection manager
func (cm *ConnectionManager) Start() {
	_ = cm.StartWithContext(context.Background())
}

// StartWithContext starts the connection manager with a lifecycle context.
func (cm *ConnectionManager) StartWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cm.mu.Lock()
	if cm.cancel != nil {
		cm.mu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	cm.cancel = cancel
	cm.mu.Unlock()

	if err := cm.checkConnectionContext(ctx); err != nil {
		cancel()
		cm.mu.Lock()
		cm.cancel = nil
		cm.mu.Unlock()
		return err
	}

	cm.healthChecker.StartWithContext(runCtx)
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		cm.run(runCtx)
	}()

	return nil
}

// Stop stops the connection manager
func (cm *ConnectionManager) Stop() {
	cm.mu.Lock()
	cancel := cm.cancel
	cm.cancel = nil
	cm.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	cm.healthChecker.Stop()
	cm.wg.Wait()
}

// IsConnected checks if connected
func (cm *ConnectionManager) IsConnected() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.connected
}

// GetHealthChecker gets health checker
func (cm *ConnectionManager) GetHealthChecker() HealthCheckerInterface {
	return cm.healthChecker
}

// ForceReconnect forces reconnection
func (cm *ConnectionManager) ForceReconnect() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.connected = false
	cm.metrics.IncrementReconnectionCount()
	log.Info("Forced reconnection")
}

// run runs the connection manager loop
func (cm *ConnectionManager) run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := cm.checkConnectionContext(ctx); err != nil {
				log.Debug("RocketMQ connection probe failed", "addrs", cm.nameServerAddrs, "error", err)
			}
		}
	}
}

// checkConnectionContext checks connection health by probing NameServer when addresses are configured.
func (cm *ConnectionManager) checkConnectionContext(ctx context.Context) error {
	if len(cm.nameServerAddrs) == 0 {
		cm.mu.Lock()
		cm.connected = true
		cm.mu.Unlock()
		return nil
	}

	dialer := &net.Dialer{Timeout: nameServerProbeTimeout}
	var lastErr error
	for _, addr := range cm.nameServerAddrs {
		if err := ctx.Err(); err != nil {
			cm.mu.Lock()
			cm.connected = false
			cm.mu.Unlock()
			return err
		}

		probeCtx, cancel := context.WithTimeout(ctx, nameServerProbeTimeout)
		conn, err := dialer.DialContext(probeCtx, "tcp", addr)
		cancel()
		if err == nil {
			_ = conn.Close()
			cm.mu.Lock()
			cm.connected = true
			cm.mu.Unlock()
			return nil
		}
		lastErr = err
	}
	cm.mu.Lock()
	cm.connected = false
	cm.mu.Unlock()
	if lastErr == nil {
		lastErr = fmt.Errorf("rocketmq nameserver probe failed")
	}
	return lastErr
}

// HealthChecker performs health checks
type HealthChecker struct {
	metrics    *Metrics
	connMgr    *ConnectionManager
	mu         sync.RWMutex
	healthy    bool
	lastCheck  time.Time
	errorCount int64
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewHealthChecker creates a new health checker. When connMgr is non-nil and has NameServer addrs,
// healthy is derived from connMgr.IsConnected(); otherwise from error count heuristic.
func NewHealthChecker(metrics *Metrics, connMgr *ConnectionManager) *HealthChecker {
	return &HealthChecker{
		metrics:   metrics,
		connMgr:   connMgr,
		lastCheck: time.Now(),
	}
}

// Start starts health check
func (hc *HealthChecker) Start() {
	hc.StartWithContext(context.Background())
}

// StartWithContext starts health checks with a lifecycle context.
func (hc *HealthChecker) StartWithContext(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}

	hc.mu.Lock()
	if hc.cancel != nil {
		hc.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	hc.cancel = cancel
	hc.mu.Unlock()

	hc.wg.Add(1)
	go func() {
		defer hc.wg.Done()
		hc.run(runCtx)
	}()
}

// Stop stops health check
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	cancel := hc.cancel
	hc.cancel = nil
	hc.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	hc.wg.Wait()
}

// IsHealthy checks if healthy
func (hc *HealthChecker) IsHealthy() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.healthy
}

// GetLastCheck gets last check time
func (hc *HealthChecker) GetLastCheck() time.Time {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.lastCheck
}

// GetErrorCount gets error count
func (hc *HealthChecker) GetErrorCount() int {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return int(hc.errorCount)
}

// run runs the health check loop
func (hc *HealthChecker) run(ctx context.Context) {
	hc.performHealthCheck(ctx)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hc.performHealthCheck(ctx)
		}
	}
}

// performHealthCheck performs a health check.
// When ConnectionManager has NameServer addrs, health is based on actual TCP probe (IsConnected).
// Otherwise falls back to error-count heuristic.
func (hc *HealthChecker) performHealthCheck(ctx context.Context) {
	if hc.connMgr != nil {
		if err := hc.connMgr.checkConnectionContext(ctx); err != nil {
			log.Debug("RocketMQ health checker connection probe failed", "error", err)
		}
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.metrics.IncrementHealthCheckCount()
	hc.lastCheck = time.Now()
	hc.metrics.UpdateLastHealthCheck()

	if hc.connMgr != nil && len(hc.connMgr.nameServerAddrs) > 0 {
		hc.healthy = hc.connMgr.IsConnected()
	} else if hc.errorCount < 5 {
		hc.healthy = true
	} else {
		hc.healthy = false
	}

	if hc.healthy {
		hc.metrics.SetHealthy(true)
	} else {
		hc.metrics.SetHealthy(false)
		hc.metrics.IncrementHealthCheckErrors()
		log.Warn("Health check failed", "errorCount", hc.errorCount, "connected", hc.connMgr != nil && hc.connMgr.IsConnected())
	}
}
