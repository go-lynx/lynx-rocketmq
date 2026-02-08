package rocketmq

import (
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
	stopCh          chan struct{}
}

// NewConnectionManager creates a new connection manager.
// If nameServerAddrs is non-empty, checkConnection will probe RocketMQ by TCP dial to one of the NameServer addresses.
func NewConnectionManager(metrics *Metrics, nameServerAddrs []string) *ConnectionManager {
	cm := &ConnectionManager{
		metrics:         metrics,
		nameServerAddrs: nameServerAddrs,
		stopCh:          make(chan struct{}),
	}
	cm.healthChecker = NewHealthChecker(metrics, cm)
	return cm
}

// Start starts the connection manager
func (cm *ConnectionManager) Start() {
	go cm.run()
}

// Stop stops the connection manager
func (cm *ConnectionManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	select {
	case <-cm.stopCh:
		// Already stopped
		return
	default:
		close(cm.stopCh)
	}

	cm.healthChecker.Stop()
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
func (cm *ConnectionManager) run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.checkConnection()
		}
	}
}

// checkConnection checks connection health by probing NameServer when addresses are configured
func (cm *ConnectionManager) checkConnection() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(cm.nameServerAddrs) == 0 {
		cm.connected = true
		return
	}

	for _, addr := range cm.nameServerAddrs {
		conn, err := net.DialTimeout("tcp", addr, nameServerProbeTimeout)
		if err == nil {
			_ = conn.Close()
			cm.connected = true
			return
		}
		log.Debug("RocketMQ NameServer probe failed", "addrs", cm.nameServerAddrs, "lastErr", err)
	}
	cm.connected = false
}

// HealthChecker performs health checks
type HealthChecker struct {
	metrics     *Metrics
	connMgr     *ConnectionManager
	mu          sync.RWMutex
	healthy     bool
	lastCheck   time.Time
	errorCount  int64
	stopCh      chan struct{}
	checkTicker *time.Ticker
}

// NewHealthChecker creates a new health checker. When connMgr is non-nil and has NameServer addrs,
// healthy is derived from connMgr.IsConnected(); otherwise from error count heuristic.
func NewHealthChecker(metrics *Metrics, connMgr *ConnectionManager) *HealthChecker {
	return &HealthChecker{
		metrics:     metrics,
		connMgr:     connMgr,
		lastCheck:   time.Now(),
		stopCh:      make(chan struct{}),
		checkTicker: time.NewTicker(10 * time.Second),
	}
}

// Start starts health check
func (hc *HealthChecker) Start() {
	go hc.run()
}

// Stop stops health check
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	select {
	case <-hc.stopCh:
		// Already stopped
		return
	default:
		close(hc.stopCh)
	}

	hc.checkTicker.Stop()
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
func (hc *HealthChecker) run() {
	for {
		select {
		case <-hc.stopCh:
			return
		case <-hc.checkTicker.C:
			hc.performHealthCheck()
		}
	}
}

// performHealthCheck performs a health check.
// When ConnectionManager has NameServer addrs, health is based on actual TCP probe (IsConnected).
// Otherwise falls back to error-count heuristic.
func (hc *HealthChecker) performHealthCheck() {
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
