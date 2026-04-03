package rocketmq

import (
	"context"
	"fmt"

	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
)

func (r *Client) PluginProtocol() plugins.PluginProtocol {
	protocol := r.BasePlugin.PluginProtocol()
	protocol.ContextLifecycle = true
	return protocol
}

func (r *Client) IsContextAware() bool {
	return true
}

func (r *Client) InitializeContext(ctx context.Context, plugin plugins.Plugin, rt plugins.Runtime) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("initialize canceled before start: %w", err)
	}
	return r.BasePlugin.Initialize(plugin, rt)
}

func (r *Client) StartContext(ctx context.Context, plugin plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("start canceled before execution: %w", err)
	}
	if r.Status(plugin) == plugins.StatusActive {
		return plugins.ErrPluginAlreadyActive
	}

	r.SetStatus(plugins.StatusInitializing)
	r.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStarting,
		Priority: plugins.PriorityNormal,
		Source:   "StartContext",
		Category: "lifecycle",
	})

	r.ensureLifecycleContext()
	if err := r.startupTasksContext(ctx); err != nil {
		r.SetStatus(plugins.StatusFailed)
		return plugins.NewPluginError(r.ID(), "Start", "Failed to perform startup tasks", err)
	}

	if err := r.CheckHealth(); err != nil {
		r.SetStatus(plugins.StatusFailed)
		log.Errorf("Plugin %s health check failed: %v", plugin.Name(), err)
		return fmt.Errorf("plugin %s health check failed: %w", plugin.Name(), err)
	}

	r.SetStatus(plugins.StatusActive)
	r.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStarted,
		Priority: plugins.PriorityNormal,
		Source:   "StartContext",
		Category: "lifecycle",
	})

	return nil
}

func (r *Client) StopContext(ctx context.Context, plugin plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("stop canceled before execution: %w", err)
	}
	if r.Status(plugin) != plugins.StatusActive {
		return plugins.NewPluginError(r.ID(), "Stop", "Plugin must be active to stop", plugins.ErrPluginNotActive)
	}

	r.SetStatus(plugins.StatusStopping)
	r.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStopping,
		Priority: plugins.PriorityNormal,
		Source:   "StopContext",
		Category: "lifecycle",
	})

	if err := r.shutdownTasksContext(ctx); err != nil {
		r.SetStatus(plugins.StatusFailed)
		return plugins.NewPluginError(r.ID(), "Stop", "Failed to perform cleanup tasks", err)
	}

	r.SetStatus(plugins.StatusTerminated)
	r.EmitEvent(plugins.PluginEvent{
		Type:     plugins.EventPluginStopped,
		Priority: plugins.PriorityNormal,
		Source:   "StopContext",
		Category: "lifecycle",
	})

	return nil
}

func (r *Client) CleanupTasks() error {
	return r.ShutdownTasks()
}

func (r *Client) CheckHealth() error {
	r.mu.RLock()
	prodConnMgrs := make(map[string]*ConnectionManager, len(r.prodConnMgrs))
	for name, mgr := range r.prodConnMgrs {
		prodConnMgrs[name] = mgr
	}
	consConnMgrs := make(map[string]*ConnectionManager, len(r.consConnMgrs))
	for name, mgr := range r.consConnMgrs {
		consConnMgrs[name] = mgr
	}
	r.mu.RUnlock()

	for name, mgr := range prodConnMgrs {
		if mgr == nil || !mgr.IsConnected() {
			return fmt.Errorf("producer connection manager %s is not connected", name)
		}
		healthChecker := mgr.GetHealthChecker()
		if healthChecker != nil && !healthChecker.IsHealthy() {
			return fmt.Errorf("producer health checker %s is unhealthy", name)
		}
	}

	for name, mgr := range consConnMgrs {
		if mgr == nil || !mgr.IsConnected() {
			return fmt.Errorf("consumer connection manager %s is not connected", name)
		}
		healthChecker := mgr.GetHealthChecker()
		if healthChecker != nil && !healthChecker.IsHealthy() {
			return fmt.Errorf("consumer health checker %s is unhealthy", name)
		}
	}

	return nil
}
