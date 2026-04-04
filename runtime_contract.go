package rocketmq

import "github.com/go-lynx/lynx/log"

const (
	sharedPluginResourceName     = pluginName + ".plugin"
	sharedReadinessResourceName  = pluginName + ".readiness"
	sharedHealthResourceName     = pluginName + ".health"
	privateReadinessResourceName = "readiness"
	privateHealthResourceName    = "health"
)

func (r *Client) registerRuntimePluginAlias() {
	if r == nil || r.rt == nil {
		return
	}
	if err := r.rt.RegisterSharedResource(sharedPluginResourceName, r); err != nil {
		log.Warn("Failed to register RocketMQ shared plugin alias", "error", err)
	}
}

func (r *Client) publishRuntimeContract(ready, healthy bool) {
	if r == nil || r.rt == nil {
		return
	}
	for _, item := range []struct {
		name  string
		value any
	}{
		{name: sharedReadinessResourceName, value: ready},
		{name: sharedHealthResourceName, value: healthy},
	} {
		if err := r.rt.RegisterSharedResource(item.name, item.value); err != nil {
			log.Warn("Failed to register RocketMQ shared runtime contract", "name", item.name, "error", err)
		}
	}
	if err := r.rt.RegisterPrivateResource(privateReadinessResourceName, ready); err != nil {
		log.Warn("Failed to register RocketMQ private readiness resource", "error", err)
	}
	if err := r.rt.RegisterPrivateResource(privateHealthResourceName, healthy); err != nil {
		log.Warn("Failed to register RocketMQ private health resource", "error", err)
	}
}
