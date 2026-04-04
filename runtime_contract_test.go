package rocketmq

import (
	"context"
	"testing"

	"github.com/go-lynx/lynx-rocketmq/conf"
	"github.com/go-lynx/lynx/plugins"
)

func TestRocketMQRuntimeContract_NoInstanceLifecycle(t *testing.T) {
	base := plugins.NewSimpleRuntime()
	rt := base.WithPluginContext(pluginName)

	client := NewRocketMQClient()
	client.rt = rt
	client.conf = &conf.RocketMQ{}

	if err := client.StartContext(context.Background(), client); err != nil {
		t.Fatalf("StartContext failed: %v", err)
	}

	if alias, err := base.GetSharedResource(sharedPluginResourceName); err != nil || alias != client {
		t.Fatalf("unexpected shared plugin alias: value=%#v err=%v", alias, err)
	}
	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != true {
		t.Fatalf("unexpected shared readiness: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != true {
		t.Fatalf("unexpected shared health: value=%#v err=%v", health, err)
	}
	if _, err := rt.GetPrivateResource("config"); err != nil {
		t.Fatalf("private config resource missing: %v", err)
	}

	if err := client.StopContext(context.Background(), client); err != nil {
		t.Fatalf("StopContext failed: %v", err)
	}

	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != false {
		t.Fatalf("unexpected shared readiness after stop: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != false {
		t.Fatalf("unexpected shared health after stop: value=%#v err=%v", health, err)
	}
}
