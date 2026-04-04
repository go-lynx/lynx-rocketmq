# RocketMQ Plugin for Lynx

`lynx-rocketmq` provides a Lynx runtime plugin named `rocketmq` plus the `rocketmq.ClientInterface` used by application code.

## Runtime facts

- Go module: `github.com/go-lynx/lynx-rocketmq`
- Config prefix: `rocketmq`
- Runtime plugin name: `rocketmq`
- Public API: `rocketmq.ClientInterface`

## What the current YAML controls

The runnable template lives in [`conf/example_config.yml`](./conf/example_config.yml) and uses the `rocketmq:` root directly.

Important compatibility notes:

- `dial_timeout` and `request_timeout` are kept in the template for compatibility and planning, but the current client creation path does not wire them into the RocketMQ SDK.
- `producers[].enable_trace` and `consumers[].enable_trace` are also template-era knobs that are not wired into the current SDK options.
- `send_timeout` is the active per-producer send timeout.
- Consumer `topics` are part of config review and validation, but application code still needs to call `Subscribe` or `SubscribeWith` to start message handling.

## Example configuration

```yaml
rocketmq:
  name_server:
    - "127.0.0.1:9876"
    - "127.0.0.1:9877"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  dial_timeout: "3s"
  request_timeout: "30s"

  producers:
    - name: "default-producer"
      enabled: true
      group_name: "lynx-producer-group"
      max_retries: 3
      retry_backoff: "100ms"
      send_timeout: "3s"
      enable_trace: false
      topics:
        - "test-topic"
        - "user-events"

  consumers:
    - name: "default-consumer"
      enabled: true
      group_name: "lynx-consumer-group"
      consume_model: "CLUSTERING"
      consume_order: "CONCURRENTLY"
      max_concurrency: 4
      pull_batch_size: 32
      pull_interval: "100ms"
      enable_trace: false
      topics:
        - "test-topic"
        - "user-events"
```

## Usage

Register the plugin with a blank import, then resolve the runtime plugin from the Lynx plugin manager:

```go
import (
	"context"

	"github.com/go-lynx/lynx"
	rocketmq "github.com/go-lynx/lynx-rocketmq"
)

func sendMessage(ctx context.Context) error {
	plugin := lynx.Lynx().GetPluginManager().GetPlugin("rocketmq")
	client := plugin.(rocketmq.ClientInterface)

	return client.SendMessageWith(ctx, "default-producer", "test-topic", []byte("hello"))
}
```

For consumers, use `Subscribe` or `SubscribeWith` after startup:

```go
err := client.SubscribeWith(ctx, "default-consumer", []string{"test-topic"}, handler)
```

## Operational guidance

- Keep producer and consumer `name` values stable because application code routes by those names.
- The first enabled producer becomes the default producer for `SendMessage`; the first enabled consumer becomes the default for `Subscribe`.
- If you rely on strict send or request SLAs, validate the current runtime behavior against `send_timeout` rather than assuming `request_timeout` is already active.
