# RocketMQ Plugin for Lynx Framework

## Overview

The RocketMQ plugin provides integration with Apache RocketMQ message queue system for the Lynx framework. It supports both producer and consumer functionality with comprehensive monitoring, health checks, and retry mechanisms.

## Features

- **Multi-instance Support**: Support for multiple producer and consumer instances
- **Health Monitoring**: Built-in health checks and connection management
- **Metrics Collection**: Comprehensive metrics for monitoring and observability
- **Retry Mechanism**: Configurable retry logic with exponential backoff
- **Connection Pooling**: Efficient connection and resource management
- **Graceful Shutdown**: Proper cleanup and resource management

## Configuration

### Basic Configuration

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
      
  consumers:
    - name: "default-consumer"
      enabled: true
      group_name: "lynx-consumer-group"
      consume_model: "CLUSTERING"
      consume_order: "CONCURRENTLY"
      max_concurrency: 1
      pull_batch_size: 32
      pull_interval: "100ms"
      enable_trace: false
```

### Producer Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Whether to enable the producer |
| `group_name` | string | "lynx-producer-group" | Producer group name |
| `max_retries` | int | 3 | Maximum number of retries |
| `retry_backoff` | duration | "100ms" | Retry interval |
| `send_timeout` | duration | "3s" | Send message timeout |
| `name` | string | "" | Producer instance name |
| `topics` | []string | [] | Allowed topics for routing/permissions |
| `enable_trace` | bool | false | Whether to enable trace |

### Consumer Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Whether to enable the consumer |
| `group_name` | string | "lynx-consumer-group" | Consumer group name |
| `consume_model` | string | "CLUSTERING" | Consumption model (CLUSTERING/BROADCASTING) |
| `consume_order` | string | "CONCURRENTLY" | Message consumption order (CONCURRENTLY/ORDERLY) |
| `max_concurrency` | int | 1 | Maximum processing concurrency |
| `pull_batch_size` | int | 32 | Pull batch size |
| `pull_interval` | duration | "100ms" | Pull interval |
| `name` | string | "" | Consumer instance name |
| `topics` | []string | [] | Subscribed topic list |
| `enable_trace` | bool | false | Whether to enable trace |

## Usage

### Producer Usage

```go
package main

import (
    "context"
    "github.com/go-lynx/lynx/plugins/mq/rocketmq"
)

func main() {
    // Get RocketMQ client from plugin manager
    client := pluginManager.GetPlugin("rocketmq").(rocketmq.ClientInterface)
    
    // Send message
    err := client.SendMessage(context.Background(), "test-topic", []byte("Hello RocketMQ"))
    if err != nil {
        log.Fatal(err)
    }
    
    // Send message with specific producer
    err = client.SendMessageWith(context.Background(), "default-producer", "test-topic", []byte("Hello RocketMQ"))
    if err != nil {
        log.Fatal(err)
    }
}
```

### Consumer Usage

```go
package main

import (
    "context"
    "github.com/go-lynx/lynx/plugins/mq/rocketmq"
    "github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
    // Get RocketMQ client from plugin manager
    client := pluginManager.GetPlugin("rocketmq").(rocketmq.ClientInterface)
    
    // Define message handler
    handler := func(ctx context.Context, msg *primitive.MessageExt) error {
        log.Printf("Received message: %s", string(msg.Body))
        return nil
    }
    
    // Subscribe to one or more topics (all topics in the slice are subscribed)
    err := client.Subscribe(context.Background(), []string{"test-topic"}, handler)
    if err != nil {
        log.Fatal(err)
    }
    
    // Subscribe with specific consumer to multiple topics
    err = client.SubscribeWith(context.Background(), "default-consumer", []string{"topic-a", "topic-b"}, handler)
    if err != nil {
        log.Fatal(err)
    }
}
```

## Monitoring

The plugin provides comprehensive metrics that can be accessed through the `GetMetrics()` method:

```go
metrics := client.GetMetrics()
stats := metrics.GetStats()

// Access producer metrics
producerStats := stats["producer"].(map[string]interface{})
messagesSent := producerStats["messages_sent"].(int64)

// Access consumer metrics
consumerStats := stats["consumer"].(map[string]interface{})
messagesReceived := consumerStats["messages_received"].(int64)

// Access health metrics
healthStats := stats["health"].(map[string]interface{})
isHealthy := healthStats["is_healthy"].(bool)
```

## Health Checks

The plugin includes built-in health checks that monitor:

- Connection status
- Producer/Consumer readiness
- Message processing health
- Error rates and latencies

Health status can be checked programmatically:

```go
// Check if producer is ready
if client.IsProducerReady("default-producer") {
    // Producer is ready
}

// Check if consumer is ready
if client.IsConsumerReady("default-consumer") {
    // Consumer is ready
}
```

## Error Handling

The plugin provides comprehensive error handling with context-aware error wrapping:

```go
if err := client.SendMessage(ctx, topic, body); err != nil {
    if rocketmq.IsError(err, rocketmq.ErrProducerNotReady) {
        // Handle producer not ready error
    } else if rocketmq.IsError(err, rocketmq.ErrSendMessageFailed) {
        // Handle send message failed error
    }
}
```

## Fixes and Improvements (Changelog)

The following fixes have been applied to improve production readiness and configuration consistency:

### 1. Multi-Topic Subscribe Fix

- **Issue**: When passing multiple topics to `Subscribe`/`SubscribeWith`, only `topics[0]` was passed to the SDK’s `Subscribe`; the rest were never subscribed.
- **Fix**: Iterate over the full `topics` list and call `Subscribe` once per topic with the same consume callback; return an error and stop if any subscription fails.
- **Impact**: Passing multiple topics in config or code now correctly subscribes and consumes from all of them.

### 2. Consume Model Configuration (CLUSTERING / BROADCASTING)

- **Issue**: In `createConsumer`, the consume model was hardcoded to `consumer.Clustering`, so `consume_model: BROADCASTING` had no effect.
- **Fix**: Choose `consumer.Clustering` or `consumer.BroadCasting` from config `ConsumeModel` and pass it to the SDK.
- **Impact**: With `consume_model: "BROADCASTING"`, consumers run in broadcast mode; otherwise they use clustering (default).

### 3. Consumer Pull and Concurrency Configuration

- **Issue**: `pull_batch_size`, `pull_interval`, and `max_concurrency` were not passed to the RocketMQ client; SDK defaults were used instead.
- **Fix**: In `createConsumer`, add:
  - `consumer.WithPullBatchSize(config.PullBatchSize)`
  - `consumer.WithPullInterval(...)` (with nil-safe handling for `pull_interval`)
  - `consumer.WithConsumeGoroutineNums(int(config.MaxConcurrency))`
- **Impact**: These options now control pull batch size, pull interval, and consume goroutine count for tuning and stability.

### 4. Health Check Uses Real RocketMQ Probe

- **Issue**: `ConnectionManager.checkConnection` did not probe RocketMQ and always set `connected = true`, so health did not reflect real connectivity.
- **Fix**:
  - `NewConnectionManager` now accepts `nameServerAddrs []string` (from `rocketmq.name_server`).
  - When `nameServerAddrs` is non-empty, `checkConnection` probes each address with TCP `DialTimeout`; if any succeeds, the connection is considered up, otherwise down.
  - When a `ConnectionManager` with NameServer addrs is present, `HealthChecker` uses `IsConnected()` for health; otherwise it keeps the previous error-count heuristic.
- **Impact**: With `name_server` configured, health reflects actual NameServer reachability for deployment and ops.

### 5. Other Notes

- **ACK and consume result**: The consumer still drives SDK `ConsumeRetryLater`/`ConsumeSuccess` via the handler’s return value (“indirect ACK”); there is no separate manual ACK API.
- **Transactional messages**: The plugin still does not support transactional messages (TransactionProducer); implement or extend at the application layer if needed.

## Dependencies

- `github.com/apache/rocketmq-client-go/v2` - Official RocketMQ Go client
- `github.com/go-lynx/lynx` - Lynx framework core

## License

This plugin is part of the Lynx framework and follows the same license terms.
