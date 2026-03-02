package rocketmq

const (
	// Plugin metadata
	pluginName        = "rocketmq"
	pluginVersion     = "v1.5.4"
	pluginDescription = "RocketMQ message queue plugin for Lynx framework"
	confPrefix        = "rocketmq"

	// Default values
	defaultDialTimeout    = "3s"
	defaultRequestTimeout = "30s"
	defaultSendTimeout    = "3s"
	defaultRetryBackoff   = "100ms"
	defaultMaxRetries     = 3
	defaultPullBatchSize  = 32
	defaultPullInterval   = "100ms"
	defaultMaxConcurrency = 1

	// Consumption models
	ConsumeModelClustering = "CLUSTERING"
	ConsumeModelBroadcast  = "BROADCASTING"

	// Consumption orders
	ConsumeOrderConcurrent = "CONCURRENTLY"
	ConsumeOrderOrderly    = "ORDERLY"

	// Default group names
	defaultProducerGroup = "lynx-producer-group"
	defaultConsumerGroup = "lynx-consumer-group"
)
