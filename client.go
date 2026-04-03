package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/go-lynx/lynx-rocketmq/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Client RocketMQ client plugin
type Client struct {
	*plugins.BasePlugin
	conf *conf.RocketMQ
	rt   plugins.Runtime
	// Multi-instance producers/consumers
	producers       map[string]rocketmq.Producer
	consumers       map[string]rocketmq.PushConsumer
	defaultProducer string
	defaultConsumer string
	// Connection managers
	prodConnMgrs map[string]*ConnectionManager
	consConnMgrs map[string]*ConnectionManager
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	metrics      *Metrics
	retryHandler *RetryHandler
}

// Ensure Client implements all interfaces
var _ ClientInterface = (*Client)(nil)
var _ Producer = (*Client)(nil)
var _ Consumer = (*Client)(nil)

// NewRocketMQClient creates a new RocketMQ client plugin instance
func NewRocketMQClient() *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		BasePlugin: plugins.NewBasePlugin(
			plugins.GeneratePluginID("", pluginName, pluginVersion),
			pluginName,
			pluginDescription,
			pluginVersion,
			confPrefix,
			100,
		),
		ctx:          ctx,
		cancel:       cancel,
		metrics:      NewMetrics(),
		retryHandler: NewRetryHandler(RetryConfig{MaxRetries: 3, BackoffTime: time.Second, MaxBackoff: 30 * time.Second}),
		producers:    make(map[string]rocketmq.Producer),
		consumers:    make(map[string]rocketmq.PushConsumer),
		prodConnMgrs: make(map[string]*ConnectionManager),
		consConnMgrs: make(map[string]*ConnectionManager),
	}
}

// InitializeResources initializes RocketMQ resources
func (r *Client) InitializeResources(rt plugins.Runtime) error {
	if err := r.BasePlugin.InitializeResources(rt); err != nil {
		return err
	}
	r.rt = rt
	r.conf = &conf.RocketMQ{}

	// Load configuration
	err := rt.GetConfig().Value(confPrefix).Scan(r.conf)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)
	}

	// Validate configuration
	if err := r.validateConfiguration(); err != nil {
		return err
	}

	// Set default values
	r.setDefaultValues()

	return nil
}

// StartupTasks startup tasks
func (r *Client) StartupTasks() error {
	r.ensureLifecycleContext()
	return r.startupTasksContext(r.ctx)
}

func (r *Client) startupTasksContext(ctx context.Context) (startErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	defer func() {
		if startErr == nil {
			return
		}
		if cleanupErr := r.shutdownTasksContext(context.Background()); cleanupErr != nil {
			startErr = errors.Join(startErr, cleanupErr)
		}
	}()

	// Initialize all enabled producer instances
	var firstProducerName string
	for _, p := range r.conf.Producers {
		if p == nil || !p.Enabled {
			continue
		}
		name := p.Name
		if name == "" {
			name = "default-producer"
		}

		producer, err := r.createProducer(name, p)
		if err != nil {
			return WrapError(err, "failed to create producer: "+name)
		}

		connMgr := NewConnectionManager(r.metrics, r.conf.NameServer)
		if err := connMgr.checkConnectionContext(ctx); err != nil {
			_ = producer.Shutdown()
			return WrapError(err, "failed to probe producer nameserver: "+name)
		}
		if err := connMgr.StartWithContext(r.ctx); err != nil {
			_ = producer.Shutdown()
			return WrapError(err, "failed to start producer connection manager: "+name)
		}

		r.mu.Lock()
		r.producers[name] = producer
		r.prodConnMgrs[name] = connMgr
		if firstProducerName == "" {
			firstProducerName = name
			r.defaultProducer = name
		}
		r.mu.Unlock()
	}

	// Initialize all enabled consumer instances
	var firstConsumerName string
	for _, c := range r.conf.Consumers {
		if c == nil || !c.Enabled {
			continue
		}
		name := c.Name
		if name == "" {
			name = "default-consumer"
		}

		consumer, err := r.createConsumer(name, c)
		if err != nil {
			return WrapError(err, "failed to create consumer: "+name)
		}

		connMgr := NewConnectionManager(r.metrics, r.conf.NameServer)
		if err := connMgr.checkConnectionContext(ctx); err != nil {
			_ = consumer.Shutdown()
			return WrapError(err, "failed to probe consumer nameserver: "+name)
		}
		if err := connMgr.StartWithContext(r.ctx); err != nil {
			_ = consumer.Shutdown()
			return WrapError(err, "failed to start consumer connection manager: "+name)
		}

		r.mu.Lock()
		r.consumers[name] = consumer
		r.consConnMgrs[name] = connMgr
		if firstConsumerName == "" {
			firstConsumerName = name
			r.defaultConsumer = name
		}
		r.mu.Unlock()
	}

	if r.rt != nil {
		if err := r.rt.RegisterSharedResource(pluginName, r); err != nil {
			return WrapError(err, "failed to register RocketMQ shared resource")
		}
		if len(r.producers) > 0 {
			if err := r.rt.RegisterPrivateResource("producers", r.producers); err != nil {
				log.Warn("Failed to register RocketMQ private producers resource", "error", err)
			}
		}
		if len(r.consumers) > 0 {
			if err := r.rt.RegisterPrivateResource("consumers", r.consumers); err != nil {
				log.Warn("Failed to register RocketMQ private consumers resource", "error", err)
			}
		}
		if len(r.prodConnMgrs) > 0 {
			if err := r.rt.RegisterPrivateResource("producer_connection_managers", r.prodConnMgrs); err != nil {
				log.Warn("Failed to register RocketMQ private producer connection managers resource", "error", err)
			}
		}
		if len(r.consConnMgrs) > 0 {
			if err := r.rt.RegisterPrivateResource("consumer_connection_managers", r.consConnMgrs); err != nil {
				log.Warn("Failed to register RocketMQ private consumer connection managers resource", "error", err)
			}
		}
		if r.metrics != nil {
			if err := r.rt.RegisterPrivateResource("metrics", r.metrics); err != nil {
				log.Warn("Failed to register RocketMQ private metrics resource", "error", err)
			}
		}
		if r.retryHandler != nil {
			if err := r.rt.RegisterPrivateResource("retry_handler", r.retryHandler); err != nil {
				log.Warn("Failed to register RocketMQ private retry handler resource", "error", err)
			}
		}
	}

	log.Info("RocketMQ plugin started successfully")
	return nil
}

// ShutdownTasks shutdown tasks
func (r *Client) ShutdownTasks() error {
	return r.shutdownTasksContext(context.Background())
}

func (r *Client) shutdownTasksContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	prodConnMgrs := r.prodConnMgrs
	consConnMgrs := r.consConnMgrs
	producers := r.producers
	consumers := r.consumers
	r.prodConnMgrs = make(map[string]*ConnectionManager)
	r.consConnMgrs = make(map[string]*ConnectionManager)
	r.producers = make(map[string]rocketmq.Producer)
	r.consumers = make(map[string]rocketmq.PushConsumer)
	r.defaultProducer = ""
	r.defaultConsumer = ""
	cancel := r.cancel
	r.ctx = nil
	r.cancel = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var errs []error

	// Stop all connection managers
	for name, connMgr := range prodConnMgrs {
		connMgr.Stop()
		log.Info("Stopped producer connection manager", "name", name)
	}

	for name, connMgr := range consConnMgrs {
		connMgr.Stop()
		log.Info("Stopped consumer connection manager", "name", name)
	}

	// Shutdown all producers
	for name, producer := range producers {
		if err := runShutdownWithContext(ctx, "producer "+name, producer.Shutdown); err != nil {
			log.Error("Failed to shutdown producer", "name", name, "error", err)
			errs = append(errs, err)
		} else {
			log.Info("Shutdown producer", "name", name)
		}
	}

	// Shutdown all consumers
	for name, consumer := range consumers {
		if err := runShutdownWithContext(ctx, "consumer "+name, consumer.Shutdown); err != nil {
			log.Error("Failed to shutdown consumer", "name", name, "error", err)
			errs = append(errs, err)
		} else {
			log.Info("Shutdown consumer", "name", name)
		}
	}

	log.Info("RocketMQ plugin shutdown completed")
	return errors.Join(errs...)
}

// GetMetrics gets monitoring metrics
func (r *Client) GetMetrics() *Metrics {
	return r.metrics
}

func (r *Client) ensureLifecycleContext() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			r.ctx = nil
			r.cancel = nil
		default:
			return
		}
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
}

func runShutdownWithContext(ctx context.Context, component string, shutdown func() error) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s shutdown canceled before execution: %w", component, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- shutdown()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%s shutdown canceled: %w", component, ctx.Err())
	}
}

// validateConfiguration validates the configuration
func (r *Client) validateConfiguration() error {
	if len(r.conf.NameServer) == 0 {
		return ErrMissingNameServer
	}

	// Validate producers
	for _, p := range r.conf.Producers {
		if p == nil || !p.Enabled {
			continue
		}
		if err := r.validateProducerConfig(p); err != nil {
			return err
		}
	}

	// Validate consumers
	for _, c := range r.conf.Consumers {
		if c == nil || !c.Enabled {
			continue
		}
		if err := r.validateConsumerConfig(c); err != nil {
			return err
		}
	}

	return nil
}

// validateProducerConfig validates producer configuration
func (r *Client) validateProducerConfig(p *conf.Producer) error {
	if p.GroupName == "" {
		p.GroupName = defaultProducerGroup
	}

	if err := validateGroupName(p.GroupName); err != nil {
		return err
	}

	for _, topic := range p.Topics {
		if err := validateTopic(topic); err != nil {
			return err
		}
	}

	return nil
}

// validateConsumerConfig validates consumer configuration
func (r *Client) validateConsumerConfig(c *conf.Consumer) error {
	if c.GroupName == "" {
		c.GroupName = defaultConsumerGroup
	}

	if err := validateGroupName(c.GroupName); err != nil {
		return err
	}

	if err := validateConsumeModel(c.ConsumeModel); err != nil {
		return err
	}

	if err := validateConsumeOrder(c.ConsumeOrder); err != nil {
		return err
	}

	for _, topic := range c.Topics {
		if err := validateTopic(topic); err != nil {
			return err
		}
	}

	return nil
}

// setDefaultValues sets default values for configuration
func (r *Client) setDefaultValues() {
	// Set default timeouts if not specified
	if r.conf.DialTimeout == nil {
		r.conf.DialTimeout = durationpb.New(parseDuration(defaultDialTimeout, 3*time.Second))
	}

	if r.conf.RequestTimeout == nil {
		r.conf.RequestTimeout = durationpb.New(parseDuration(defaultRequestTimeout, 30*time.Second))
	}

	// Set producer defaults
	for _, p := range r.conf.Producers {
		if p == nil {
			continue
		}
		if p.MaxRetries == 0 {
			p.MaxRetries = defaultMaxRetries
		}
		if p.RetryBackoff == nil {
			p.RetryBackoff = durationpb.New(parseDuration(defaultRetryBackoff, 100*time.Millisecond))
		}
		if p.SendTimeout == nil {
			p.SendTimeout = durationpb.New(parseDuration(defaultSendTimeout, 3*time.Second))
		}
	}

	// Set consumer defaults
	for _, c := range r.conf.Consumers {
		if c == nil {
			continue
		}
		if c.MaxConcurrency == 0 {
			c.MaxConcurrency = defaultMaxConcurrency
		}
		if c.PullBatchSize == 0 {
			c.PullBatchSize = defaultPullBatchSize
		}
		if c.PullInterval == nil {
			c.PullInterval = durationpb.New(parseDuration(defaultPullInterval, 100*time.Millisecond))
		}
	}
}

// createProducer creates a RocketMQ producer
func (r *Client) createProducer(name string, config *conf.Producer) (rocketmq.Producer, error) {
	// Create producer options
	opts := []producer.Option{
		producer.WithNameServer(primitive.NamesrvAddr(r.conf.NameServer)),
		producer.WithGroupName(config.GroupName),
		producer.WithRetry(int(config.MaxRetries)),
		producer.WithSendMsgTimeout(config.SendTimeout.AsDuration()),
	}

	// Add authentication if provided
	if r.conf.AccessKey != "" && r.conf.SecretKey != "" {
		opts = append(opts, producer.WithCredentials(primitive.Credentials{
			AccessKey: r.conf.AccessKey,
			SecretKey: r.conf.SecretKey,
		}))
	}

	// Create producer
	producer, err := rocketmq.NewProducer(opts...)
	if err != nil {
		return nil, WrapError(err, "failed to create producer")
	}

	// Start producer
	if err := producer.Start(); err != nil {
		return nil, WrapError(err, "failed to start producer")
	}

	log.Info("Created RocketMQ producer", "name", name, "group", config.GroupName)
	return producer, nil
}

// createConsumer creates a RocketMQ consumer
func (r *Client) createConsumer(name string, config *conf.Consumer) (rocketmq.PushConsumer, error) {
	// Consume model: CLUSTERING or BROADCASTING
	consumeModel := consumer.Clustering
	if config.ConsumeModel == ConsumeModelBroadcast {
		consumeModel = consumer.BroadCasting
	}

	opts := []consumer.Option{
		consumer.WithNameServer(primitive.NamesrvAddr(r.conf.NameServer)),
		consumer.WithGroupName(config.GroupName),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		consumer.WithConsumerModel(consumeModel),
		consumer.WithConsumerOrder(config.ConsumeOrder == ConsumeOrderOrderly),
		consumer.WithPullBatchSize(config.PullBatchSize),
		consumer.WithPullInterval(consumerPullInterval(config.PullInterval)),
		consumer.WithConsumeGoroutineNums(int(config.MaxConcurrency)),
	}

	// Add authentication if provided
	if r.conf.AccessKey != "" && r.conf.SecretKey != "" {
		opts = append(opts, consumer.WithCredentials(primitive.Credentials{
			AccessKey: r.conf.AccessKey,
			SecretKey: r.conf.SecretKey,
		}))
	}

	// Create consumer
	consumer, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		return nil, WrapError(err, "failed to create consumer")
	}

	log.Info("Created RocketMQ consumer", "name", name, "group", config.GroupName)
	return consumer, nil
}
