package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"kafkatest/cmd/internal/config"
	"kafkatest/cmd/internal/logger"
	"kafkatest/cmd/internal/metrics"
	"kafkatest/pkg/utils"
)

// Consumer Kafka 消费者
type Consumer struct {
	config   *config.Config
	logger   *logger.Logger
	metrics  *metrics.Metrics
	consumer sarama.ConsumerGroup
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewConsumer 创建一个新的 Kafka 消费者
func NewConsumer(cfg *config.Config, log *logger.Logger) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建 Sarama 配置
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_5_0_0 // 使用 Kafka 2.5.0 版本

	// 设置初始偏移量
	switch cfg.Consumer.Offset {
	case "earliest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "latest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	Brokers := strings.Split(cfg.Kafka.Brokers, ",")
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = "kafka-admin"
	config.Net.SASL.Password = "Aa71-Sa8+cM2w09Y"
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &utils.XDGSCRAMClient{HashGeneratorFcn: utils.SHA512} }

	// 开启了 dialer proxy
	if cfg.Kafka.Proxy {
		config.Net.Proxy.Enable = true
		config.Net.Proxy.Dialer = utils.GetDialerProxy(config, cfg.Kafka.AddrMap)
	}

	// 创建消费者组
	consumer, err := sarama.NewConsumerGroup(Brokers, cfg.Consumer.Group, config)
	if err != nil {
		log.Fatalf("创建 Kafka 消费者失败: %v", err)
	}

	return &Consumer{
		config:   cfg,
		logger:   log,
		metrics:  metrics.NewMetrics(),
		consumer: consumer,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start 开始消费者测试
func (c *Consumer) Start() error {
	c.logger.Info("开始 Kafka 消费者性能测试")
	c.logger.Infof("主题: %s", c.config.Consumer.Topic)
	c.logger.Infof("消费者组: %s", c.config.Consumer.Group)
	c.logger.Infof("并发数: %d", c.config.Consumer.Concurrency)
	c.logger.Infof("测试时长: %v", c.config.Consumer.Duration)

	// 创建消费者组处理器
	handler := &consumerGroupHandler{
		consumer: c,
	}

	// 启动消费者组
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Info("消费者组收到停止信号")
				return
			default:
				// 消费消息
				if err := c.consumer.Consume(c.ctx, []string{c.config.Consumer.Topic}, handler); err != nil {
					if err == sarama.ErrClosedConsumerGroup {
						c.logger.Info("消费者组已关闭")
						return
					}
					c.logger.Errorf("消费消息错误: %v", err)
				}
			}
		}
	}()

	// 如果设置了测试时长，启动定时器
	if c.config.Consumer.Duration > 0 {
		go func() {
			time.Sleep(c.config.Consumer.Duration)
			c.Stop()
		}()
	}

	return nil
}

// Stop 停止消费者测试
func (c *Consumer) Stop() {
	c.logger.Info("停止 Kafka 消费者性能测试")
	c.cancel()
	c.wg.Wait()
	c.metrics.Finish()
	c.consumer.Close()
}

// consumerGroupHandler 实现 sarama.ConsumerGroupHandler 接口
type consumerGroupHandler struct {
	consumer *Consumer
}

// Setup 在消费者组会话开始时调用
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 在消费者组会话结束时调用
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理消息消费
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.consumer.logger.Infof("开始消费分区: %d", claim.Partition())

	for message := range claim.Messages() {
		select {
		case <-h.consumer.ctx.Done():
			h.consumer.logger.Info("消费者收到停止信号")
			return nil
		default:
			// 计算消息延迟
			messageLatency := time.Since(message.Timestamp)
			data, _ := json.Marshal(message)
			h.consumer.logger.Infof("messageLatency: %v, consumeClaim data: %s", messageLatency, data)

			// 记录指标
			size := int64(len(message.Value))
			h.consumer.metrics.Record(size, messageLatency, nil)

			// 标记消息为已处理
			session.MarkMessage(message, "")

			h.consumer.logger.Debugf("处理消息: topic=%s, partition=%d, offset=%d",
				message.Topic, message.Partition, message.Offset)
		}
	}

	return nil
}

// Wait 等待消费者测试完成
func (c *Consumer) Wait() {
	c.wg.Wait()
}

// GetMetrics 获取性能指标
func (c *Consumer) GetMetrics() *metrics.Metrics {
	return c.metrics
}

// Run 运行消费者测试
func Run(cfg *config.Config, log *logger.Logger) error {
	consumer := NewConsumer(cfg, log)

	if err := consumer.Start(); err != nil {
		return fmt.Errorf("启动消费者失败: %w", err)
	}

	// 等待测试完成
	consumer.wg.Wait()

	// 获取性能指标
	metrics := consumer.GetMetrics()

	// 打印性能指标摘要
	summary := metrics.Summary()
	log.Info("性能测试完成")
	log.Infof("消息总数: %d", summary["message_count"])
	log.Infof("总字节数: %d", summary["total_bytes"])
	log.Infof("错误数: %d", summary["errors"])
	log.Infof("吞吐量: %.2f 字节/秒", summary["throughput"])
	log.Infof("消息速率: %.2f 消息/秒", summary["message_rate"])
	log.Infof("平均延迟: %v", summary["avg_latency"])
	log.Infof("成功率: %.2f%%", summary["success_rate"])

	return nil
}
