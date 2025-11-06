package consumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"kafkatest/cmd/internal/config"
	"kafkatest/cmd/internal/logger"
	"kafkatest/cmd/internal/metrics"
)

// Consumer Kafka 消费者
type Consumer struct {
	config  *config.Config
	logger  *logger.Logger
	metrics *metrics.Metrics
	reader  *kafka.Reader
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewConsumer 创建一个新的 Kafka 消费者
func NewConsumer(cfg *config.Config, log *logger.Logger) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建 Kafka Reader
	Brokers := strings.Split(cfg.Kafka.Brokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  Brokers,
		Topic:    cfg.Consumer.Topic,
		GroupID:  cfg.Consumer.Group,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	// 设置初始偏移量
	switch cfg.Consumer.Offset {
	case "earliest":
		reader.SetOffset(kafka.FirstOffset)
	case "latest":
		reader.SetOffset(kafka.LastOffset)
	}

	return &Consumer{
		config:  cfg,
		logger:  log,
		metrics: metrics.NewMetrics(),
		reader:  reader,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start 开始消费者测试
func (c *Consumer) Start() error {
	c.logger.Info("开始 Kafka 消费者性能测试")
	c.logger.Infof("主题: %s", c.config.Consumer.Topic)
	c.logger.Infof("消费者组: %s", c.config.Consumer.Group)
	c.logger.Infof("并发数: %d", c.config.Consumer.Concurrency)
	c.logger.Infof("测试时长: %v", c.config.Consumer.Duration)

	// 启动工作协程
	for i := 0; i < c.config.Consumer.Concurrency; i++ {
		c.wg.Add(1)
		go c.worker(i)
	}

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
	c.reader.Close()
}

// worker 工作协程
func (c *Consumer) worker(id int) {
	defer c.wg.Done()

	c.logger.Infof("启动工作协程 %d", id)

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Infof("工作协程 %d 收到停止信号", id)
			return
		default:
			startTime := time.Now()

			// 读取消息
			message, err := c.reader.ReadMessage(c.ctx)
			latency := time.Since(startTime)

			if err != nil {
				// 检查是否是上下文取消错误
				if err == context.Canceled {
					c.logger.Infof("工作协程 %d 收到停止信号", id)
					return
				}

				// 记录错误
				c.metrics.Record(0, latency, err)
				c.logger.Errorf("读取消息失败: %v", err)
				continue
			}

			// 计算消息延迟
			messageLatency := time.Since(message.Time)

			// 记录指标
			size := int64(len(message.Value))
			c.metrics.Record(size, messageLatency, nil)

			// 手动提交偏移量
			if err := c.reader.CommitMessages(c.ctx, message); err != nil {
				c.logger.Errorf("提交偏移量失败: %v", err)
			}
		}
	}
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
