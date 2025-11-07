package producer

import (
	"context"
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

// Producer Kafka 生产者
type Producer struct {
	config   *config.Config
	logger   *logger.Logger
	metrics  *metrics.Metrics
	producer sarama.SyncProducer
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewProducer 创建一个新的 Kafka 生产者
func NewProducer(cfg *config.Config, log *logger.Logger) *Producer {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建 Sarama 配置
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = cfg.Kafka.User
	config.Net.SASL.Password = cfg.Kafka.Password
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &utils.XDGSCRAMClient{HashGeneratorFcn: utils.SHA512} }

	// 开启了 dialer proxy
	if cfg.Kafka.Proxy {
		config.Net.Proxy.Enable = true
		config.Net.Proxy.Dialer = utils.GetDialerProxy(config, cfg.Kafka.AddrMap)
	}

	Brokers := strings.Split(cfg.Kafka.Brokers, ",")
	// 创建同步生产者
	producer, err := sarama.NewSyncProducer(Brokers, config)
	if err != nil {
		log.Fatalf("创建 Kafka 生产者失败: %v", err)
	}

	return &Producer{
		config:   cfg,
		logger:   log,
		metrics:  metrics.NewMetrics(),
		producer: producer,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start 开始生产者测试
func (p *Producer) Start() error {
	p.logger.Info("开始 Kafka 生产者性能测试")
	p.logger.Infof("主题: %s", p.config.Producer.Topic)
	p.logger.Infof("消息大小: %d 字节", p.config.Producer.MessageSize)
	p.logger.Infof("消息数量: %d", p.config.Producer.MessageCount)
	p.logger.Infof("发送速率: %d 消息/秒", p.config.Producer.Rate)
	p.logger.Infof("并发数: %d", p.config.Producer.Concurrency)

	// 计算每个工作协程的消息数量
	messagesPerWorker := p.config.Producer.MessageCount / p.config.Producer.Concurrency
	if p.config.Producer.MessageCount%p.config.Producer.Concurrency != 0 {
		messagesPerWorker++
	}

	// 计算发送间隔
	interval := time.Second / time.Duration(p.config.Producer.Rate/p.config.Producer.Concurrency)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}

	// 启动工作协程
	for i := 0; i < p.config.Producer.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(i, messagesPerWorker, interval)
	}

	// 如果设置了测试时长，启动定时器
	if p.config.Producer.Duration > 0 {
		go func() {
			time.Sleep(p.config.Producer.Duration)
			p.Stop()
		}()
	}

	return nil
}

// Stop 停止生产者测试
func (p *Producer) Stop() {
	p.logger.Info("停止 Kafka 生产者性能测试")
	p.cancel()
	p.wg.Wait()
	p.metrics.Finish()
	p.producer.Close()
}

// worker 工作协程
func (p *Producer) worker(id int, messageCount int, interval time.Duration) {
	defer p.wg.Done()

	p.logger.Infof("启动工作协程 %d", id)

	// 生成消息数据
	messageData, err := utils.GenerateRandomData(p.config.Producer.MessageSize)
	if err != nil {
		p.logger.Errorf("生成随机数据失败: %v", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i := 0; i < messageCount; i++ {
		select {
		case <-p.ctx.Done():
			p.logger.Infof("工作协程 %d 收到停止信号", id)
			return
		case <-ticker.C:
			startTime := time.Now()

			// 创建消息
			message := &sarama.ProducerMessage{
				Topic: p.config.Producer.Topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("key-%d-%d", id, i)),
				Value: sarama.ByteEncoder(messageData),
			}

			// 发送消息
			partition, offset, err := p.producer.SendMessage(message)
			latency := time.Since(startTime)

			// 记录指标
			size := int64(len(messageData))
			p.metrics.Record(size, latency, err)

			if err != nil {
				p.logger.Errorf("发送消息失败: %v", err)
			} else {
				p.logger.Debugf("消息发送成功: partition=%d, offset=%d", partition, offset)
			}
		}
	}

	p.logger.Infof("工作协程 %d 完成", id)
}

// Wait 等待生产者测试完成
func (p *Producer) Wait() {
	p.wg.Wait()
}

// GetMetrics 获取性能指标
func (p *Producer) GetMetrics() *metrics.Metrics {
	return p.metrics
}

// Run 运行生产者测试
func Run(cfg *config.Config, log *logger.Logger) error {
	producer := NewProducer(cfg, log)

	if err := producer.Start(); err != nil {
		return fmt.Errorf("启动生产者失败: %w", err)
	}

	// 等待测试完成
	producer.wg.Wait()

	// 获取性能指标
	metrics := producer.GetMetrics()

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
