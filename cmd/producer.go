package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"kafkatest/cmd/internal/config"
	"kafkatest/cmd/internal/logger"
	"kafkatest/cmd/internal/producer"
	"kafkatest/cmd/internal/report"
)

var (
	topic        string
	messageSize  int
	messageCount int
	rate         int
	concurrency  int
	duration     time.Duration
	producerCmd  = &cobra.Command{
		Use:   "producer",
		Short: "Kafka 生产者性能测试",
		Long:  `测试 Kafka 生产者的性能，包括消息发送速率、延迟等指标。`,
		Run: func(cmd *cobra.Command, args []string) {
			// 加载配置
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				fmt.Printf("加载配置失败: %v\n", err)
				os.Exit(1)
			}

			// 应用命令行参数覆盖配置
			if cmd.Flags().Changed("brokers") {
				cfg.Kafka.Brokers = brokers
			}
			if cmd.Flags().Changed("topic") {
				cfg.Producer.Topic = topic
			}
			if cmd.Flags().Changed("message-size") {
				cfg.Producer.MessageSize = messageSize
			}
			if cmd.Flags().Changed("message-count") {
				cfg.Producer.MessageCount = messageCount
			}
			if cmd.Flags().Changed("rate") {
				cfg.Producer.Rate = rate
			}
			if cmd.Flags().Changed("concurrency") {
				cfg.Producer.Concurrency = concurrency
			}
			if cmd.Flags().Changed("duration") {
				cfg.Producer.Duration = duration
			}
			if cmd.Flags().Changed("output") {
				cfg.Output.Format = outputFormat
			}

			// 验证配置
			if err := cfg.Validate(); err != nil {
				fmt.Printf("配置验证失败: %v\n", err)
				os.Exit(1)
			}

			// 创建日志记录器
			log, err := logger.NewLogger(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output)
			if err != nil {
				fmt.Printf("创建日志记录器失败: %v\n", err)
				os.Exit(1)
			}

			// 创建生产者实例
			prod := producer.NewProducer(cfg, log)

			// 启动生产者测试
			if err := prod.Start(); err != nil {
				log.Errorf("启动生产者失败: %v", err)
				os.Exit(1)
			}

			// 等待测试完成
			prod.Wait()

			// 获取性能指标
			metrics := prod.GetMetrics()

			// 生成报告
			reporter, err := report.NewReporter(cfg.Output.Format, cfg.Output.File, cfg.Output.Verbose)
			if err != nil {
				log.Errorf("创建报告生成器失败: %v", err)
				os.Exit(1)
			}

			if err := reporter.Generate(metrics); err != nil {
				log.Errorf("生成报告失败: %v", err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	rootCmd.AddCommand(producerCmd)

	// 生产者命令特定标志
	producerCmd.Flags().StringVarP(&topic, "topic", "t", "test-topic", "Kafka 主题")
	producerCmd.Flags().IntVarP(&messageSize, "message-size", "s", 1024, "消息大小（字节）")
	producerCmd.Flags().IntVarP(&messageCount, "message-count", "n", 1000, "消息数量")
	producerCmd.Flags().IntVarP(&rate, "rate", "r", 100, "发送速率（消息/秒）")
	producerCmd.Flags().IntVarP(&concurrency, "concurrency", "C", 1, "并发数")
	producerCmd.Flags().DurationVarP(&duration, "duration", "d", 0, "测试时长（0 表示不限制）")
}
