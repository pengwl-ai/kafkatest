package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"kafkatest/cmd/internal/config"
	"kafkatest/cmd/internal/consumer"
	"kafkatest/cmd/internal/logger"
	"kafkatest/cmd/internal/report"
)

var (
	group       string
	consumerCmd = &cobra.Command{
		Use:   "consumer",
		Short: "Kafka 消费者性能测试",
		Long:  `测试 Kafka 消费者的性能，包括消息消费速率、延迟等指标。`,
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
				cfg.Consumer.Topic = topic
			}
			if cmd.Flags().Changed("group") {
				cfg.Consumer.Group = group
			}
			if cmd.Flags().Changed("concurrency") {
				cfg.Consumer.Concurrency = concurrency
			}
			if cmd.Flags().Changed("duration") {
				cfg.Consumer.Duration = duration
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

			// 创建消费者实例
			cons := consumer.NewConsumer(cfg, log)

			// 启动消费者测试
			if err := cons.Start(); err != nil {
				log.Errorf("启动消费者失败: %v", err)
				os.Exit(1)
			}

			// 等待测试完成
			cons.Wait()

			// 获取性能指标
			metrics := cons.GetMetrics()

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
	rootCmd.AddCommand(consumerCmd)

	// 消费者命令特定标志
	consumerCmd.Flags().StringVarP(&topic, "topic", "t", "test-topic", "Kafka 主题")
	consumerCmd.Flags().StringVarP(&group, "group", "g", "test-group", "消费者组")
	consumerCmd.Flags().IntVarP(&concurrency, "concurrency", "C", 1, "并发数")
	consumerCmd.Flags().DurationVarP(&duration, "duration", "d", 30*time.Second, "测试时长")
}
