package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	brokers      string
	outputFormat string
	configFile   string
	rootCmd      = &cobra.Command{
		Use:   "kafkatest",
		Short: "Kafka 性能测试工具",
		Long:  `一个用于测试 Kafka 生产者和消费者性能的工具，支持多种性能指标收集和报告生成。`,
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// 全局标志
	rootCmd.PersistentFlags().StringVarP(&brokers, "brokers", "b", "localhost:9092", "Kafka 服务器地址，多个地址用逗号分隔")
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "console", "输出格式 (console, json, csv)")
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "配置文件路径")
}
