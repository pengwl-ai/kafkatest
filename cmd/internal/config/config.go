package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config 表示应用程序的配置
type Config struct {
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	Producer ProducerConfig `mapstructure:"producer"`
	Consumer ConsumerConfig `mapstructure:"consumer"`
	Output   OutputConfig   `mapstructure:"output"`
	Logging  LoggingConfig  `mapstructure:"logging"`
}

// KafkaConfig 表示 Kafka 相关配置
type KafkaConfig struct {
	Brokers      string                 `mapstructure:"brokers"`
	DialTimeout  time.Duration          `mapstructure:"dial_timeout"`
	WriteTimeout time.Duration          `mapstructure:"write_timeout"`
	ReadTimeout  time.Duration          `mapstructure:"read_timeout"`
	Proxy        bool                   `mapstructure:"proxy"`
	AddrMap      map[string]interface{} `mapstructure:"addr_map"`
}

// ProducerConfig 表示生产者测试配置
type ProducerConfig struct {
	Topic        string        `mapstructure:"topic"`
	MessageSize  int           `mapstructure:"message_size"`
	MessageCount int           `mapstructure:"message_count"`
	Rate         int           `mapstructure:"rate"`
	Concurrency  int           `mapstructure:"concurrency"`
	Duration     time.Duration `mapstructure:"duration"`
	BatchSize    int           `mapstructure:"batch_size"`
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
}

// ConsumerConfig 表示消费者测试配置
type ConsumerConfig struct {
	Topic       string        `mapstructure:"topic"`
	Group       string        `mapstructure:"group"`
	Concurrency int           `mapstructure:"concurrency"`
	Duration    time.Duration `mapstructure:"duration"`
	BatchSize   int           `mapstructure:"batch_size"`
	Offset      string        `mapstructure:"offset"`
}

// OutputConfig 表示输出配置
type OutputConfig struct {
	Format  string `mapstructure:"format"`
	File    string `mapstructure:"file"`
	Verbose bool   `mapstructure:"verbose"`
}

// LoggingConfig 表示日志配置
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	Output string `mapstructure:"output"`
}

// LoadConfig 加载配置
func LoadConfig(configPath string) (*Config, error) {
	// 设置默认值
	setDefaults()

	// 如果指定了配置文件，则加载
	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		// 否则尝试加载默认配置文件
		viper.SetConfigName("config")
		viper.AddConfigPath("./configs")
		viper.AddConfigPath(".")
	}

	// 读取环境变量
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// 读取配置
	if err := viper.ReadInConfig(); err != nil {
		// 如果配置文件不存在，使用默认值
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}

	return &config, nil
}

// setDefaults 设置默认配置值
func setDefaults() {
	// Kafka 默认配置
	viper.SetDefault("kafka.brokers", "localhost:9092")
	viper.SetDefault("kafka.dial_timeout", "10s")
	viper.SetDefault("kafka.write_timeout", "10s")
	viper.SetDefault("kafka.read_timeout", "10s")

	// 生产者默认配置
	viper.SetDefault("producer.topic", "test-topic")
	viper.SetDefault("producer.message_size", 1024)
	viper.SetDefault("producer.message_count", 1000)
	viper.SetDefault("producer.rate", 100)
	viper.SetDefault("producer.concurrency", 1)
	viper.SetDefault("producer.duration", 0)
	viper.SetDefault("producer.batch_size", 100)
	viper.SetDefault("producer.batch_timeout", "1s")

	// 消费者默认配置
	viper.SetDefault("consumer.topic", "test-topic")
	viper.SetDefault("consumer.group", "test-group")
	viper.SetDefault("consumer.concurrency", 1)
	viper.SetDefault("consumer.duration", "30s")
	viper.SetDefault("consumer.batch_size", 100)
	viper.SetDefault("consumer.offset", "latest")

	// 输出默认配置
	viper.SetDefault("output.format", "console")
	viper.SetDefault("output.file", "")
	viper.SetDefault("output.verbose", false)

	// 日志默认配置
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "text")
	viper.SetDefault("logging.output", "stdout")
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Kafka.Brokers == "" {
		return fmt.Errorf("Kafka 服务器地址不能为空")
	}

	if c.Producer.Topic == "" {
		return fmt.Errorf("生产者主题不能为空")
	}

	if c.Producer.MessageSize <= 0 {
		return fmt.Errorf("消息大小必须大于 0")
	}

	if c.Producer.MessageCount <= 0 {
		return fmt.Errorf("消息数量必须大于 0")
	}

	if c.Producer.Rate <= 0 {
		return fmt.Errorf("发送速率必须大于 0")
	}

	if c.Producer.Concurrency <= 0 {
		return fmt.Errorf("并发数必须大于 0")
	}

	if c.Consumer.Topic == "" {
		return fmt.Errorf("消费者主题不能为空")
	}

	if c.Consumer.Group == "" {
		return fmt.Errorf("消费者组不能为空")
	}

	if c.Consumer.Concurrency <= 0 {
		return fmt.Errorf("并发数必须大于 0")
	}

	if c.Consumer.Duration <= 0 {
		return fmt.Errorf("测试时长必须大于 0")
	}

	validFormats := map[string]bool{
		"console": true,
		"json":    true,
		"csv":     true,
	}
	if !validFormats[c.Output.Format] {
		return fmt.Errorf("不支持的输出格式: %s", c.Output.Format)
	}

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLevels[c.Logging.Level] {
		return fmt.Errorf("不支持的日志级别: %s", c.Logging.Level)
	}

	return nil
}
