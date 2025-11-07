package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"net"
)

type KafkaConfig struct {
	BootstrapServers string            `yaml:"bootstrap_servers"`
	AddrMap          map[string]string `yaml:"addr_map"` // 直接使用 map
}

func main() {
	data := []byte(`
kafka:
  bootstrap_servers: "10.22.132.21:9093,10.22.132.22:9093,10.22.132.23:9093"
  addr_map:
    - "10.22.132.21:9093": "10.223.0.11:9092"
    - "10.22.132.22:9093": "10.223.0.12:9092"
    - "10.22.132.23:9093": "10.223.0.13:9092"
`)

	var cfg KafkaConfig
	yaml.Unmarshal(data, &cfg)

	// 验证地址格式
	for k, v := range cfg.AddrMap {
		if _, err := net.ResolveTCPAddr("tcp", k); err != nil {
			panic(fmt.Sprintf("无效的源地址: %s", k))
		}
		if _, err := net.ResolveTCPAddr("tcp", v); err != nil {
			panic(fmt.Sprintf("无效的目标地址: %s", v))
		}
	}

	fmt.Printf("配置解析成功: %v", cfg.AddrMap)
}
