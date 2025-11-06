package utils

import (
	"log"
	"net"

	"github.com/IBM/sarama"
)

// DialerProxy 连接kafka broker时做地址代理
type DialerProxy struct {
	addrMap map[string]interface{}
	dialer  *net.Dialer
}

func GetDialerProxy(kafkaConfig *sarama.Config, addrMap map[string]interface{}) *DialerProxy {
	return &DialerProxy{
		addrMap: addrMap,
		dialer: &net.Dialer{
			Timeout:   kafkaConfig.Net.DialTimeout,
			KeepAlive: kafkaConfig.Net.KeepAlive,
			LocalAddr: kafkaConfig.Net.LocalAddr,
		},
	}
}

func (d *DialerProxy) Dial(network, address string) (net.Conn, error) {
	newAddr, ok := d.addrMap[address]
	if ok {
		log.Printf("kafka dial info,oldAddr: %s, newAddr: %v", address, newAddr)
		address = newAddr.(string)
	}

	return d.dialer.Dial(network, address)
}
