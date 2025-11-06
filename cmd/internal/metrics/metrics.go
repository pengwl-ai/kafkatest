package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Metrics 表示性能指标
type Metrics struct {
	StartTime    time.Time       `json:"start_time"`
	EndTime      time.Time       `json:"end_time"`
	Duration     time.Duration   `json:"duration"`
	MessageCount int64           `json:"message_count"`
	TotalBytes   int64           `json:"total_bytes"`
	Errors       int64           `json:"errors"`
	Latencies    []time.Duration `json:"latencies"`
	minLatency   time.Duration
	maxLatency   time.Duration
	sumLatency   time.Duration
	mu           sync.Mutex
}

// NewMetrics 创建一个新的性能指标收集器
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime: time.Now(),
		Latencies: make([]time.Duration, 0),
	}
}

// Record 记录一个消息的处理结果
func (m *Metrics) Record(size int64, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.MessageCount++
	m.TotalBytes += size

	if err != nil {
		m.Errors++
	}

	m.Latencies = append(m.Latencies, latency)

	if m.minLatency == 0 || latency < m.minLatency {
		m.minLatency = latency
	}

	if m.maxLatency == 0 || latency > m.maxLatency {
		m.maxLatency = latency
	}

	m.sumLatency += latency
}

// Finish 完成指标收集
func (m *Metrics) Finish() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.EndTime = time.Now()
	m.Duration = m.EndTime.Sub(m.StartTime)
}

// Throughput 返回吞吐量（字节/秒）
func (m *Metrics) Throughput() float64 {
	if m.Duration == 0 {
		return 0
	}
	return float64(m.TotalBytes) / m.Duration.Seconds()
}

// MessageRate 返回消息速率（消息/秒）
func (m *Metrics) MessageRate() float64 {
	if m.Duration == 0 {
		return 0
	}
	return float64(m.MessageCount) / m.Duration.Seconds()
}

// AvgLatency 返回平均延迟
func (m *Metrics) AvgLatency() time.Duration {
	if m.MessageCount == 0 {
		return 0
	}
	return m.sumLatency / time.Duration(m.MessageCount)
}

// MinLatency 返回最小延迟
func (m *Metrics) MinLatency() time.Duration {
	return m.minLatency
}

// MaxLatency 返回最大延迟
func (m *Metrics) MaxLatency() time.Duration {
	return m.maxLatency
}

// Percentile 返回延迟百分位数
func (m *Metrics) Percentile(p float64) time.Duration {
	if len(m.Latencies) == 0 {
		return 0
	}

	// 复制一份延迟切片以避免修改原始数据
	latencies := make([]time.Duration, len(m.Latencies))
	copy(latencies, m.Latencies)

	// 排序
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// 计算百分位数位置
	index := int(math.Ceil(float64(len(latencies))*p/100)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(latencies) {
		index = len(latencies) - 1
	}

	return latencies[index]
}

// P50 返回 P50 延迟
func (m *Metrics) P50() time.Duration {
	return m.Percentile(50)
}

// P90 返回 P90 延迟
func (m *Metrics) P90() time.Duration {
	return m.Percentile(90)
}

// P95 返回 P95 延迟
func (m *Metrics) P95() time.Duration {
	return m.Percentile(95)
}

// P99 返回 P99 延迟
func (m *Metrics) P99() time.Duration {
	return m.Percentile(99)
}

// SuccessRate 返回成功率
func (m *Metrics) SuccessRate() float64 {
	if m.MessageCount == 0 {
		return 0
	}
	return float64(m.MessageCount-m.Errors) / float64(m.MessageCount) * 100
}

// Summary 返回性能指标摘要
func (m *Metrics) Summary() map[string]interface{} {
	return map[string]interface{}{
		"start_time":    m.StartTime,
		"end_time":      m.EndTime,
		"duration":      m.Duration,
		"message_count": m.MessageCount,
		"total_bytes":   m.TotalBytes,
		"errors":        m.Errors,
		"throughput":    m.Throughput(),
		"message_rate":  m.MessageRate(),
		"avg_latency":   m.AvgLatency(),
		"min_latency":   m.MinLatency(),
		"max_latency":   m.MaxLatency(),
		"p50_latency":   m.P50(),
		"p90_latency":   m.P90(),
		"p95_latency":   m.P95(),
		"p99_latency":   m.P99(),
		"success_rate":  m.SuccessRate(),
	}
}
