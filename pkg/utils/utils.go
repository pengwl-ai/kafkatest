package utils

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

// GenerateRandomString 生成指定长度的随机字符串
func GenerateRandomString(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)

	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		result[i] = charset[num.Int64()]
	}

	return string(result), nil
}

// GenerateRandomData 生成指定大小的随机数据
func GenerateRandomData(size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// FormatDuration 格式化时间持续时间为易读的字符串
func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2f µs", float64(d.Nanoseconds())/1000)
	} else if d < time.Second {
		return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000)
	} else if d < time.Minute {
		return fmt.Sprintf("%.2f s", float64(d.Milliseconds())/1000)
	} else if d < time.Hour {
		return fmt.Sprintf("%.2f m", float64(d.Seconds())/60)
	} else {
		return fmt.Sprintf("%.2f h", float64(d.Minutes())/60)
	}
}

// FormatBytes 格式化字节数为易读的字符串
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// ParseDuration 解析时间持续期字符串
func ParseDuration(s string) (time.Duration, error) {
	// 尝试标准解析
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// 尝试自定义格式
	var value int
	var unit string
	_, err := fmt.Sscanf(s, "%d%s", &value, &unit)
	if err != nil {
		return 0, fmt.Errorf("无效的持续时间格式: %s", s)
	}

	switch unit {
	case "ns", "纳秒":
		return time.Duration(value) * time.Nanosecond, nil
	case "us", "µs", "微秒":
		return time.Duration(value) * time.Microsecond, nil
	case "ms", "毫秒":
		return time.Duration(value) * time.Millisecond, nil
	case "s", "秒", "second", "seconds":
		return time.Duration(value) * time.Second, nil
	case "m", "分钟", "minute", "minutes":
		return time.Duration(value) * time.Minute, nil
	case "h", "小时", "hour", "hours":
		return time.Duration(value) * time.Hour, nil
	default:
		return 0, fmt.Errorf("未知的时间单位: %s", unit)
	}
}

// MinInt 返回两个整数中的较小值
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt 返回两个整数中的较大值
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinInt64 返回两个 int64 中的较小值
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxInt64 返回两个 int64 中的较大值
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ClampInt 将整数限制在指定范围内
func ClampInt(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// SafeClose 安全关闭通道
func SafeClose(ch chan struct{}) {
	select {
	case <-ch:
		// 通道已关闭
	default:
		close(ch)
	}
}
