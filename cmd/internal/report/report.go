package report

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"kafkatest/cmd/internal/metrics"
	"os"
	"text/tabwriter"
)

// Reporter 接口定义了报告生成的方法
type Reporter interface {
	Generate(*metrics.Metrics) error
}

// ConsoleReporter 控制台报告生成器
type ConsoleReporter struct {
	Verbose bool
}

// NewConsoleReporter 创建一个新的控制台报告生成器
func NewConsoleReporter(verbose bool) *ConsoleReporter {
	return &ConsoleReporter{Verbose: verbose}
}

// Generate 生成控制台报告
func (r *ConsoleReporter) Generate(m *metrics.Metrics) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "Kafka 性能测试报告")
	fmt.Fprintln(w, "==================")
	fmt.Fprintln(w)

	fmt.Fprintf(w, "开始时间:\t%s\n", m.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "结束时间:\t%s\n", m.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "测试时长:\t%s\n", m.Duration)
	fmt.Fprintln(w)

	fmt.Fprintln(w, "基本指标")
	fmt.Fprintln(w, "--------")
	fmt.Fprintf(w, "消息总数:\t%d\n", m.MessageCount)
	fmt.Fprintf(w, "总字节数:\t%d\n", m.TotalBytes)
	fmt.Fprintf(w, "错误数:\t%d\n", m.Errors)
	fmt.Fprintf(w, "成功率:\t%.2f%%\n", m.SuccessRate())
	fmt.Fprintln(w)

	fmt.Fprintln(w, "性能指标")
	fmt.Fprintln(w, "--------")
	fmt.Fprintf(w, "吞吐量:\t%.2f 字节/秒\n", m.Throughput())
	fmt.Fprintf(w, "消息速率:\t%.2f 消息/秒\n", m.MessageRate())
	fmt.Fprintln(w)

	fmt.Fprintln(w, "延迟指标")
	fmt.Fprintln(w, "--------")
	fmt.Fprintf(w, "平均延迟:\t%s\n", m.AvgLatency())
	fmt.Fprintf(w, "最小延迟:\t%s\n", m.MinLatency())
	fmt.Fprintf(w, "最大延迟:\t%s\n", m.MaxLatency())
	fmt.Fprintf(w, "P50 延迟:\t%s\n", m.P50())
	fmt.Fprintf(w, "P90 延迟:\t%s\n", m.P90())
	fmt.Fprintf(w, "P95 延迟:\t%s\n", m.P95())
	fmt.Fprintf(w, "P99 延迟:\t%s\n", m.P99())
	fmt.Fprintln(w)

	if r.Verbose {
		fmt.Fprintln(w, "详细延迟数据")
		fmt.Fprintln(w, "------------")
		for i, latency := range m.Latencies {
			fmt.Fprintf(w, "消息 %d:\t%s\n", i+1, latency)
			if i >= 99 { // 只显示前100条
				fmt.Fprintf(w, "...\n")
				break
			}
		}
		fmt.Fprintln(w)
	}

	return nil
}

// JSONReporter JSON 报告生成器
type JSONReporter struct {
	OutputFile string
}

// NewJSONReporter 创建一个新的 JSON 报告生成器
func NewJSONReporter(outputFile string) *JSONReporter {
	return &JSONReporter{OutputFile: outputFile}
}

// Generate 生成 JSON 报告
func (r *JSONReporter) Generate(m *metrics.Metrics) error {
	summary := m.Summary()
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("生成 JSON 报告失败: %w", err)
	}

	if r.OutputFile == "" {
		fmt.Println(string(data))
		return nil
	}

	file, err := os.Create(r.OutputFile)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("写入输出文件失败: %w", err)
	}

	fmt.Printf("JSON 报告已保存到: %s\n", r.OutputFile)
	return nil
}

// CSVReporter CSV 报告生成器
type CSVReporter struct {
	OutputFile string
}

// NewCSVReporter 创建一个新的 CSV 报告生成器
func NewCSVReporter(outputFile string) *CSVReporter {
	return &CSVReporter{OutputFile: outputFile}
}

// Generate 生成 CSV 报告
func (r *CSVReporter) Generate(m *metrics.Metrics) error {
	var file *os.File
	var err error

	if r.OutputFile == "" {
		file = os.Stdout
	} else {
		file, err = os.Create(r.OutputFile)
		if err != nil {
			return fmt.Errorf("创建输出文件失败: %w", err)
		}
		defer file.Close()
	}

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题行
	headers := []string{
		"开始时间", "结束时间", "测试时长(秒)", "消息总数", "总字节数", "错误数",
		"吞吐量(字节/秒)", "消息速率(消息/秒)", "成功率(%)",
		"平均延迟(毫秒)", "最小延迟(毫秒)", "最大延迟(毫秒)",
		"P50延迟(毫秒)", "P90延迟(毫秒)", "P95延迟(毫秒)", "P99延迟(毫秒)",
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("写入 CSV 标题失败: %w", err)
	}

	// 写入数据行
	record := []string{
		m.StartTime.Format("2006-01-02 15:04:05"),
		m.EndTime.Format("2006-01-02 15:04:05"),
		fmt.Sprintf("%.2f", m.Duration.Seconds()),
		fmt.Sprintf("%d", m.MessageCount),
		fmt.Sprintf("%d", m.TotalBytes),
		fmt.Sprintf("%d", m.Errors),
		fmt.Sprintf("%.2f", m.Throughput()),
		fmt.Sprintf("%.2f", m.MessageRate()),
		fmt.Sprintf("%.2f", m.SuccessRate()),
		fmt.Sprintf("%.2f", float64(m.AvgLatency().Nanoseconds())/1e6),
		fmt.Sprintf("%.2f", float64(m.MinLatency().Nanoseconds())/1e6),
		fmt.Sprintf("%.2f", float64(m.MaxLatency().Nanoseconds())/1e6),
		fmt.Sprintf("%.2f", float64(m.P50().Nanoseconds())/1e6),
		fmt.Sprintf("%.2f", float64(m.P90().Nanoseconds())/1e6),
		fmt.Sprintf("%.2f", float64(m.P95().Nanoseconds())/1e6),
		fmt.Sprintf("%.2f", float64(m.P99().Nanoseconds())/1e6),
	}
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("写入 CSV 数据失败: %w", err)
	}

	if r.OutputFile != "" {
		fmt.Printf("CSV 报告已保存到: %s\n", r.OutputFile)
	}

	return nil
}

// NewReporter 根据格式创建相应的报告生成器
func NewReporter(format, outputFile string, verbose bool) (Reporter, error) {
	switch format {
	case "console":
		return NewConsoleReporter(verbose), nil
	case "json":
		return NewJSONReporter(outputFile), nil
	case "csv":
		return NewCSVReporter(outputFile), nil
	default:
		return nil, fmt.Errorf("不支持的报告格式: %s", format)
	}
}
