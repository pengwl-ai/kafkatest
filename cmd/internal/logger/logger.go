package logger

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// Logger 是应用程序的日志记录器
type Logger struct {
	*logrus.Logger
}

// NewLogger 创建一个新的日志记录器
func NewLogger(level, format, output string) (*Logger, error) {
	log := logrus.New()

	// 设置日志级别
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	log.SetLevel(lvl)

	// 设置日志格式
	switch format {
	case "json":
		log.SetFormatter(&logrus.JSONFormatter{})
	default:
		log.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
	}

	// 设置输出位置
	var out io.Writer
	switch output {
	case "stdout":
		out = os.Stdout
	case "stderr":
		out = os.Stderr
	default:
		// 尝试打开文件
		file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		out = file
	}
	log.SetOutput(out)

	return &Logger{Logger: log}, nil
}

// DefaultLogger 创建一个默认的日志记录器
func DefaultLogger() *Logger {
	logger, _ := NewLogger("info", "text", "stdout")
	return logger
}
