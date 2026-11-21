// Package logx 提供结构化日志构造。
package logx

import (
	"io"
	"log/slog"
	"os"
)

// New 按配置创建 slog.Logger。
//
// level: debug | info | warn | error
// format: text | json
// file: 为空时输出到 stderr
func New(level, format, file string) *slog.Logger {
	var w io.Writer = os.Stderr
	if file != "" {
		if f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			w = f
		}
	}

	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
