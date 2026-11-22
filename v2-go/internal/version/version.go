// Package version 提供版本与环境信息。
//
// 版本号可在编译时注入：
//
//	go build -ldflags "-X github.com/biaoge/binlog2redis/internal/version.Version=1.2.0" \
//	         -o bin/binlog2redis ./cmd
package version

import (
	"fmt"
	"runtime"
)

// Version 语义化版本号，编译时可覆盖
var Version = "1.2.0"

// BuildTime 构建时间，编译时可覆盖
var BuildTime = "unknown"

// String 返回版本字符串
func String() string {
	if BuildTime == "unknown" {
		return Version
	}
	return fmt.Sprintf("%s (%s)", Version, BuildTime)
}

// Environment 返回运行时关键信息
func Environment() map[string]string {
	return map[string]string{
		"Go":     runtime.Version(),
		"GOOS":   runtime.GOOS,
		"GOARCH": runtime.GOARCH,
		"CPU":    fmt.Sprintf("%d", runtime.NumCPU()),
	}
}
