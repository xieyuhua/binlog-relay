package main

import (
	"fmt"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/redis/go-redis/v9"
)

// newRedisClient 按配置建 Redis 客户端（位点存储与输出端共用）
func newRedisClient(cfg config.RedisConfig) (*redis.Client, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("Redis 地址未配置")
	}
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}), nil
}
