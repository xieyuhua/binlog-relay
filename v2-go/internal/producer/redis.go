package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
	"github.com/redis/go-redis/v9"
)

// Redis 把事件写入 Redis Stream。
//
// 单主题：全部写 cfg.Producer.Redis.Stream
// 多主题：按 producer.topics 规则路由到不同 stream
type Redis struct {
	client *redis.Client
	stream string
	maxLen int64

	// pipeline 一次网络往返写入整批，显著降低延迟
	multi *MultiTarget
}

func NewRedis(cfg *config.Config) (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Producer.Redis.Addr,
		Password: cfg.Producer.Redis.Password,
		DB:       cfg.Producer.Redis.DB,
		// 写超时：Redis 慢时快速失败，避免拖住整个管道
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("Redis 连接失败 (%s): %w", cfg.Producer.Redis.Addr, err)
	}

	r := &Redis{
		client: client,
		stream: cfg.Producer.Redis.Stream,
		maxLen: cfg.Producer.Redis.MaxLen,
	}

	if len(cfg.Producer.Topics) > 0 {
		r.multi = NewMultiTarget("redis-multi", BuildRoutes(cfg.Producer.Topics), r.sendToStream)
	}
	return r, nil
}

func (r *Redis) Name() string { return "redis:" + r.stream }

func (r *Redis) Send(ctx context.Context, envs []*event.Envelope) error {
	if r.multi != nil {
		return r.multi.Send(ctx, envs)
	}
	return r.sendToStream(ctx, r.stream, envs)
}

// sendToStream 用 pipeline 把整批 XADD 一次发出
func (r *Redis) sendToStream(ctx context.Context, stream string, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, env := range envs {
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}

		args := &redis.XAddArgs{
			Stream: stream,
			ID:     "*",
			Values: []any{
				"id", env.ID,
				"kind", env.Kind,
				"op", env.Op,
				"db", env.DB,
				"tbl", env.Table,
				"pos", env.Pos,
				"ts", env.TS,
				"data", string(data),
			},
		}
		if r.maxLen > 0 {
			// 近似裁剪是 O(1)，不会阻塞写入
			args.MaxLen = r.maxLen
			args.Approx = true
		}
		pipe.XAdd(ctx, args)
	}

	// pipeline 内任一命令失败都会在这里返回
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("XADD 失败: %w", err)
	}
	return nil
}

func (r *Redis) Close() error { return r.client.Close() }
