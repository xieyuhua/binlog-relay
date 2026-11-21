// 消费示例 2：Kafka
//
// 适用：v2-go 输出到 Kafka（producer.type=kafka）。
//
// 可靠性要点：
// - 关闭自动提交，处理成功后才 CommitMessages
// - 同一行事件因为 key 相同落在同一分区，分区内有序
// - 崩溃后从上次 offset 重放，下游按 msg.id 幂等
//
//	go run ./examples/consumer/kafka
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/biaoge/binlog2redis/internal/event"
	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := envOr("KAFKA_BROKERS", "127.0.0.1:9092")
	topic := envOr("KAFKA_TOPIC", "binlog-events")
	group := envOr("KAFKA_GROUP", "binlog-consumer")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		GroupID:  group,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10 << 20, // 10MB
		// 一批最多等 1 秒，兼顾吞吐与延迟
		MaxWait: time.Second,
	})
	defer reader.Close()

	log.Printf("消费 %s / %s via %s", topic, group, brokers)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("已停止")
				return
			}
			log.Printf("读取失败: %v", err)
			continue
		}

		if err := process(msg); err != nil {
			// 处理失败不提交 offset，下次会重放
			log.Printf("处理 %s 失败: %v", string(msg.Key), err)
			continue
		}

		// 只有处理成功才提交 offset
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("提交 offset 失败: %v", err)
		}
	}
}

// process 解析并处理单条消息
func process(msg kafka.Message) error {
	var env event.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return fmt.Errorf("解析 JSON 失败: %w", err)
	}

	if env.Kind == "ddl" {
		fmt.Printf("[DDL] %s %s tables=%v\n  %s\n",
			env.Op, env.DB, env.DDLTables, env.DDLQuery)
		return nil
	}

	fmt.Printf("[%s] %s.%s id=%s\n  row=%v\n",
		env.Op, env.DB, env.Table, env.ID, env.Row)

	if env.Before != nil {
		fmt.Printf("  before=%v\n", env.Before)
	}

	// labels 是 topic 级静态标签，可用于区分环境/来源
	if env.Labels["env"] == "prod" {
		// 生产环境专属逻辑
	}

	// 下游落库示例：按 env.ID 幂等
	//   INSERT ... ON DUPLICATE KEY UPDATE ...
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
