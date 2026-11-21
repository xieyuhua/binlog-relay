// 消费示例 3：RabbitMQ
//
// 适用：v2-go 输出到 RabbitMQ（producer.type=rabbitmq）。
//
// 可靠性要点：
// - 手动 Ack：处理成功才 Ack，失败 Nack 后 requeue
// - Prefetch 限制未确认消息数，避免单个消费者堆积
// - 生产者用 topic 交换机 + routing key，消费端按 binding key 订阅
//
//	go run ./examples/consumer/rabbitmq
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/biaoge/binlog2redis/internal/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchange   = "binlog"
	queueName  = "binlog-events"
	routingKey = "binlog.events"
)

func main() {
	url := envOr("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("连接 RabbitMQ 失败: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("打开 channel 失败: %v", err)
	}
	defer ch.Close()

	// 声明交换机与队列（与生产者侧保持一致，幂等）
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("声明交换机失败: %v", err)
	}
	q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("声明队列失败: %v", err)
	}
	if err := ch.QueueBind(q.Name, routingKey, exchange, false, nil); err != nil {
		log.Fatalf("绑定队列失败: %v", err)
	}

	// Prefetch：限制未确认消息数，避免一个消费者吃掉全部
	if err := ch.Qos(100, 0, false); err != nil {
		log.Fatalf("设置 prefetch 失败: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",    // 自动生成 consumer tag
		false, // autoAck=false，手动确认
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("开始消费失败: %v", err)
	}

	log.Printf("消费 %s / %s", exchange, q.Name)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				log.Println("channel 已关闭")
				return
			}

			if err := process(msg); err != nil {
				log.Printf("处理 %s 失败: %v", msg.MessageId, err)
				// requeue=true：重新入队，稍后重试
				_ = msg.Nack(false, true)
				continue
			}
			// 处理成功才确认
			_ = msg.Ack(false)

		case <-sigCh:
			log.Println("收到停止信号")
			return
		}
	}
}

// process 解析并处理单条消息
func process(msg amqp.Delivery) error {
	var env event.Envelope
	if err := json.Unmarshal(msg.Body, &env); err != nil {
		return fmt.Errorf("解析 JSON 失败: %w", err)
	}

	if env.Kind == "ddl" {
		fmt.Printf("[DDL] %s %s tables=%v\n  %s\n",
			env.Op, env.DB, env.DDLTables, env.DDLQuery)
		return nil
	}

	fmt.Printf("[%s] %s.%s id=%s routing_key=%s\n  row=%v\n",
		env.Op, env.DB, env.Table, env.ID, msg.RoutingKey, env.Row)

	// 生产者把 db/table/op/pos 也放进了 header，可按 header 快速过滤
	if db := msg.Headers["db"]; db != nil {
		_ = db
	}

	// 下游按 msg.MessageId（= 幂等键）或 env.ID 去重
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
