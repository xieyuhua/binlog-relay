package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ 把事件发布到交换机。
//
// 关键取舍：
//   - 开启 publisher confirm 后，Publish 返回即代表 broker 已确认，
//     不会静默丢消息；代价是每批一次往返
//   - 消息设为 persistent，配合 durable 队列可抵抗 broker 重启
//   - 多主题模式用 routing key 区分，消费端按 binding key 订阅
type RabbitMQ struct {
	cfg config.RabbitConfig

	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms chan amqp.Confirmation

	multi *MultiTarget
}

func NewRabbitMQ(cfg *config.Config) (*RabbitMQ, error) {
	r := &RabbitMQ{cfg: cfg.Producer.Rabbit}
	if err := r.ensureChannel(); err != nil {
		return nil, err
	}

	if len(cfg.Producer.Topics) > 0 {
		r.multi = NewMultiTarget("rabbitmq-multi", BuildRoutes(cfg.Producer.Topics), r.publish)
	}
	return r, nil
}

func (r *RabbitMQ) Name() string { return "rabbitmq:" + r.cfg.Exchange }

// ensureChannel 建立连接、声明交换机、开启 confirm
func (r *RabbitMQ) ensureChannel() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensureChannelLocked()
}

func (r *RabbitMQ) ensureChannelLocked() error {
	if r.channel != nil {
		return nil
	}

	conn, err := amqp.Dial(r.cfg.URL)
	if err != nil {
		return fmt.Errorf("RabbitMQ 连接失败: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("打开 channel 失败: %w", err)
	}

	// topic 交换机最灵活：routing key 支持通配
	if err := ch.ExchangeDeclare(
		r.cfg.Exchange, "topic",
		r.cfg.Durable, false, false, false, nil,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("声明交换机失败: %w", err)
	}

	// 单队列模式：确保队列存在并绑定
	if r.cfg.Queue != "" {
		if _, err := ch.QueueDeclare(
			r.cfg.Queue, r.cfg.Durable, false, false, false, nil,
		); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return fmt.Errorf("声明队列失败: %w", err)
		}
		key := r.cfg.RoutingKey
		if key == "" {
			key = r.cfg.Queue
		}
		if err := ch.QueueBind(r.cfg.Queue, key, r.cfg.Exchange, false, nil); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return fmt.Errorf("绑定队列失败: %w", err)
		}
	}

	if r.cfg.Confirm {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return fmt.Errorf("开启 publisher confirm 失败: %w", err)
		}
		r.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 128))
	}

	r.conn, r.channel = conn, ch
	return nil
}

func (r *RabbitMQ) Send(ctx context.Context, envs []*event.Envelope) error {
	if r.multi != nil {
		return r.multi.Send(ctx, envs)
	}
	key := r.cfg.RoutingKey
	if key == "" {
		key = r.cfg.Queue
	}
	return r.publish(ctx, key, envs)
}

func (r *RabbitMQ) publish(ctx context.Context, routingKey string, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	r.mu.Lock()
	if err := r.ensureChannelLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	ch := r.channel
	r.mu.Unlock()

	var lastTag uint64
	for _, env := range envs {
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}

		pub := amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // 落盘，broker 重启不丢
			MessageId:    env.ID,          // 消费端可据此去重
			Timestamp:    time.Unix(env.TS, 0),
			Body:         data,
			Headers: amqp.Table{
				"db":    env.DB,
				"table": env.Table,
				"op":    env.Op,
				"pos":   env.Pos,
			},
		}

		r.mu.Lock()
		err = ch.PublishWithContext(ctx, r.cfg.Exchange, routingKey, false, false, pub)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}
		lastTag++
	}

	// 等待 broker 确认（confirm 模式下每次发布对应一个确认）
	if r.cfg.Confirm && r.confirms != nil {
		deadline := time.After(10 * time.Second)
		for i := uint64(0); i < uint64(len(envs)); i++ {
			select {
			case c := <-r.confirms:
				if !c.Ack {
					return fmt.Errorf("broker 拒绝消息（nack）")
				}
			case <-deadline:
				return fmt.Errorf("等待 broker 确认超时")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	_ = lastTag
	return nil
}

func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.channel != nil {
		_ = r.channel.Close()
		r.channel = nil
	}
	if r.conn != nil {
		err := r.conn.Close()
		r.conn = nil
		return err
	}
	return nil
}
