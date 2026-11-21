package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
	"github.com/segmentio/kafka-go"
)

// Kafka 把事件写入 Kafka。
//
// 关键取舍：
// - 用 Sync 写（RequiredAcks=-1）保证不丢，代价是每批一次往返
// - key 用幂等键，保证同一行进入同一分区，从而保序
// - 批量参数（BatchBytes/BatchTimeout）直接影响吞吐与延迟
type Kafka struct {
	writer *kafka.Writer
	topic  string
	multi  *MultiTarget
	// topic 模式下每个主题一个 writer（懒创建）
	writers map[string]*kafka.Writer
	cfg     config.KafkaConfig
}

func NewKafka(cfg *config.Config) (*Kafka, error) {
	if len(cfg.Producer.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("kafka 缺少 brokers")
	}

	k := &Kafka{
		topic:   cfg.Producer.Kafka.Topic,
		cfg:     cfg.Producer.Kafka,
		writers: make(map[string]*kafka.Writer),
	}

	if len(cfg.Producer.Topics) > 0 {
		k.multi = NewMultiTarget("kafka-multi", BuildRoutes(cfg.Producer.Topics), k.sendToTopic)
	} else {
		k.writer = k.newWriter(k.topic)
	}
	return k, nil
}

func (k *Kafka) newWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(k.cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // 按 key 哈希，同一行保序
		RequiredAcks: kafka.RequiredAcks(k.cfg.RequiredAcks),
		BatchBytes:   int64(k.cfg.BatchBytes),
		BatchTimeout: k.cfg.BatchTimeout,
		Compression:  parseCompression(k.cfg.Compression),
		// 不自动创建主题，避免误配置导致静默创建
		AllowAutoTopicCreation: false,
	}
}

func (k *Kafka) Name() string { return "kafka:" + k.topic }

func (k *Kafka) Send(ctx context.Context, envs []*event.Envelope) error {
	if k.multi != nil {
		return k.multi.Send(ctx, envs)
	}
	return k.sendToTopic(ctx, k.topic, envs)
}

func (k *Kafka) sendToTopic(ctx context.Context, topic string, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	w, ok := k.writers[topic]
	if !ok {
		if k.writer != nil && topic == k.topic {
			w = k.writer
		} else {
			w = k.newWriter(topic)
		}
		k.writers[topic] = w
	}

	msgs := make([]kafka.Message, 0, len(envs))
	for _, env := range envs {
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(env.ID), // 同一行落同一分区
			Value: data,
			Time:  time.Unix(env.TS, 0),
		})
	}

	if err := w.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("Kafka 写入失败 (topic=%s): %w", topic, err)
	}
	return nil
}

func (k *Kafka) Close() error {
	var firstErr error
	for _, w := range k.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if k.writer != nil {
		if err := k.writer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func parseCompression(s string) kafka.Compression {
	switch s {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return 0 // 不压缩
	}
}
