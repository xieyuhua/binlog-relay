// Package producer 把事件投递到下游。
//
// 所有实现共享同一契约：
//   - Send 收到一批记录，全部成功投递后返回 nil
//   - 返回错误表示整批失败，上层不推进位点、稍后重试
//   - 实现必须尽量批量写，这是吞吐的关键
package producer

import (
	"context"
	"fmt"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
)

// Producer 是下游投递接口
type Producer interface {
	Send(ctx context.Context, envs []*event.Envelope) error
	Name() string
	Close() error
}

// Route 是一条多主题路由规则
type Route struct {
	Target    string
	Databases map[string]struct{}
	Tables    map[string]struct{}
	Ops       map[string]struct{}
}

// Match 判断某条记录是否命中该规则
func (r *Route) Match(meta event.TopicMeta) bool {
	if len(r.Databases) > 0 {
		if _, ok := r.Databases[meta.Database]; !ok {
			return false
		}
	}
	if len(r.Tables) > 0 {
		if _, ok := r.Tables[meta.Table]; !ok {
			if _, star := r.Tables["*"]; !star {
				return false
			}
		}
	}
	if len(r.Ops) > 0 {
		if _, ok := r.Ops[meta.Op]; !ok {
			return false
		}
	}
	return true
}

// BuildRoutes 把配置转成路由表
func BuildRoutes(rules []config.TopicRule) []Route {
	out := make([]Route, 0, len(rules))
	for _, r := range rules {
		out = append(out, Route{
			Target:    r.Target,
			Databases: toSet(r.Databases),
			Tables:    toSet(r.Tables),
			Ops:       toSet(r.Ops),
		})
	}
	return out
}

// MultiTarget 把记录按规则分发到多个目标。
//
// 用于 Redis Stream / Kafka / RabbitMQ 的多主题模式：
// 一条变更可以同时进入多个 topic，各目标独立消费。
type MultiTarget struct {
	routes []Route
	// send 由具体实现提供：投递一组记录到指定目标
	send func(ctx context.Context, target string, envs []*event.Envelope) error
	name string
}

func NewMultiTarget(name string, routes []Route,
	send func(ctx context.Context, target string, envs []*event.Envelope) error) *MultiTarget {
	return &MultiTarget{routes: routes, send: send, name: name}
}

func (m *MultiTarget) Name() string { return m.name }

func (m *MultiTarget) Send(ctx context.Context, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	// 先按目标分组，再逐个目标批量投递：
	// 避免逐条往返，也保证同一目标内的顺序
	grouped := make(map[string][]*event.Envelope)
	for _, env := range envs {
		meta := event.TopicMeta{
			Database: env.DB,
			Table:    env.Table,
			Op:       env.Op,
		}
		for i := range m.routes {
			if m.routes[i].Match(meta) {
				grouped[m.routes[i].Target] = append(grouped[m.routes[i].Target], env)
			}
		}
	}

	// target 顺序固定，便于排查
	targets := make([]string, 0, len(grouped))
	for t := range grouped {
		targets = append(targets, t)
	}
	// 简单排序，避免 map 遍历顺序不稳定影响测试
	sortStrings(targets)

	for _, t := range targets {
		if err := m.send(ctx, t, grouped[t]); err != nil {
			return fmt.Errorf("投递到 %s 失败: %w", t, err)
		}
	}
	return nil
}

func (m *MultiTarget) Close() error { return nil }

func toSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		if it != "" {
			m[it] = struct{}{}
		}
	}
	return m
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// New 按配置构造 Producer
func New(cfg *config.Config) (Producer, error) {
	switch cfg.Producer.Type {
	case "stdout":
		return NewStdout()
	case "redis":
		return NewRedis(cfg)
	case "kafka":
		return NewKafka(cfg)
	case "rabbitmq":
		return NewRabbitMQ(cfg)
	case "http":
		return NewHTTP(cfg)
	case "mysql":
		return NewMySQL(cfg)
	default:
		return nil, fmt.Errorf("未知的 producer 类型: %s", cfg.Producer.Type)
	}
}
