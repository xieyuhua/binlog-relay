package producer

import (
	"context"
	"testing"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
)

func TestRouteMatch(t *testing.T) {
	cases := []struct {
		name  string
		rule  config.TopicRule
		meta  event.TopicMeta
		want  bool
	}{
		{"全表通配", config.TopicRule{Tables: []string{"*"}}, event.TopicMeta{Table: "orders"}, true},
		{"表名命中", config.TopicRule{Tables: []string{"orders"}}, event.TopicMeta{Table: "orders"}, true},
		{"表名不命中", config.TopicRule{Tables: []string{"orders"}}, event.TopicMeta{Table: "users"}, false},
		{"操作命中", config.TopicRule{Tables: []string{"orders"}, Ops: []string{"delete"}},
			event.TopicMeta{Table: "orders", Op: "delete"}, true},
		{"操作不命中", config.TopicRule{Tables: []string{"orders"}, Ops: []string{"delete"}},
			event.TopicMeta{Table: "orders", Op: "insert"}, false},
		{"库命中", config.TopicRule{Databases: []string{"shop"}},
			event.TopicMeta{Database: "shop", Table: "x"}, true},
		{"库不命中", config.TopicRule{Databases: []string{"shop"}},
			event.TopicMeta{Database: "other", Table: "x"}, false},
	}

	for _, c := range cases {
		routes := BuildRoutes([]config.TopicRule{c.rule})
		if got := routes[0].Match(c.meta); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestMultiTargetFanout(t *testing.T) {
	// 一条变更同时命中两条规则 → 应分发到两个目标
	rules := BuildRoutes([]config.TopicRule{
		{Target: "topic:all", Tables: []string{"*"}},
		{Target: "topic:audit", Tables: []string{"orders"}, Ops: []string{"delete"}},
	})

	sent := map[string]int{}
	m := NewMultiTarget("test", rules, func(_ context.Context, target string, envs []*event.Envelope) error {
		sent[target] += len(envs)
		return nil
	})

	envs := []*event.Envelope{
		{DB: "shop", Table: "orders", Op: "delete"},
		{DB: "shop", Table: "orders", Op: "insert"},
		{DB: "shop", Table: "users", Op: "insert"},
	}
	if err := m.Send(context.Background(), envs); err != nil {
		t.Fatal(err)
	}

	if sent["topic:all"] != 3 {
		t.Errorf("topic:all 应收到 3 条，实际 %d", sent["topic:all"])
	}
	if sent["topic:audit"] != 1 {
		t.Errorf("topic:audit 应只收到 1 条 delete，实际 %d", sent["topic:audit"])
	}
}

func TestMultiTargetEmpty(t *testing.T) {
	m := NewMultiTarget("test", nil, func(context.Context, string, []*event.Envelope) error {
		t.Fatal("空批次不应调用 send")
		return nil
	})
	if err := m.Send(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestMultiTargetNoMatch(t *testing.T) {
	rules := BuildRoutes([]config.TopicRule{
		{Target: "topic:x", Tables: []string{"nonexistent"}},
	})
	called := false
	m := NewMultiTarget("test", rules, func(context.Context, string, []*event.Envelope) error {
		called = true
		return nil
	})
	_ = m.Send(context.Background(), []*event.Envelope{{DB: "shop", Table: "orders", Op: "insert"}})
	if called {
		t.Fatal("未命中任何规则时不应投递")
	}
}

// labels 应按目标注入，且不同目标之间互不污染
func TestMultiTargetLabelsIsolation(t *testing.T) {
	rules := BuildRoutes([]config.TopicRule{
		{Target: "prod", Tables: []string{"*"}, Labels: map[string]string{"env": "prod"}},
		{Target: "stage", Tables: []string{"*"}, Labels: map[string]string{"env": "stage"}},
	})

	got := map[string]map[string]string{}
	m := NewMultiTarget("test", rules, func(_ context.Context, target string, envs []*event.Envelope) error {
		if len(envs) > 0 {
			got[target] = envs[0].Labels
		}
		return nil
	})

	env := &event.Envelope{ID: "shop.orders:1", DB: "shop", Table: "orders", Op: "insert"}
	if err := m.Send(context.Background(), []*event.Envelope{env}); err != nil {
		t.Fatal(err)
	}

	if got["prod"]["env"] != "prod" {
		t.Errorf("prod labels = %v", got["prod"])
	}
	if got["stage"]["env"] != "stage" {
		t.Errorf("stage labels = %v", got["stage"])
	}
	// 原始信封不应被污染
	if env.Labels != nil {
		t.Errorf("原始信封被修改: %v", env.Labels)
	}
}

// 无 labels 的规则不应复制信封（零开销路径）
func TestMultiTargetNoLabelsNoClone(t *testing.T) {
	rules := BuildRoutes([]config.TopicRule{{Target: "t", Tables: []string{"*"}}})

	var received *event.Envelope
	m := NewMultiTarget("test", rules, func(_ context.Context, _ string, envs []*event.Envelope) error {
		received = envs[0]
		return nil
	})

	env := &event.Envelope{ID: "x", DB: "d", Table: "t", Op: "insert"}
	_ = m.Send(context.Background(), []*event.Envelope{env})

	if received != env {
		t.Error("没有 labels 时应复用同一对象，避免无谓复制")
	}
}
