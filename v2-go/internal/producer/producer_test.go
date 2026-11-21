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
