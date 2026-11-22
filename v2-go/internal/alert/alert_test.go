package alert

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type countingNotifier struct {
	mu    sync.Mutex
	count int
	fails bool
}

func (c *countingNotifier) Send(title, message string, fields map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	if c.fails {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (c *countingNotifier) Name() string { return "counting" }

func (c *countingNotifier) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestManagerEmptyIsNoop(t *testing.T) {
	m := NewManager(quietLogger(), time.Minute, 10)
	if !m.IsEmpty() {
		t.Fatal("未配置通道时 IsEmpty 应为 true")
	}
	// 不应 panic
	m.Alert("k", "t", "m", nil)
}

func TestManagerDedup(t *testing.T) {
	n := &countingNotifier{}
	m := NewManager(quietLogger(), time.Minute, 100)
	m.Add(n)

	m.Alert("same-key", "标题", "消息", nil)
	m.Alert("same-key", "标题", "消息", nil)
	m.Alert("same-key", "标题", "消息", nil)

	if got := n.Count(); got != 1 {
		t.Fatalf("同一 key 在冷却期内应只发 1 次，实际 %d", got)
	}

	// 不同 key 应各发一次
	m.Alert("other-key", "标题", "消息", nil)
	if got := n.Count(); got != 2 {
		t.Fatalf("不同 key 应分别发送，实际 %d", got)
	}
}

func TestManagerCooldownExpiry(t *testing.T) {
	n := &countingNotifier{}
	// 冷却时间设为极小值，模拟过期
	m := NewManager(quietLogger(), time.Nanosecond, 100)
	m.Add(n)

	m.Alert("k", "标题", "消息", nil)
	time.Sleep(2 * time.Millisecond)
	m.Alert("k", "标题", "消息", nil)

	if got := n.Count(); got != 2 {
		t.Fatalf("冷却过期后应重新发送，实际 %d", got)
	}
}

func TestManagerRateLimit(t *testing.T) {
	n := &countingNotifier{}
	m := NewManager(quietLogger(), time.Nanosecond, 3) // 每分钟最多 3 条
	m.Add(n)

	for i := 0; i < 10; i++ {
		m.Alert(string(rune('a'+i)), "标题", "消息", nil)
		time.Sleep(time.Millisecond)
	}

	if got := n.Count(); got != 3 {
		t.Fatalf("应被限流到 3 条，实际 %d", got)
	}
}

func TestManagerNotifierFailureIsolation(t *testing.T) {
	bad := &countingNotifier{fails: true}
	good := &countingNotifier{}
	m := NewManager(quietLogger(), time.Minute, 100)
	m.Add(bad)
	m.Add(good)

	// 一个通道失败不应影响另一个，也不应 panic
	m.Alert("k", "标题", "消息", nil)

	if bad.Count() != 1 || good.Count() != 1 {
		t.Fatalf("两个通道都应被调用: bad=%d good=%d", bad.Count(), good.Count())
	}
}

func TestRenderTruncate(t *testing.T) {
	// 超过 4000 字节应被截断并带提示
	long := make([]byte, 6000)
	for i := range long {
		long[i] = 'x'
	}
	out := truncate(string(long))
	if len(out) > maxContentBytes+50 {
		t.Fatalf("截断后仍过长: %d", len(out))
	}
	if !contains(out, "已截断") {
		t.Fatal("截断后应带提示")
	}
}

func TestRenderWeComIncludesFields(t *testing.T) {
	out := renderWeCom("测试标题", "测试消息", map[string]string{"表": "orders"})
	if !contains(out, "测试标题") || !contains(out, "orders") {
		t.Fatalf("渲染结果缺少内容: %s", out)
	}
}

func TestRenderDingTalkKeyword(t *testing.T) {
	// 关键词模式：消息必须以关键词开头，否则钉钉会拒绝
	out := renderDingTalk("告警", "标题", "消息", nil)
	if !contains(out[:20], "告警") {
		t.Fatalf("关键词应出现在开头: %s", out[:20])
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
