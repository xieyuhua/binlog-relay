// Package alert 提供企微/钉钉群机器人告警。
//
// 两个保护机制，避免故障时把机器人限流打满：
//   - 去重：同一 key 在 cooldown 内只发一次
//   - 限流：全局每分钟最多 N 条（企微/钉钉限制均为 20 条/分钟）
package alert

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const maxContentBytes = 4000

// Notifier 是一个告警通道
type Notifier interface {
	Send(title, message string, fields map[string]string) error
	Name() string
}

// Manager 负责多渠道分发、去重与限流
type Manager struct {
	notifiers []Notifier
	log       *slog.Logger
	cooldown  time.Duration
	maxPerMin int

	mu          sync.Mutex
	lastSent    map[string]time.Time
	recentSends []time.Time
}

// NewManager 构造告警管理器
func NewManager(log *slog.Logger, cooldown time.Duration, maxPerMinute int) *Manager {
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	if maxPerMinute <= 0 {
		maxPerMinute = 15
	}
	return &Manager{
		log:       log,
		cooldown:  cooldown,
		maxPerMin: maxPerMinute,
		lastSent:  make(map[string]time.Time),
	}
}

// Add 注册一个告警通道
func (m *Manager) Add(n Notifier) { m.notifiers = append(m.notifiers, n) }

// IsEmpty 判断是否配置了任何通道
func (m *Manager) IsEmpty() bool { return len(m.notifiers) == 0 }

// Alert 发送告警。
//
// key 用于去重：同一 key 在 cooldown 内只发一次。
// 未配置任何通道时是空操作。
func (m *Manager) Alert(key, title, message string, fields map[string]string) {
	if len(m.notifiers) == 0 {
		return
	}

	now := time.Now()

	m.mu.Lock()
	if last, ok := m.lastSent[key]; ok && now.Sub(last) < m.cooldown {
		m.mu.Unlock()
		return
	}

	// 清理一分钟前的记录并检查限流
	cutoff := now.Add(-time.Minute)
	kept := m.recentSends[:0]
	for _, t := range m.recentSends {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	m.recentSends = kept

	if len(m.recentSends) >= m.maxPerMin {
		m.mu.Unlock()
		m.log.Warn("告警已达每分钟上限，本条被丢弃", "key", key, "limit", m.maxPerMin)
		return
	}

	m.lastSent[key] = now
	m.recentSends = append(m.recentSends, now)
	m.mu.Unlock()

	for _, n := range m.notifiers {
		if err := n.Send(title, message, fields); err != nil {
			// 告警通道故障绝不能影响主流程
			m.log.Warn("告警发送异常", "channel", n.Name(), "err", err)
		}
	}
	m.log.Info("已发送告警", "key", key, "title", title)
}

// ---------- 企业微信群机器人 ----------

// WeCom 企业微信群机器人
type WeCom struct {
	Key    string
	Client *http.Client
}

func NewWeCom(key string) *WeCom {
	return &WeCom{Key: key, Client: &http.Client{Timeout: 5 * time.Second}}
}

func (w *WeCom) Name() string { return "wecom" }

func (w *WeCom) Send(title, message string, fields map[string]string) error {
	content := renderWeCom(title, message, fields)

	payload := map[string]any{
		"msgtype":  "markdown",
		"markdown": map[string]string{"content": content},
	}
	return postJSON(w.Client,
		"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="+url.QueryEscape(w.Key),
		payload)
}

func renderWeCom(title, message string, fields map[string]string) string {
	// <font color="warning"> 是企微 markdown 支持的告警色
	var b bytes.Buffer
	fmt.Fprintf(&b, "## <font color=\"warning\">%s</font>\n\n", title)
	if message != "" {
		b.WriteString(message)
		b.WriteString("\n\n")
	}
	for k, v := range fields {
		fmt.Fprintf(&b, "> **%s**：%s\n", k, v)
	}
	return truncate(b.String())
}

// ---------- 钉钉群机器人 ----------

// DingTalk 钉钉群机器人
type DingTalk struct {
	Token   string
	Secret  string // 加签密钥；用关键词或 IP 白名单时留空
	Keyword string // 关键词模式时必须出现在消息里
	Client  *http.Client
}

func NewDingTalk(token, secret, keyword string) *DingTalk {
	return &DingTalk{
		Token:   token,
		Secret:  secret,
		Keyword: keyword,
		Client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *DingTalk) Name() string { return "dingtalk" }

func (d *DingTalk) Send(title, message string, fields map[string]string) error {
	endpoint := "https://oapi.dingtalk.com/robot/send?access_token=" + url.QueryEscape(d.Token)

	// 加签：timestamp 为毫秒，sign = base64(hmac_sha256(secret, timestamp+"\n"+secret))
	if d.Secret != "" {
		ts := fmt.Sprintf("%d", time.Now().UnixMilli())
		mac := hmac.New(sha256.New, []byte(d.Secret))
		mac.Write([]byte(ts + "\n" + d.Secret))
		sign := url.QueryEscape(base64.StdEncoding.EncodeToString(mac.Sum(nil)))
		endpoint += "&timestamp=" + ts + "&sign=" + sign
	}

	content := renderDingTalk(d.Keyword, title, message, fields)
	payload := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": title,
			"text":  content,
		},
	}
	return postJSON(d.Client, endpoint, payload)
}

func renderDingTalk(keyword, title, message string, fields map[string]string) string {
	prefix := ""
	if keyword != "" {
		prefix = keyword + " "
	}

	var b bytes.Buffer
	fmt.Fprintf(&b, "### %s%s\n\n", prefix, title)
	if message != "" {
		b.WriteString(message)
		b.WriteString("\n\n")
	}
	for k, v := range fields {
		fmt.Fprintf(&b, "- **%s**：%s\n", k, v)
	}
	return truncate(b.String())
}

// ---------- 公共 ----------

func postJSON(client *http.Client, endpoint string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := client.Post(endpoint, "application/json; charset=utf-8", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, respBody)
	}

	// 企微/钉钉即使 HTTP 200 也可能在 body 里返回 errcode != 0
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(respBody, &result); err == nil && result.ErrCode != 0 {
		return fmt.Errorf("平台拒绝 (errcode=%d): %s", result.ErrCode, result.ErrMsg)
	}
	return nil
}

// truncate 按字节截断，避免超过平台的内容上限
func truncate(s string) string {
	if len(s) <= maxContentBytes {
		return s
	}
	// 按 rune 边界截断，避免切断多字节字符
	cut := s[:maxContentBytes]
	for len(cut) > 0 && !utf8ValidRune(cut[len(cut)-1]) {
		cut = cut[:len(cut)-1]
	}
	return cut + "\n\n…（内容过长已截断）"
}

// utf8ValidRune 判断该字节是否不是 UTF-8 续字节（简化判断）
func utf8ValidRune(b byte) bool {
	return b&0xC0 != 0x80
}
