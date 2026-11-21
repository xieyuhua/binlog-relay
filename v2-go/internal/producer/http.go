package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
)

// HTTP 把整批事件 POST 到业务接口。
//
// 请求体：{"events": [Envelope, ...]}
// 接收方按 2xx 视为成功；4xx 立即失败（重试无意义），5xx 由上层重试。
type HTTP struct {
	url     string
	headers map[string]string
	client  *http.Client
}

func NewHTTP(cfg *config.Config) (*HTTP, error) {
	timeout := cfg.Producer.HTTP.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTP{
		url:     cfg.Producer.HTTP.URL,
		headers: cfg.Producer.HTTP.Headers,
		client:  &http.Client{Timeout: timeout},
	}, nil
}

func (h *HTTP) Name() string { return "http:" + h.url }

func (h *HTTP) Send(ctx context.Context, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	body, err := json.Marshal(map[string]any{"events": envs})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP 推送失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		// 4xx 重试也不会成功，直接失败让告警触发
		return fmt.Errorf("HTTP 推送被拒绝 (%d): %s", resp.StatusCode, snippet)
	}
	return fmt.Errorf("HTTP 推送失败 (%d): %s", resp.StatusCode, snippet)
}

func (h *HTTP) Close() error { return nil }
