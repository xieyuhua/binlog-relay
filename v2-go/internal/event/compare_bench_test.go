package event

import (
	"encoding/json"
	"testing"
)

// 优化前的等价路径：payload 先序列化，再包进嵌套信封序列化
func BenchmarkPathOld(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload, _ := json.Marshal(c)
		env := struct {
			ID      string          `json:"id"`
			Kind    string          `json:"kind"`
			Op      string          `json:"op"`
			DB      string          `json:"db"`
			Table   string          `json:"table"`
			Pos     string          `json:"pos"`
			TS      int64           `json:"ts"`
			Payload json.RawMessage `json:"payload"`
		}{
			c.Key(), "row", string(c.Op), c.Database, c.Table,
			c.Pos(), c.Timestamp, payload,
		}
		_, _ = json.Marshal(env)
	}
}

// 优化后的路径：构造扁平信封 + 一次序列化
func BenchmarkPathNew(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		env, _ := NewEnvelope(c)
		_, _ = json.Marshal(env)
	}
}
