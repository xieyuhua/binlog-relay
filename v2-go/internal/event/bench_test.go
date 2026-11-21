package event

import (
	"encoding/json"
	"testing"
)

func benchChange() *Change {
	return &Change{
		Op: "update", Database: "shop", Table: "orders",
		Row: map[string]any{
			"id": int64(7), "status": "paid", "amount": 99.5,
			"name": "张三", "created_at": "2026-11-21 10:00:00",
			"note": "some text", "flag": true,
		},
		Before:     map[string]any{"id": int64(7), "status": "new"},
		TableKey:   []string{"id"},
		BinlogFile: "mysql-bin.000012", BinlogPos: 4321,
		Timestamp: 1795013415, ServerID: 1,
	}
}

// 当前实现的幂等键生成
func BenchmarkChangeKey(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.Key()
	}
}

// 无主键表：走 SHA1 分支
func BenchmarkChangeKeyNoKey(b *testing.B) {
	c := benchChange()
	c.TableKey = nil
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.Key()
	}
}

// 单条事件的完整序列化
func BenchmarkChangeMarshal(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = c.Marshal()
	}
}

// 信封包装（幂等键 + 序列化）
func BenchmarkNewEnvelope(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = NewEnvelope(c)
	}
}

// 对比：json.Marshal 直接调用的下限
func BenchmarkRawMarshal(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(c)
	}
}
