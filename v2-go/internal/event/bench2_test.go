package event

import (
	"encoding/json"
	"testing"
)

// 完整投递路径：构造信封 + 最终序列化（producer 实际做的事）
func BenchmarkFullDeliveryPath(b *testing.B) {
	c := benchChange()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env, err := NewEnvelope(c)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := json.Marshal(env); err != nil {
			b.Fatal(err)
		}
	}
}

// 批量 200 条（默认 batch_size）的完整路径
func BenchmarkBatch200(b *testing.B) {
	changes := make([]*Change, 200)
	for i := range changes {
		c := benchChange()
		c.TableKey = []string{"id"}
		changes[i] = c
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf []byte
		for _, c := range changes {
			env, _ := NewEnvelope(c)
			out, _ := json.Marshal(env)
			buf = append(buf, out...)
		}
		_ = buf
	}
}
