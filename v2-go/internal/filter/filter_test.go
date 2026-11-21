package filter

import (
	"testing"

	"github.com/biaoge/binlog2redis/internal/config"
)

func TestAllowOnlyDatabases(t *testing.T) {
	f := New(config.FilterConfig{Databases: []string{"shop"}})
	if !f.Allow("shop", "orders") {
		t.Fatal("白名单库应放行")
	}
	if f.Allow("other", "orders") {
		t.Fatal("白名单外应拦截")
	}
}

func TestAllowIgnorePriority(t *testing.T) {
	f := New(config.FilterConfig{
		Tables:       []string{"orders", "logs"},
		IgnoreTables: []string{"logs"},
	})
	if !f.Allow("shop", "orders") {
		t.Fatal("orders 应放行")
	}
	if f.Allow("shop", "logs") {
		t.Fatal("忽略规则优先级最高")
	}
}

func TestOnlyConfiguredUnion(t *testing.T) {
	f := New(config.FilterConfig{
		Tables:         []string{"orders"},
		Columns:        map[string][]string{"users": {"id", "name"}},
		OnlyConfigured: true,
	})
	// 清单是 tables ∪ columns，而不是交集
	if !f.Allow("shop", "orders") {
		t.Fatal("tables 里的表应放行")
	}
	if !f.Allow("shop", "users") {
		t.Fatal("columns 里的表也应放行（并集）")
	}
	if f.Allow("shop", "audit_log") {
		t.Fatal("未配置的表应丢弃")
	}
}

func TestKeepSetIncludesPrimaryKey(t *testing.T) {
	f := New(config.FilterConfig{
		Columns: map[string][]string{"orders": {"id", "status"}},
	})
	keep := f.KeepSet("orders", []string{"id"})
	if _, ok := keep["id"]; !ok {
		t.Fatal("主键必须在保留集合里")
	}
	if _, ok := keep["status"]; !ok {
		t.Fatal("配置的列应保留")
	}
	if len(keep) != 2 {
		t.Fatalf("应只有 2 列，得到 %d", len(keep))
	}
}

func TestKeepSetNotPolluteShared(t *testing.T) {
	f := New(config.FilterConfig{
		Columns: map[string][]string{"orders": {"status"}},
	})
	// 第一次带主键 id 调用
	_ = f.KeepSet("orders", []string{"id"})
	// 第二次带不同的主键，不应看到上一次的 id
	keep := f.KeepSet("orders", []string{"uid"})
	if _, ok := keep["id"]; ok {
		t.Fatal("不应污染共享配置（仍含上次的 id）")
	}
	if _, ok := keep["uid"]; !ok {
		t.Fatal("本次主键应保留")
	}
}

func TestKeepSetWildcard(t *testing.T) {
	f := New(config.FilterConfig{
		Columns: map[string][]string{"*": {"id"}},
	})
	keep := f.KeepSet("anything", []string{"id"})
	if keep == nil {
		t.Fatal("通配规则应生效")
	}
	if _, ok := keep["id"]; !ok {
		t.Fatal("应保留 id")
	}
}

func TestKeepSetNilWhenUnconfigured(t *testing.T) {
	f := New(config.FilterConfig{})
	if keep := f.KeepSet("orders", []string{"id"}); keep != nil {
		t.Fatal("未配置裁剪时应返回 nil（零开销路径）")
	}
}

func TestAllowCache(t *testing.T) {
	f := New(config.FilterConfig{Tables: []string{"orders"}})
	// 多次调用结果一致（缓存路径）
	for i := 0; i < 100; i++ {
		if !f.Allow("shop", "orders") {
			t.Fatal("缓存后结果不一致")
		}
	}
}
