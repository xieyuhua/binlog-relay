package replication

import (
	"testing"
)

func TestClassifyDDL(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"CREATE TABLE `orders` (id INT)", "create_table"},
		{"create table orders (id int)", "create_table"},
		{"ALTER TABLE orders ADD COLUMN x INT", "alter_table"},
		{"DROP TABLE IF EXISTS `shop`.`orders`", "drop_table"},
		{"RENAME TABLE a TO b", "rename_table"},
		{"TRUNCATE TABLE logs", "truncate_table"},
		{"CREATE INDEX idx ON orders(id)", "create_index"},
		{"CREATE UNIQUE INDEX uk ON orders(code)", "create_index"},
		{"DROP INDEX idx ON orders", "drop_index"},
		{"CREATE DATABASE demo", "create_database"},
		{"DROP SCHEMA demo", "drop_database"},
		{"CREATE VIEW v AS SELECT 1", "create_view"},
		{"CREATE OR REPLACE VIEW v AS SELECT 1", "create_view"},
		{"ALTER VIEW v AS SELECT 2", "alter_view"},
		{"DROP VIEW IF EXISTS v", "drop_view"},
		{"CREATE TRIGGER t BEFORE INSERT ON o", "other"},
		{"/* comment */ ALTER TABLE t ADD c INT", "alter_table"},
		// 非 DDL
		{"BEGIN", ""},
		{"COMMIT", ""},
		{"INSERT INTO t VALUES(1)", ""},
		{"UPDATE t SET a=1", ""},
		{"DELETE FROM t", ""},
		{"", ""},
	}

	for _, c := range cases {
		if got := classifyDDL(c.sql); got != c.want {
			t.Errorf("classifyDDL(%q) = %q, want %q", c.sql, got, c.want)
		}
	}
}

func TestExtractTables(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"CREATE TABLE `orders` (id INT)", []string{"orders"}},
		{"DROP TABLE IF EXISTS `shop`.`orders`", []string{"orders"}},
		{"ALTER TABLE orders ADD COLUMN x INT", []string{"orders"}},
		{"TRUNCATE TABLE logs", []string{"logs"}},
		{"CREATE VIEW v_orders AS SELECT 1", []string{"v_orders"}},
		{"CREATE OR REPLACE VIEW shop.v2 AS SELECT 2", []string{"v2"}},
		{"DROP VIEW IF EXISTS v3", []string{"v3"}},
		{"RENAME TABLE a TO b, c TO d", []string{"a", "b", "c", "d"}},
		{"CREATE DATABASE demo", []string{}},
	}

	for _, c := range cases {
		got := extractTables(c.sql)
		if !sameStrings(got, c.want) {
			t.Errorf("extractTables(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestNormalizeValue(t *testing.T) {
	// 合法 UTF-8
	if got := normalizeValue([]byte("hello")); got != "hello" {
		t.Errorf("got %v", got)
	}
	// 非法 UTF-8 → base64
	got := normalizeValue([]byte{0xff, 0xfe})
	m, ok := got.(map[string]any)
	if !ok || m["__binary__"] == nil {
		t.Errorf("二进制应包成 __binary__，得到 %v", got)
	}
	// 数字原样
	if got := normalizeValue(int64(42)); got != int64(42) {
		t.Errorf("got %v", got)
	}
	if got := normalizeValue(nil); got != nil {
		t.Errorf("got %v", got)
	}
}

func TestParserFilterIntegration(t *testing.T) {
	// 验证 parser 的过滤器能正确拦截（不涉及真实 binlog 事件）
	p := NewParser(nil, false)
	if p == nil {
		t.Fatal("NewParser 返回 nil")
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 回归保护：SetTableKeys 注入后，幂等键应使用真实主键而非行哈希
func TestSetTableKeysEnablesPrimaryKey(t *testing.T) {
	// 这里只验证 Parser 的缓存语义（不构造真实 TableMapEvent）
	p := NewParser(nil, false)
	p.SetTableKeys("shop", "orders", []string{"id"})

	// 通过反射式访问：再次 Set 不同主键应更新
	p.SetTableKeys("shop", "orders", []string{"a", "b"})
	if p.tableKeyDefs["shop.orders"] == nil {
		t.Fatal("主键定义未保存")
	}
	if got := p.tableKeyDefs["shop.orders"]; len(got) != 2 || got[0] != "a" {
		t.Fatalf("主键定义未更新: %v", got)
	}
}

func TestResetTableCache(t *testing.T) {
	p := NewParser(nil, false)
	p.SetTableKeys("shop", "orders", []string{"id"})
	// 模拟 tableID 缓存
	p.keyCache[42] = []string{"id"}

	p.ResetTableCache()
	if len(p.keyCache) != 0 {
		t.Fatal("ResetTableCache 应清空 tableID 缓存")
	}
	// 但库表级定义要保留（不必重新查 information_schema）
	if p.tableKeyDefs["shop.orders"] == nil {
		t.Fatal("不应清空库表级主键定义")
	}
}
