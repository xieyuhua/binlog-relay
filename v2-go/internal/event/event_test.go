package event

import (
	"testing"
)

func TestChangeKeyWithPrimaryKey(t *testing.T) {
	c := &Change{
		Op: "update", Database: "shop", Table: "orders",
		Row:      map[string]any{"id": 7, "status": "paid"},
		Before:   map[string]any{"id": 7, "status": "new"},
		TableKey: []string{"id"},
	}
	if got, want := c.Key(), "shop.orders:7"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestChangeKeyComposite(t *testing.T) {
	c := &Change{
		Op: "insert", Database: "shop", Table: "items",
		Row:      map[string]any{"a": 1, "b": 2},
		TableKey: []string{"a", "b"},
	}
	if got, want := c.Key(), "shop.items:1,2"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestChangeKeyNoKeyUsesHash(t *testing.T) {
	c1 := &Change{Op: "insert", Database: "d", Table: "t", Row: map[string]any{"x": 1, "y": 2}}
	c2 := &Change{Op: "insert", Database: "d", Table: "t", Row: map[string]any{"x": 3, "y": 4}}

	if c1.Key() == c2.Key() {
		t.Fatal("不同行应生成不同的键")
	}
	if c1.Key()[:len("d.t:#")] != "d.t:#" {
		t.Fatalf("无键表应以 # 前缀: %s", c1.Key())
	}
}

func TestChangeKeyDeleteUsesBefore(t *testing.T) {
	ins := &Change{Op: "insert", Database: "d", Table: "t", Row: map[string]any{"x": 1, "y": 2}}
	del := &Change{
		Op: "delete", Database: "d", Table: "t",
		Row:    map[string]any{"x": 1, "y": 2},
		Before: map[string]any{"x": 1, "y": 2},
	}
	if ins.Key() != del.Key() {
		t.Fatalf("insert 与 delete 的键应一致: %s vs %s", ins.Key(), del.Key())
	}
}

func TestNewEnvelope(t *testing.T) {
	c := &Change{
		Op: "insert", Database: "shop", Table: "orders",
		Row:        map[string]any{"id": 1},
		TableKey:   []string{"id"},
		BinlogFile: "mysql-bin.000001", BinlogPos: 100,
		Timestamp: 1795013415, ServerID: 1,
	}
	env, err := NewEnvelope(c)
	if err != nil {
		t.Fatal(err)
	}

	if env.Kind != "row" || env.Op != "insert" {
		t.Fatalf("kind/op 错误: %s/%s", env.Kind, env.Op)
	}
	if env.ID != "shop.orders:1" {
		t.Fatalf("id 错误: %s", env.ID)
	}
	if env.Pos != "mysql-bin.000001:100" {
		t.Fatalf("pos 错误: %s", env.Pos)
	}
	if _, err := env.Marshal(); err != nil {
		t.Fatal("payload 不是合法 JSON")
	}
}

func TestDDLClassify(t *testing.T) {
	// classifyDDL 在 replication 包内，这里只验证 DDL 事件本身
	d := &DDL{
		Op: "alter_table", Database: "demo", Tables: []string{"orders"},
		Query:      "ALTER TABLE orders ADD COLUMN x INT",
		BinlogFile: "bin.1", BinlogPos: 200,
	}
	env, err := NewEnvelope(d)
	if err != nil {
		t.Fatal(err)
	}
	if env.Kind != "ddl" {
		t.Fatalf("kind=%s want ddl", env.Kind)
	}
	if env.Pos != "bin.1:200" {
		t.Fatalf("pos=%s", env.Pos)
	}
}

func TestDDLKeyStable(t *testing.T) {
	d1 := &DDL{Query: "CREATE TABLE t (id INT)", BinlogFile: "b", BinlogPos: 1}
	d2 := &DDL{Query: "CREATE TABLE t (id INT)", BinlogFile: "b", BinlogPos: 1}
	d3 := &DDL{Query: "CREATE TABLE t (id BIGINT)", BinlogFile: "b", BinlogPos: 1}

	if d1.Key() != d2.Key() {
		t.Fatal("相同 DDL 的键应稳定")
	}
	if d1.Key() == d3.Key() {
		t.Fatal("不同 DDL 的键应不同")
	}
}
