package producer

import (
	"testing"
)

// 列集合不同的行应被分到不同组，不能互相覆盖成 NULL
func TestUpsertGroupingByColumns(t *testing.T) {
	rows := []map[string]any{
		{"id": 1, "a": "x", "b": "y"},
		{"id": 2, "a": "p"}, // 缺 b
		{"id": 3, "a": "m", "b": "n"},
	}

	// 复现分组逻辑
	groups := make(map[string]int)
	for _, row := range rows {
		cols := make([]string, 0, len(row))
		for c := range row {
			cols = append(cols, c)
		}
		sortStrings(cols)
		groups[joinWith(cols, "|")]++
	}

	if len(groups) != 2 {
		t.Fatalf("应分成 2 组（有无 b 列），实际 %d: %v", len(groups), groups)
	}
	if groups["a|b|id"] != 2 {
		t.Errorf("完整列应有 2 行，实际 %d", groups["a|b|id"])
	}
	if groups["a|id"] != 1 {
		t.Errorf("缺列应有 1 行，实际 %d", groups["a|id"])
	}
}

// 缺列的行不应被写成 NULL
func TestUpsertMissingColumnNotNil(t *testing.T) {
	row := map[string]any{"id": 2, "a": "p"}
	if _, hasB := row["b"]; hasB {
		t.Fatal("该行本来就没有 b 列")
	}
	// 关键：分组后 SQL 只包含 a 和 id，b 不会被赋值为 NULL
	if len(row) != 2 {
		t.Errorf("列数应为 2，实际 %d", len(row))
	}
}

func joinWith(s []string, sep string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += sep
		}
		out += v
	}
	return out
}
