package fullsync

import (
	"context"
	"database/sql"
	"testing"

	"github.com/biaoge/binlog2redis/internal/event"
	_ "modernc.org/sqlite"
)

// fakeProducer 收集投递的记录
type fakeProducer struct {
	rows []map[string]any
}

func (f *fakeProducer) Send(_ context.Context, envs []*event.Envelope) error {
	for _, e := range envs {
		if e.Row != nil {
			f.rows = append(f.rows, e.Row)
		}
	}
	return nil
}
func (f *fakeProducer) Name() string  { return "fake" }
func (f *fakeProducer) Close() error  { return nil }

// cursorPage 是 SyncTable 分页逻辑的等价实现，用于验证不重不漏
func cursorPage(ctx context.Context, db *sql.DB, table string, keys []string, batch int) ([]string, error) {
	order := ""
	qkeys := make([]string, len(keys))
	for i, k := range keys {
		qkeys[i] = "`" + k + "`"
	}
	order = joinComma(qkeys)

	var seen []string
	var lastKey []any

	for {
		var (
			query string
			args  []any
		)
		if lastKey == nil {
			query = "SELECT " + order + " FROM " + table + " ORDER BY " + order + " LIMIT " + itoa(batch)
		} else if len(keys) == 1 {
			query = "SELECT " + order + " FROM " + table + " WHERE `" + keys[0] + "` > ? ORDER BY " + order + " LIMIT " + itoa(batch)
			args = []any{lastKey[0]}
		} else {
			var conds []string
			for i := range keys {
				var parts []string
				for j := 0; j < i; j++ {
					parts = append(parts, qkeys[j]+" = ?")
					args = append(args, lastKey[j])
				}
				parts = append(parts, qkeys[i]+" > ?")
				args = append(args, lastKey[i])
				conds = append(conds, "("+joinAND(parts)+")")
			}
			query = "SELECT " + order + " FROM " + table + " WHERE " + joinOR(conds) + " ORDER BY " + order + " LIMIT " + itoa(batch)
		}

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}

		var lastRow []any
		count := 0
		for rows.Next() {
			vals := make([]any, len(keys))
			ptrs := make([]any, len(keys))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, err
			}
			count++
			lastRow = vals

			key := ""
			for i, v := range vals {
				if i > 0 {
					key += ","
				}
				key += toString(v)
			}
			seen = append(seen, key)
		}
		rows.Close()

		if count < batch || lastRow == nil {
			break
		}
		lastKey = lastRow
	}
	return seen, nil
}

func TestCursorPaginationSingleKey(t *testing.T) {
	db := openSQLite(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 25; i++ {
		if _, err := db.Exec("INSERT INTO t (id, name) VALUES (?, ?)", i, "row"); err != nil {
			t.Fatal(err)
		}
	}

	for _, batch := range []int{1, 7, 25, 100} {
		got, err := cursorPage(context.Background(), db, "t", []string{"id"}, batch)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 25 {
			t.Errorf("batch=%d: 行数 %d want 25", batch, len(got))
		}
		uniq := map[string]bool{}
		for _, k := range got {
			if uniq[k] {
				t.Errorf("batch=%d: 重复行 %s", batch, k)
			}
			uniq[k] = true
		}
	}
}

func TestCursorPaginationCompositeKey(t *testing.T) {
	db := openSQLite(t)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t2 (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a,b))`); err != nil {
		t.Fatal(err)
	}
	var total int
	for a := 1; a <= 4; a++ {
		for b := 1; b <= 6; b++ {
			if _, err := db.Exec("INSERT INTO t2 (a,b,v) VALUES (?,?,?)", a, b, "v"); err != nil {
				t.Fatal(err)
			}
			total++
		}
	}

	for _, batch := range []int{1, 5, 24, 100} {
		got, err := cursorPage(context.Background(), db, "t2", []string{"a", "b"}, batch)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != total {
			t.Errorf("batch=%d: 行数 %d want %d", batch, len(got), total)
		}
		uniq := map[string]bool{}
		for _, k := range got {
			if uniq[k] {
				t.Errorf("batch=%d: 重复行 %s", batch, k)
			}
			uniq[k] = true
		}
	}
}

func TestNormalizeSQLValue(t *testing.T) {
	// 合法 UTF-8 字符串
	if got := normalizeSQLValue([]byte("hello")); got != "hello" {
		t.Errorf("UTF-8: got %v", got)
	}
	// 非法 UTF-8 → base64 包装
	got := normalizeSQLValue([]byte{0xff, 0xfe})
	m, ok := got.(map[string]any)
	if !ok || m["__binary__"] == nil {
		t.Errorf("二进制应包成 __binary__，得到 %v", got)
	}
	// nil
	if got := normalizeSQLValue(nil); got != nil {
		t.Errorf("nil: got %v", got)
	}
}

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func joinComma(s []string) string { return joinWith(s, ",") }
func joinAND(s []string) string   { return joinWith(s, " AND ") }
func joinOR(s []string) string    { return joinWith(s, " OR ") }

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

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func toString(v any) string {
	switch x := v.(type) {
	case int64:
		return itoa(int(x))
	case nil:
		return ""
	default:
		return ""
	}
}
