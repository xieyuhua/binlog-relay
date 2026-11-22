// Package replication 负责连接 MySQL、读取 binlog 并归一化成 event.Record。
package replication

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/biaoge/binlog2redis/internal/event"
	"github.com/biaoge/binlog2redis/internal/filter"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Parser 把 go-mysql 的原始事件转成框架的 Change / DDL。
//
// 设计要点：
// - 主键列按 tableID 缓存：DDL 后 tableID 会变，缓存自然失效
// - 列裁剪在构造 Change 之前完成，避免多余的对象分配
// - 所有 map 值都做 JSON 安全化（二进制转 base64）
type Parser struct {
	filter  *filter.Filter
	emitDDL bool

	mu sync.RWMutex
	// tableID => 主键/唯一键列名
	keyCache map[uint64][]string
	// 当前事件的 GTID（由 GTIDEvent 更新，RowsEvent 复用）
	currentGTID string
	// "库.表" => 主键/唯一键列名（由 SetTableKeys 注入）
	tableKeyDefs map[string][]string
}

func NewParser(f *filter.Filter, emitDDL bool) *Parser {
	return &Parser{
		filter:   f,
		emitDDL:  emitDDL,
		keyCache: make(map[uint64][]string),
	}
}

// SetGTID 由上层在收到 GTIDEvent 时调用
func (p *Parser) SetGTID(gtid string) { p.currentGTID = gtid }

// CurrentGTID 返回当前事务的 GTID
func (p *Parser) CurrentGTID() string { return p.currentGTID }

// ResetTableCache 在 DDL 后清缓存，避免用过期的主键定义
func (p *Parser) ResetTableCache() {
	p.mu.Lock()
	p.keyCache = make(map[uint64][]string)
	p.mu.Unlock()
}

// ParseRows 把一个 RowsEvent 展开成多条 Change。
//
// go-mysql 的 RowsEvent 不带事件类型（在 BinlogEvent.Header 上），
// 由调用方通过 eventType 传入。
func (p *Parser) ParseRows(e *replication.RowsEvent, eventType replication.EventType,
	file string, pos int64, ts int64, serverID uint32) []*event.Change {
	if e == nil || e.Table == nil {
		return nil
	}

	table := string(e.Table.Table)
	database := string(e.Table.Schema)

	if !p.filter.Allow(database, table) {
		return nil
	}

	keys := p.tableKeys(e.Table)

	// 按事件类型分派：
	//   Write  -> 每两行是一组新增行（实际就是一行一个）
	//   Update -> 每两行是 before/after
	//   Delete -> 一行一个
	var op event.Op
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		op = event.OpInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		op = event.OpUpdate
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		op = event.OpDelete
	default:
		return nil
	}

	keep := p.filter.KeepSet(table, keys)
	changes := make([]*event.Change, 0, len(e.Rows))

	if op == event.OpUpdate {
		// Update 的 Rows 是 [before, after, before, after, ...]
		for i := 0; i+1 < len(e.Rows); i += 2 {
			before := p.rowToMap(e.Table, e.Rows[i], keep)
			after := p.rowToMap(e.Table, e.Rows[i+1], keep)
			changes = append(changes, &event.Change{
				Op:         op,
				Database:   database,
				Table:      table,
				Row:        after,
				Before:     before,
				TableKey:   keys,
				BinlogFile: file,
				BinlogPos:  pos,
				GTID:       p.currentGTID,
				Timestamp:  ts,
				ServerID:   serverID,
			})
		}
		return changes
	}

	for _, raw := range e.Rows {
		changes = append(changes, &event.Change{
			Op:         op,
			Database:   database,
			Table:      table,
			Row:        p.rowToMap(e.Table, raw, keep),
			TableKey:   keys,
			BinlogFile: file,
			BinlogPos:  pos,
			GTID:       p.currentGTID,
			Timestamp:  ts,
			ServerID:   serverID,
		})
	}
	return changes
}

// ParseDDL 从 QueryEvent 中识别 DDL；非 DDL 返回 nil
func (p *Parser) ParseDDL(q *replication.QueryEvent, file string, pos int64, ts int64, serverID uint32) *event.DDL {
	if !p.emitDDL {
		return nil
	}
	query := string(q.Query)
	op := classifyDDL(query)
	if op == "" {
		return nil
	}
	return &event.DDL{
		Op:         op,
		Database:   string(q.Schema),
		Tables:     extractTables(query),
		Query:      query,
		BinlogFile: file,
		BinlogPos:  pos,
		Timestamp:  ts,
		ServerID:   serverID,
	}
}

// tableKeys 返回主键列；无主键时回退唯一键。
//
// go-mysql 的 TableMapEvent 只提供列名，不含主键信息，
// 所以主键通过 SetTableKeys 由外部（information_schema 查询）注入。
// 未注入时返回空切片，下游会退化为按整行内容生成幂等键。
func (p *Parser) tableKeys(t *replication.TableMapEvent) []string {
	p.mu.RLock()
	if keys, ok := p.keyCache[t.TableID]; ok {
		p.mu.RUnlock()
		return keys
	}
	p.mu.RUnlock()

	// 按 库.表 查外部注入的主键定义
	qualified := string(t.Schema) + "." + string(t.Table)
	p.mu.RLock()
	keys := p.tableKeyDefs[qualified]
	p.mu.RUnlock()

	p.mu.Lock()
	// 表数量有限，超限整体清空即可（DDL 后本来就会失效）
	if len(p.keyCache) > 2048 {
		p.keyCache = make(map[uint64][]string)
	}
	p.keyCache[t.TableID] = keys
	p.mu.Unlock()

	return keys
}

// SetTableKeys 注入某张表的主键/唯一键列（按优先级排好序）
//
// 由 schema 加载器在启动时与 DDL 后调用。
func (p *Parser) SetTableKeys(database, table string, keys []string) {
	p.mu.Lock()
	if p.tableKeyDefs == nil {
		p.tableKeyDefs = make(map[string][]string)
	}
	p.tableKeyDefs[database+"."+table] = keys
	p.mu.Unlock()

	// 已有的 tableID 缓存需要失效，下一次访问会重新查
	p.ResetTableCache()
}

// rowToMap 把一行值按列名组装成 map，并按 keep 裁剪
func (p *Parser) rowToMap(t *replication.TableMapEvent, raw []any, keep map[string]struct{}) map[string]any {
	names := t.ColumnNameString()
	n := len(raw)
	if len(names) < n {
		n = len(names)
	}

	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		col := names[i]
		if keep != nil {
			if _, ok := keep[col]; !ok {
				continue
			}
		}
		out[col] = normalizeValue(raw[i])
	}
	return out
}

// normalizeValue 保证值可 JSON 序列化
func normalizeValue(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		// 二进制列（BLOB/BINARY）或非 UTF-8 文本转 base64
		if !isUTF8(val) {
			return map[string]any{"__binary__": encodeBase64(val)}
		}
		return string(val)
	case string, bool, int64, uint64, int32, uint32, int, float64, float32:
		return val
	default:
		return fmt.Sprint(val)
	}
}

// classifyDDL 识别 DDL 类型，返回空串表示不是 DDL
func classifyDDL(query string) string {
	sql := strings.TrimSpace(query)
	if sql == "" {
		return ""
	}
	// 去掉前导注释
	for strings.HasPrefix(sql, "/*") {
		if end := strings.Index(sql, "*/"); end > 0 {
			sql = strings.TrimSpace(sql[end+2:])
		} else {
			break
		}
	}
	upper := strings.ToUpper(sql)

	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return "create_table"
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return "alter_table"
	case strings.HasPrefix(upper, "DROP TABLE"):
		return "drop_table"
	case strings.HasPrefix(upper, "RENAME TABLE"):
		return "rename_table"
	case strings.HasPrefix(upper, "TRUNCATE"):
		return "truncate_table"
	case strings.HasPrefix(upper, "CREATE INDEX"), strings.HasPrefix(upper, "CREATE UNIQUE INDEX"):
		return "create_index"
	case strings.HasPrefix(upper, "DROP INDEX"):
		return "drop_index"
	case strings.HasPrefix(upper, "CREATE DATABASE"), strings.HasPrefix(upper, "CREATE SCHEMA"):
		return "create_database"
	case strings.HasPrefix(upper, "DROP DATABASE"), strings.HasPrefix(upper, "DROP SCHEMA"):
		return "drop_database"
	// 视图单独识别，便于下游重建视图或刷新元数据。
	// 注意判断顺序：CREATE OR REPLACE VIEW 要先于 CREATE PROCEDURE 等分支。
	case strings.HasPrefix(upper, "CREATE OR REPLACE VIEW"), strings.HasPrefix(upper, "CREATE VIEW"):
		return "create_view"
	case strings.HasPrefix(upper, "ALTER VIEW"):
		return "alter_view"
	case strings.HasPrefix(upper, "DROP VIEW"):
		return "drop_view"
	// 存储过程/函数/触发器仍归到 other
	case strings.HasPrefix(upper, "CREATE PROCEDURE"), strings.HasPrefix(upper, "DROP PROCEDURE"),
		strings.HasPrefix(upper, "CREATE FUNCTION"), strings.HasPrefix(upper, "DROP FUNCTION"),
		strings.HasPrefix(upper, "CREATE TRIGGER"), strings.HasPrefix(upper, "DROP TRIGGER"):
		return "other"
	}
	return ""
}

// extractTables 从 DDL 中提取表名（保守解析，取不到返回空）
func extractTables(query string) []string {
	seen := make(map[string]struct{})
	add := func(name string) {
		name = strings.Trim(name, "` ")
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = strings.Trim(name[i+1:], "` ")
		}
		if name != "" {
			seen[name] = struct{}{}
		}
	}

	upper := strings.ToUpper(query)
	keywords := []string{
		"CREATE TABLE", "DROP TABLE", "ALTER TABLE", "TRUNCATE TABLE",
		// 视图也提取名字，便于下游重建
		"CREATE OR REPLACE VIEW", "CREATE VIEW", "ALTER VIEW", "DROP VIEW",
	}
	for _, kw := range keywords {
		idx := strings.Index(upper, kw)
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(query[idx+len(kw):])
		rest = strings.TrimPrefix(rest, "IF NOT EXISTS")
		rest = strings.TrimSpace(strings.TrimPrefix(rest, "IF EXISTS"))
		rest = strings.TrimSpace(rest)
		if rest == "" {
			continue
		}
		if i := strings.IndexAny(rest, " \t\n("); i > 0 {
			rest = rest[:i]
		}
		add(rest)
	}

	// RENAME TABLE a TO b, c TO d
	// 逐段按 " TO " 切分（大小写不敏感），避免手工计算下标出错
	if idx := strings.Index(upper, "RENAME TABLE"); idx >= 0 {
		rest := strings.TrimSpace(query[idx+len("RENAME TABLE"):])
		rest = strings.TrimSuffix(rest, ";")
		for _, pair := range strings.Split(rest, ",") {
			parts := splitCaseInsensitive(pair, " TO ")
			if len(parts) == 2 {
				add(parts[0])
				add(parts[1])
			}
		}
	}

	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	// 稳定输出顺序，便于测试与日志比对
	sortStrings(out)
	return out
}

// splitCaseInsensitive 按分隔符切分（忽略大小写），只切一次
func splitCaseInsensitive(s, sep string) []string {
	upper := strings.ToUpper(s)
	idx := strings.Index(upper, strings.ToUpper(sep))
	if idx < 0 {
		return []string{strings.TrimSpace(s)}
	}
	return []string{
		strings.TrimSpace(s[:idx]),
		strings.TrimSpace(s[idx+len(sep):]),
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// 便于测试替换
var b64 = base64.StdEncoding
var encodeBase64 = func(b []byte) string { return b64.EncodeToString(b) }

// isUTF8 判断字节序列是否为合法 UTF-8
func isUTF8(b []byte) bool {
	return utf8.Valid(b)
}
