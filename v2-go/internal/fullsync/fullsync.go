// Package fullsync 实现全量快照同步。
package fullsync

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
	"github.com/biaoge/binlog2redis/internal/producer"
)

// Syncer 把现存数据推一遍。
//
// # 为什么需要
//
// 增量只能捕获「订阅之后」的变更。下游为空或与源库有偏差时，必须先全量再增量。
//
// # 如何保证不丢数据（关键）
//
// 顺序是「先记位点，后快照」：
//
//  1. 读取当前 binlog 位点 P，立刻持久化
//  2. 从 P 开始做快照（逐表游标分页）
//  3. 快照完成后启动 relay，从 P 回放
//
// 这样 P 之后的变更都会被回放（不丢），而快照期间改过的行会被写两次，
// 由下游幂等吸收（ON DUPLICATE KEY UPDATE / ES _id / CK 去重）。
//
// 反过来做（先快照后记位点）会丢掉快照期间的变更。
type Syncer struct {
	db      *sql.DB
	log     *slog.Logger
	prod    producer.Producer
	batch   int
	prefix  string
	// flushSize 攒够多少条投递一次；太小会导致大量小批次
	flushSize int
}

func NewSyncer(db *sql.DB, log *slog.Logger, prod producer.Producer, batch int) *Syncer {
	if batch <= 0 {
		batch = 1000
	}
	// 投递批次独立于读取批次：读取可以大，投递要匹配下游的批量能力
	flushSize := batch / 2
	if flushSize < 100 {
		flushSize = 100
	}
	return &Syncer{db: db, log: log, prod: prod, batch: batch, flushSize: flushSize}
}

// Table 是一张待同步的表
type Table struct {
	Database string
	Table    string
}

// ListTables 列出待同步的表
func (s *Syncer) ListTables(ctx context.Context, cfg *config.Config, only []string) ([]Table, error) {
	ignore := cfg.Filter.IgnoreDatabases
	if len(ignore) == 0 {
		ignore = []string{"mysql", "sys", "information_schema", "performance_schema"}
	}

	var (
		conditions []string
		args       []any
	)
	conditions = append(conditions, "TABLE_TYPE = ?")
	args = append(args, "BASE TABLE")

	placeholders := make([]string, len(ignore))
	for i, db := range ignore {
		placeholders[i] = "?"
		args = append(args, db)
	}
	conditions = append(conditions, "TABLE_SCHEMA NOT IN ("+strings.Join(placeholders, ",")+")")

	if len(cfg.Filter.Databases) > 0 {
		ph := make([]string, len(cfg.Filter.Databases))
		for i, db := range cfg.Filter.Databases {
			ph[i] = "?"
			args = append(args, db)
		}
		conditions = append(conditions, "TABLE_SCHEMA IN ("+strings.Join(ph, ",")+")")
	}

	tables := only
	if len(tables) == 0 {
		tables = cfg.Filter.Tables
	}
	if len(tables) > 0 {
		ph := make([]string, len(tables))
		for i, t := range tables {
			ph[i] = "?"
			args = append(args, t)
		}
		conditions = append(conditions, "TABLE_NAME IN ("+strings.Join(ph, ",")+")")
	}

	query := `SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE ` +
		strings.Join(conditions, " AND ") + ` ORDER BY TABLE_SCHEMA, TABLE_NAME`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Database, &t.Table); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// SyncTable 同步单表全部数据，返回行数。
//
// 用游标分页而非 OFFSET：大表上 OFFSET 会全表扫描，游标是 O(1) 定位。
func (s *Syncer) SyncTable(ctx context.Context, t Table) (int64, error) {
	keys, err := s.primaryKeys(ctx, t)
	if err != nil {
		return 0, err
	}
	cols, err := s.columns(ctx, t)
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, nil
	}

	quoted := quote(t.Database) + "." + quote(t.Table)
	colList := make([]string, len(cols))
	for i, c := range cols {
		colList[i] = quote(c)
	}
	selectList := strings.Join(colList, ",")

	// 无主键：单次全读并告警
	if len(keys) == 0 {
		s.log.Warn("表无主键，单次全读快照（可能不一致）", "table", t.Database+"."+t.Table)
		return s.copyAll(ctx, selectList, quoted, t, keys)
	}

	orderBy := make([]string, len(keys))
	for i, k := range keys {
		orderBy[i] = quote(k)
	}
	order := strings.Join(orderBy, ",")

	var total int64
	var lastKey []any

	for {
		var (
			query string
			args  []any
		)
		if lastKey == nil {
			query = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d",
				selectList, quoted, order, s.batch)
		} else if len(keys) == 1 {
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
				selectList, quoted, quote(keys[0]), order, s.batch)
			args = []any{lastKey[0]}
		} else {
			// 复合主键：(k1>?) OR (k1=? AND k2>?) OR ...
			var conds []string
			for i := range keys {
				var parts []string
				for j := 0; j < i; j++ {
					parts = append(parts, quote(keys[j])+" = ?")
					args = append(args, lastKey[j])
				}
				parts = append(parts, quote(keys[i])+" > ?")
				args = append(args, lastKey[i])
				conds = append(conds, "("+strings.Join(parts, " AND ")+")")
			}
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
				selectList, quoted, strings.Join(conds, " OR "), order, s.batch)
		}

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return total, fmt.Errorf("查询 %s 失败: %w", t.Table, err)
		}

		var records []*event.Envelope
		var lastRow map[string]any
		count := 0

		for rows.Next() {
			row, err := scanRow(rows, cols)
			if err != nil {
				rows.Close()
				return total, err
			}
			count++
			lastRow = row

			ch := &event.Change{
				Op:        event.OpInsert, // 快照统一按 insert，下游幂等写
				Database:  t.Database,
				Table:     t.Table,
				Row:       row,
				TableKey:  keys,
				Timestamp: time.Now().Unix(),
			}
			env, err := event.NewEnvelope(ch)
			if err != nil {
				rows.Close()
				return total, err
			}
			records = append(records, env)

			// 分批投递，避免单次内存占用过大
			if len(records) >= s.flushSize {
				if err := s.prod.Send(ctx, records); err != nil {
					rows.Close()
					return total, err
				}
				total += int64(len(records))
				records = nil
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return total, err
		}

		if len(records) > 0 {
			if err := s.prod.Send(ctx, records); err != nil {
				return total, err
			}
			total += int64(len(records))
		}

		if count < s.batch || lastRow == nil {
			break
		}

		lastKey = make([]any, len(keys))
		for i, k := range keys {
			lastKey[i] = lastRow[k]
		}
	}

	return total, nil
}

// copyAll 无主键表的兜底：一次性读完
func (s *Syncer) copyAll(ctx context.Context, selectList, quoted string, t Table, keys []string) (int64, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s", selectList, quoted))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var total int64
	var batch []*event.Envelope

	for rows.Next() {
		row, err := scanRow(rows, cols)
		if err != nil {
			return total, err
		}
		ch := &event.Change{
			Op:        event.OpInsert,
			Database:  t.Database,
			Table:     t.Table,
			Row:       row,
			TableKey:  keys,
			Timestamp: time.Now().Unix(),
		}
		env, err := event.NewEnvelope(ch)
		if err != nil {
			return total, err
		}
		batch = append(batch, env)
		if len(batch) >= s.flushSize {
			if err := s.prod.Send(ctx, batch); err != nil {
				return total, err
			}
			total += int64(len(batch))
			batch = nil
		}
	}
	if len(batch) > 0 {
		if err := s.prod.Send(ctx, batch); err != nil {
			return total, err
		}
		total += int64(len(batch))
	}
	return total, rows.Err()
}

func (s *Syncer) primaryKeys(ctx context.Context, t Table) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY IN ('PRI','UNI')
		ORDER BY ORDINAL_POSITION`, t.Database, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pri, uni []string
	for rows.Next() {
		var name, keyType string
		if err := rows.Scan(&name, &keyType); err != nil {
			return nil, err
		}
		if keyType == "PRI" {
			pri = append(pri, name)
		} else {
			uni = append(uni, name)
		}
	}
	if len(pri) > 0 {
		return pri, rows.Err()
	}
	return uni, rows.Err()
}

func (s *Syncer) columns(ctx context.Context, t Table) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, t.Database, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// scanRow 把一行扫描成 map，二进制值转 base64
func scanRow(rows *sql.Rows, cols []string) (map[string]any, error) {
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}

	out := make(map[string]any, len(cols))
	for i, c := range cols {
		out[c] = normalizeSQLValue(vals[i])
	}
	return out, nil
}

func normalizeSQLValue(v any) any {
	switch val := v.(type) {
	case []byte:
		if isUTF8(val) {
			return string(val)
		}
		return map[string]any{"__binary__": base64Encode(val)}
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return val
	}
}

func quote(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

func sortTables(ts []Table) {
	sort.Slice(ts, func(i, j int) bool {
		if ts[i].Database != ts[j].Database {
			return ts[i].Database < ts[j].Database
		}
		return ts[i].Table < ts[j].Table
	})
}
