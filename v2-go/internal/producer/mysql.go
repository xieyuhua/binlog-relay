package producer

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
	_ "github.com/go-sql-driver/mysql"
)

// columnKeySep 是列集合分组键的分隔符。
// 用不可见字符避免与列名冲突（列名不可能包含控制字符）。
const columnKeySep = "\x00"

// MySQL 把变更写入镜像表。
//
// 表名规则：prefix + 源表名（如 mirror_orders）
// 幂等：ON DUPLICATE KEY UPDATE，重放安全
//
// 关键取舍：
// - 一批记录按表分组，每张表一条多值 INSERT，避免逐行往返
// - 单主键删除用 IN (...)，复合主键逐条（仍在同一事务内）
// - 无主键表用整行匹配 + LIMIT 1，避免误删同内容的多行
type MySQL struct {
	db     *sql.DB
	prefix string
	batch  int

	mu       sync.RWMutex
	keyCache map[string][]string
}

func NewMySQL(cfg *config.Config) (*MySQL, error) {
	db, err := sql.Open("mysql", cfg.Producer.MySQL.DSN)
	if err != nil {
		return nil, fmt.Errorf("MySQL 连接失败: %w", err)
	}
	// 批量写入场景连接数不必多，但要能复用
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("MySQL ping 失败: %w", err)
	}

	batch := cfg.Producer.MySQL.BatchSize
	if batch <= 0 {
		batch = 500
	}

	return &MySQL{
		db:       db,
		prefix:   cfg.Producer.MySQL.TablePrefix,
		batch:    batch,
		keyCache: make(map[string][]string),
	}, nil
}

func (m *MySQL) Name() string { return "mysql:" + m.prefix + "*" }

func (m *MySQL) Send(ctx context.Context, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	// 分块提交，避免单个事务过大
	for start := 0; start < len(envs); start += m.batch {
		end := start + m.batch
		if end > len(envs) {
			end = len(envs)
		}
		if err := m.writeChunk(ctx, envs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (m *MySQL) writeChunk(ctx context.Context, envs []*event.Envelope) error {
	type group struct {
		upserts []map[string]any
		deletes []map[string]any
	}
	byTable := make(map[string]*group)

	for _, env := range envs {
		if env.Kind == event.KindDDL {
			// DDL 由消费者按需执行，这里跳过，避免误改镜像表结构
			continue
		}

		// 扁平信封：直接用顶层字段，省掉一次反序列化
		if env.Table == "" || env.Row == nil {
			continue
		}

		g, ok := byTable[env.Table]
		if !ok {
			g = &group{}
			byTable[env.Table] = g
		}
		if env.Op == string(event.OpDelete) {
			g.deletes = append(g.deletes, env.Row)
		} else {
			g.upserts = append(g.upserts, env.Row)
		}
	}

	tables := make([]string, 0, len(byTable))
	for t := range byTable {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, table := range tables {
		g := byTable[table]
		if len(g.upserts) > 0 {
			if err := m.upsert(ctx, tx, table, g.upserts); err != nil {
				return err
			}
		}
		if len(g.deletes) > 0 {
			if err := m.delete(ctx, tx, table, g.deletes); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (m *MySQL) targetTable(table string) string { return m.prefix + table }

func (m *MySQL) upsert(ctx context.Context, tx *sql.Tx, table string, rows []map[string]any) error {
	// 取所有行的列并集。
	//
	// 不能只用 rows[0] 的列：列裁剪或 schema 变更后，同一批里不同行的
	// 列集合可能不同。若只按第一行的列拼 SQL，后面缺列的行会被写成 NULL，
	// 覆盖掉目标表里的真实值。
	colSet := make(map[string]struct{})
	for _, row := range rows {
		for c := range row {
			colSet[c] = struct{}{}
		}
	}
	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(cols) // 列顺序固定，便于复用预编译语句
	if len(cols) == 0 {
		return nil
	}

	quoted := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
		placeholders[i] = "?"
	}

	updates := make([]string, 0, len(cols))
	keys := m.identityKeys(ctx, table)
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	for _, c := range cols {
		if _, isKey := keySet[c]; isKey {
			continue
		}
		q := "`" + strings.ReplaceAll(c, "`", "``") + "`"
		updates = append(updates, q+"=VALUES("+q+")")
	}

	// 按「列集合」分组：列集合相同的行才能共用一条多值 INSERT。
	//
	// 缺列的行不能简单补 NULL——那会把目标表里的值覆盖掉。
	// 正确做法是只用该行实际拥有的列拼 SQL，未提及的列保持原值。
	type colGroup struct {
		cols []string
		rows []map[string]any
	}
	groups := make(map[string]*colGroup)

	for _, row := range rows {
		rowCols := make([]string, 0, len(row))
		for c := range row {
			rowCols = append(rowCols, c)
		}
		sort.Strings(rowCols)
		key := strings.Join(rowCols, columnKeySep)

		g, ok := groups[key]
		if !ok {
			g = &colGroup{cols: rowCols}
			groups[key] = g
		}
		g.rows = append(g.rows, row)
	}

	// 列集合的 key 排序，保证执行顺序稳定
	groupKeys := make([]string, 0, len(groups))
	for k := range groups {
		groupKeys = append(groupKeys, k)
	}
	sort.Strings(groupKeys)

	for _, key := range groupKeys {
		g := groups[key]
		if err := m.insertGroup(ctx, tx, table, g.cols, g.rows, keySet); err != nil {
			return err
		}
	}
	return nil
}

// insertGroup 对列集合相同的一组行执行多值 INSERT
func (m *MySQL) insertGroup(
	ctx context.Context, tx *sql.Tx, table string,
	cols []string, rows []map[string]any, keySet map[string]struct{},
) error {
	if len(cols) == 0 || len(rows) == 0 {
		return nil
	}

	quoted := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
		placeholders[i] = "?"
	}

	updates := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isKey := keySet[c]; isKey {
			continue
		}
		q := "`" + strings.ReplaceAll(c, "`", "``") + "`"
		updates = append(updates, q+"=VALUES("+q+")")
	}

	valueTuple := "(" + strings.Join(placeholders, ",") + ")"
	tuples := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*len(cols))
	for _, row := range rows {
		tuples = append(tuples, valueTuple)
		for _, c := range cols {
			args = append(args, normalizeValue(row[c]))
		}
	}

	sqlText := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s",
		strings.ReplaceAll(m.targetTable(table), "`", "``"),
		strings.Join(quoted, ","),
		strings.Join(tuples, ","),
	)
	if len(updates) > 0 {
		sqlText += " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
	}

	_, err := tx.ExecContext(ctx, sqlText, args...)
	return err
}

func (m *MySQL) delete(ctx context.Context, tx *sql.Tx, table string, rows []map[string]any) error {
	keys := m.identityKeys(ctx, table)
	target := "`" + strings.ReplaceAll(m.targetTable(table), "`", "``") + "`"

	// 无主键表：整行匹配 + LIMIT 1，避免误删内容相同的多行
	if len(keys) == 0 {
		cols := make([]string, 0, len(rows[0]))
		for c := range rows[0] {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		conds := make([]string, len(cols))
		for i, c := range cols {
			// <=> 是 NULL 安全比较
			conds[i] = "`" + strings.ReplaceAll(c, "`", "``") + "` <=> ?"
		}
		sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1", target, strings.Join(conds, " AND "))
		for _, row := range rows {
			args := make([]any, len(cols))
			for i, c := range cols {
				args[i] = normalizeValue(row[c])
			}
			if _, err := tx.ExecContext(ctx, sqlText, args...); err != nil {
				return err
			}
		}
		return nil
	}

	// 单主键：批量 IN
	if len(keys) == 1 {
		k := keys[0]
		marks := make([]string, len(rows))
		args := make([]any, len(rows))
		for i, row := range rows {
			marks[i] = "?"
			args[i] = normalizeValue(row[k])
		}
		sqlText := fmt.Sprintf(
			"DELETE FROM %s WHERE `%s` IN (%s)",
			target,
			strings.ReplaceAll(k, "`", "``"),
			strings.Join(marks, ","),
		)
		_, err := tx.ExecContext(ctx, sqlText, args...)
		return err
	}

	// 复合主键：逐条，仍在一个事务内
	conds := make([]string, len(keys))
	for i, k := range keys {
		conds[i] = "`" + strings.ReplaceAll(k, "`", "``") + "`=?"
	}
	sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s", target, strings.Join(conds, " AND "))
	for _, row := range rows {
		args := make([]any, len(keys))
		for i, k := range keys {
			args[i] = normalizeValue(row[k])
		}
		if _, err := tx.ExecContext(ctx, sqlText, args...); err != nil {
			return err
		}
	}
	return nil
}

// identityKeys 取目标表的定位键：主键优先，其次唯一键
func (m *MySQL) identityKeys(ctx context.Context, table string) []string {
	target := m.targetTable(table)

	m.mu.RLock()
	if keys, ok := m.keyCache[target]; ok {
		m.mu.RUnlock()
		return keys
	}
	m.mu.RUnlock()

	rows, err := m.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_KEY IN ('PRI','UNI')
		ORDER BY ORDINAL_POSITION`, target)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var pri, uni []string
	for rows.Next() {
		var name, keyType string
		if err := rows.Scan(&name, &keyType); err != nil {
			continue
		}
		if keyType == "PRI" {
			pri = append(pri, name)
		} else {
			uni = append(uni, name)
		}
	}

	keys := pri
	if len(keys) == 0 {
		keys = uni
	}

	m.mu.Lock()
	m.keyCache[target] = keys
	m.mu.Unlock()
	return keys
}

func (m *MySQL) Close() error { return m.db.Close() }

// normalizeValue 把队列里的特殊编码还原成可入库的值
func normalizeValue(v any) any {
	if m, ok := v.(map[string]any); ok {
		// 二进制列在事件里被编码成 {"__binary__": base64}
		if b64, ok := m["__binary__"].(string); ok {
			return b64
		}
		// 其它 map 转 JSON 字符串
		if b, err := jsonMarshal(m); err == nil {
			return string(b)
		}
	}
	return v
}
