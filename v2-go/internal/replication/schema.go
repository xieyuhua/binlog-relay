package replication

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// SchemaLoader 从 information_schema 读取表的主键/唯一键。
//
// go-mysql 解析 binlog 时只给列名，不给主键；
// 而幂等键、删除条件都依赖主键，所以需要单独查一次。
type SchemaLoader struct {
	db *sql.DB

	mu    sync.RWMutex
	cache map[string][]string // "库.表" => keys
}

func NewSchemaLoader(db *sql.DB) *SchemaLoader {
	return &SchemaLoader{db: db, cache: make(map[string][]string)}
}

// Load 查询某表的主键；无主键时回退唯一键。
// 结果按 库.表 缓存，DDL 后应调用 Invalidate 清除。
func (l *SchemaLoader) Load(ctx context.Context, database, table string) ([]string, error) {
	key := database + "." + table

	l.mu.RLock()
	if keys, ok := l.cache[key]; ok {
		l.mu.RUnlock()
		return keys, nil
	}
	l.mu.RUnlock()

	rows, err := l.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY IN ('PRI','UNI')
		ORDER BY ORDINAL_POSITION`, database, table)
	if err != nil {
		return nil, fmt.Errorf("查询 %s 主键失败: %w", key, err)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	keys := pri
	if len(keys) == 0 {
		keys = uni
	}

	l.mu.Lock()
	l.cache[key] = keys
	l.mu.Unlock()
	return keys, nil
}

// Invalidate 在 DDL 后清除缓存（表结构可能已变）
func (l *SchemaLoader) Invalidate(database, table string) {
	l.mu.Lock()
	delete(l.cache, database+"."+table)
	l.mu.Unlock()
}

// InvalidateAll 清空全部缓存
func (l *SchemaLoader) InvalidateAll() {
	l.mu.Lock()
	l.cache = make(map[string][]string)
	l.mu.Unlock()
}
