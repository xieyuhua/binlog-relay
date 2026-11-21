// Package filter 实现库/表/列三级过滤。
package filter

import (
	"sync"

	"github.com/biaoge/binlog2redis/internal/config"
)

// Filter 决定一条变更是否放行，以及要保留哪些列。
//
// 判定结果按 库+表 缓存：同一张表的连续事件非常多，
// 缓存能把重复的字符串比较降到一次哈希查找。
type Filter struct {
	dbOnly      map[string]struct{}
	tablesOnly  map[string]struct{}
	dbIgnore    map[string]struct{}
	tableIgnore map[string]struct{}
	configured  map[string]struct{}
	onlyConf    bool

	// columnsByTable: 表名 => 保留列集合；"*" 为兜底
	columnsByTable map[string]map[string]struct{}
	columnsDefault map[string]struct{}

	mu    sync.RWMutex
	cache map[string]bool
}

// New 从配置构造过滤器
func New(cfg config.FilterConfig) *Filter {
	f := &Filter{
		dbOnly:         toSet(cfg.Databases),
		tablesOnly:     toSet(cfg.Tables),
		dbIgnore:       toSet(cfg.IgnoreDatabases),
		tableIgnore:    toSet(cfg.IgnoreTables),
		onlyConf:       cfg.OnlyConfigured,
		columnsByTable: make(map[string]map[string]struct{}),
		cache:          make(map[string]bool, 256),
	}

	for table, cols := range cfg.Columns {
		set := toSet(cols)
		if table == "*" {
			f.columnsDefault = set
			continue
		}
		f.columnsByTable[table] = set
	}

	// only_configured 的清单 = tables ∪ columns 的表名（并集，非交集）
	if f.onlyConf {
		f.configured = make(map[string]struct{}, len(cfg.Tables)+len(f.columnsByTable))
		for t := range f.tablesOnly {
			f.configured[t] = struct{}{}
		}
		for t := range f.columnsByTable {
			f.configured[t] = struct{}{}
		}
	}

	return f
}

// Allow 判断某库某表是否放行。忽略规则优先级最高。
func (f *Filter) Allow(database, table string) bool {
	key := database + "\x00" + table

	f.mu.RLock()
	if v, ok := f.cache[key]; ok {
		f.mu.RUnlock()
		return v
	}
	f.mu.RUnlock()

	ok := f.decide(database, table)

	f.mu.Lock()
	if len(f.cache) > 4096 { // 表数量有限，超限整体清空即可
		f.cache = make(map[string]bool, 256)
	}
	f.cache[key] = ok
	f.mu.Unlock()

	return ok
}

func (f *Filter) decide(database, table string) bool {
	if _, hit := f.dbIgnore[database]; hit {
		return false
	}
	if _, hit := f.tableIgnore[table]; hit {
		return false
	}
	if len(f.dbOnly) > 0 {
		if _, hit := f.dbOnly[database]; !hit {
			return false
		}
	}
	if f.onlyConf {
		_, hit := f.configured[table]
		return hit
	}
	if len(f.tablesOnly) > 0 {
		_, hit := f.tablesOnly[table]
		return hit
	}
	return true
}

// KeepSet 返回该表要保留的列集合；nil 表示不裁剪。
//
// 调用方需要自行把主键列并入（否则下游无法幂等）。
func (f *Filter) KeepSet(table string, tableKey []string) map[string]struct{} {
	var base map[string]struct{}
	if s, ok := f.columnsByTable[table]; ok {
		base = s
	} else if f.columnsDefault != nil {
		base = f.columnsDefault
	} else {
		return nil
	}

	// 复制一份再并入主键，避免污染共享配置
	out := make(map[string]struct{}, len(base)+len(tableKey))
	for k := range base {
		out[k] = struct{}{}
	}
	for _, k := range tableKey {
		out[k] = struct{}{}
	}
	return out
}

// Tables 返回显式配置的表名（供日志与调试）
func (f *Filter) Tables() []string {
	out := make([]string, 0, len(f.tablesOnly))
	for t := range f.tablesOnly {
		out = append(out, t)
	}
	return out
}

func toSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		if it != "" {
			m[it] = struct{}{}
		}
	}
	return m
}
