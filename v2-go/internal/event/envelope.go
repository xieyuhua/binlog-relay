package event

import (
	"encoding/json"
	"time"
)

// Envelope 是投递到下游的统一格式。
//
// 设计取舍：采用**扁平结构**而非 `{meta..., payload:{...}}` 的嵌套。
//
// 嵌套写法有两个实测问题：
//  1. payload 需要先 Marshal 一次填进 RawMessage，Producers 再整体 Marshal 一次，
//     等于序列化两遍（实测 NewEnvelope 2.79µs，其中序列化占 83%）
//  2. payload 内部的引号会被转义，体积膨胀 80%+
//
// 扁平化后：只序列化一次，且 op/db/table 不再重复。
// 消费端仍能拿到全部字段，只是不再有 payload 这一层包装。
type Envelope struct {
	// ID 幂等键：库.表:主键值（无键表为行内容哈希）
	ID string `json:"id"`
	// Kind: row | ddl
	Kind string `json:"kind"`
	// Op: insert | update | delete | create_table | alter_table ...
	Op string `json:"op"`

	DB    string `json:"db"`
	Table string `json:"table"`

	// Pos 形如 mysql-bin.000012:4321
	Pos string `json:"pos"`
	// TS 事件在源库发生的时间
	TS int64 `json:"ts"`
	// EmittedAt 中继落地的本地时间
	EmittedAt int64 `json:"emitted_at"`

	// ---- 行变更字段（Kind=row）----

	// Row 变更后的行；delete 时为被删的行
	Row map[string]any `json:"row,omitempty"`
	// Before 仅 update 有值
	Before map[string]any `json:"before,omitempty"`
	// TableKey 主键/唯一键列名
	TableKey []string `json:"table_key,omitempty"`
	// GTID 事务号（可选）
	GTID     string `json:"gtid,omitempty"`
	ServerID uint32 `json:"server_id,omitempty"`

	// ---- DDL 字段（Kind=ddl）----

	// DDLTables DDL 涉及的表
	DDLTables []string `json:"ddl_tables,omitempty"`
	// DDLQuery 完整 SQL 语句
	DDLQuery string `json:"ddl_query,omitempty"`
}

// Marshal 直接用标准库编码，字段已是扁平结构，无需二次转换
func (e *Envelope) Marshal() ([]byte, error) { return json.Marshal(e) }

// NewEnvelope 把 Record 包装成投递信封。
//
// 只在 DDL 时做一次字符串拼接，行变更直接复用 Change 的字段（不复制 map）。
func NewEnvelope(r Record) (*Envelope, error) {
	switch v := r.(type) {
	case *Change:
		return &Envelope{
			ID:        v.Key(),
			Kind:      "row",
			Op:        string(v.Op),
			DB:        v.Database,
			Table:     v.Table,
			Pos:       v.Pos(),
			TS:        v.Timestamp,
			EmittedAt: time.Now().Unix(),
			Row:       v.Row,
			Before:    v.Before,
			TableKey:  v.TableKey,
			GTID:      v.GTID,
			ServerID:  v.ServerID,
		}, nil

	case *DDL:
		return &Envelope{
			ID:        v.Key(),
			Kind:      KindDDL,
			Op:        v.Op,
			DB:        v.Database,
			Table:     firstOrEmpty(v.Tables),
			Pos:       v.Pos(),
			TS:        v.Timestamp,
			EmittedAt: time.Now().Unix(),
			DDLTables: v.Tables,
			DDLQuery:  v.Query,
			ServerID:  v.ServerID,
		}, nil

	default:
		return nil, errUnknownRecord
	}
}

func firstOrEmpty(s []string) string {
	if len(s) > 0 {
		return s[0]
	}
	return ""
}
