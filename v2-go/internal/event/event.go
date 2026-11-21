// Package event 定义对外的稳定事件契约。
//
// Change 描述行级变更，DDL 描述结构变更。两者都实现了 Record 接口，
// 下游 producer 只需处理 Record。
package event

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Op 是行操作类型
type Op string

const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// Record 是可以投递给下游的一条记录
type Record interface {
	// Kind 返回记录类别：row / ddl
	Kind() string
	// Key 是幂等定位键
	Key() string
	// Marshal 返回要投递的 JSON 载荷
	Marshal() ([]byte, error)
	// Topic 供多主题路由使用
	Topic() TopicMeta
}

// TopicMeta 是路由判定所需的元信息
type TopicMeta struct {
	Database string
	Table    string
	Op       string
}

// Change 是一条行级变更
type Change struct {
	Op         Op             `json:"op"`
	Database   string         `json:"database"`
	Table      string         `json:"table"`
	Row        map[string]any `json:"row"`
	Before     map[string]any `json:"before,omitempty"`
	TableKey   []string       `json:"table_key,omitempty"`
	BinlogFile string         `json:"binlog_file"`
	BinlogPos  int64          `json:"binlog_pos"`
	GTID       string         `json:"gtid,omitempty"`
	Timestamp  int64          `json:"timestamp"`
	ServerID   uint32         `json:"server_id"`
}

func (c *Change) Kind() string { return "row" }

func (c *Change) Topic() TopicMeta {
	return TopicMeta{Database: c.Database, Table: c.Table, Op: string(c.Op)}
}

// Key 返回幂等定位键。
// 有主键/唯一键时返回 `库.表:键值1,键值2`；
// 无键表退化为行内容哈希（删除时用 before，保证与对应 insert 一致）。
func (c *Change) Key() string {
	prefix := c.Database + "." + c.Table + ":"
	if len(c.TableKey) == 0 {
		src := c.Row
		if c.Op == OpDelete && c.Before != nil {
			src = c.Before
		}
		b, _ := json.Marshal(src)
		sum := sha1.Sum(b)
		return prefix + "#" + hex.EncodeToString(sum[:])[:16]
	}

	parts := make([]string, 0, len(c.TableKey))
	for _, k := range c.TableKey {
		parts = append(parts, fmt.Sprint(c.Row[k]))
	}
	return prefix + strings.Join(parts, ",")
}

func (c *Change) Marshal() ([]byte, error) { return json.Marshal(c) }

// Pos 便于日志输出
func (c *Change) Pos() string {
	return fmt.Sprintf("%s:%d", c.BinlogFile, c.BinlogPos)
}

// DDL 是一条结构变更
type DDL struct {
	Op         string   `json:"op"` // create_table / alter_table / ...
	Database   string   `json:"database"`
	Tables     []string `json:"tables,omitempty"`
	Query      string   `json:"query"`
	BinlogFile string   `json:"binlog_file"`
	BinlogPos  int64    `json:"binlog_pos"`
	Timestamp  int64    `json:"timestamp"`
	ServerID   uint32   `json:"server_id"`
}

const KindDDL = "ddl"

func (d *DDL) Kind() string { return KindDDL }

func (d *DDL) Topic() TopicMeta {
	tbl := ""
	if len(d.Tables) > 0 {
		tbl = d.Tables[0]
	}
	return TopicMeta{Database: d.Database, Table: tbl, Op: KindDDL}
}

// Key 用位置 + 语句哈希：同一位置可能有多个 DDL
func (d *DDL) Key() string {
	sum := sha1.Sum([]byte(d.Query))
	return fmt.Sprintf("%s:%d:ddl:%s", d.BinlogFile, d.BinlogPos, hex.EncodeToString(sum[:])[:8])
}

func (d *DDL) Marshal() ([]byte, error) { return json.Marshal(d) }

// Pos 便于日志输出
func (d *DDL) Pos() string {
	return fmt.Sprintf("%s:%d", d.BinlogFile, d.BinlogPos)
}
