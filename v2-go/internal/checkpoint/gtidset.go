package checkpoint

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Interval 是闭区间 [Start, End]
type Interval struct {
	Start int64
	End   int64
}

// GtidSet 是 MySQL 已执行事务集合。
//
// 格式：`uuid1:1-5:8,uuid2:1-100`
// 与单个事务 GTID（`uuid:2901`）的区别在于区间合并：
// 处理 1,2,3,5 后应得到 `1-3:5`，而不是四个独立项。
//
// 为什么要维护完整集合：COM_BINLOG_DUMP_GTID 要求传已执行集合，
// 主库据此跳过这些事务。只传最后一个事务会导致主库尝试从头重放，
// 一旦相关 binlog 被清理就报 "master has purged binary logs"。
type GtidSet struct {
	// sid => 已排序且互不相邻的闭区间
	m map[string][]Interval
	// 去重：最近合并过的单事务
	last string
}

func NewGtidSet() *GtidSet {
	return &GtidSet{m: make(map[string][]Interval)}
}

// Parse 解析 MySQL 格式集合
func ParseGtidSet(s string) (*GtidSet, error) {
	set := NewGtidSet()
	s = strings.TrimSpace(s)
	if s == "" {
		return set, nil
	}

	for _, chunk := range strings.Split(s, ",") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		colon := strings.Index(chunk, ":")
		if colon <= 0 {
			return nil, fmt.Errorf("非法的 GTID 集合片段: %s", chunk)
		}
		sid := strings.ToLower(strings.TrimSpace(chunk[:colon]))
		if len(sid) != 36 {
			return nil, fmt.Errorf("非法的 SID: %s", sid)
		}

		for _, rng := range strings.Split(chunk[colon+1:], ":") {
			if rng == "" {
				continue
			}
			lo, hi, err := parseRange(rng)
			if err != nil {
				return nil, err
			}
			set.addInterval(sid, lo, hi)
		}
	}
	return set, nil
}

func parseRange(s string) (int64, int64, error) {
	if i := strings.Index(s, "-"); i > 0 {
		lo, err := strconv.ParseInt(s[:i], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("非法区间: %s", s)
		}
		hi, err := strconv.ParseInt(s[i+1:], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("非法区间: %s", s)
		}
		if hi < lo {
			lo, hi = hi, lo
		}
		return lo, hi, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("非法序号: %s", s)
	}
	return n, n, nil
}

// AddGtid 合并单个事务（uuid:2901）。重复添加同一事务是无害的。
func (g *GtidSet) AddGtid(gtid string) error {
	gtid = strings.TrimSpace(gtid)
	if gtid == g.last {
		return nil
	}
	colon := strings.LastIndex(gtid, ":")
	if colon <= 0 {
		return fmt.Errorf("非法的单个 GTID: %s", gtid)
	}
	sid := strings.ToLower(gtid[:colon])
	n, err := strconv.ParseInt(gtid[colon+1:], 10, 64)
	if err != nil {
		return fmt.Errorf("非法的 GTID 序号: %s", gtid)
	}
	g.addInterval(sid, n, n)
	g.last = gtid
	return nil
}

// addInterval 插入并合并
func (g *GtidSet) addInterval(sid string, lo, hi int64) {
	list := g.m[sid]
	list = append(list, Interval{Start: lo, End: hi})
	sort.Slice(list, func(i, j int) bool { return list[i].Start < list[j].Start })

	merged := make([]Interval, 0, len(list))
	cur := list[0]
	for _, it := range list[1:] {
		// 相邻（it.Start == cur.End+1）或重叠都合并
		if it.Start <= cur.End+1 {
			if it.End > cur.End {
				cur.End = it.End
			}
			continue
		}
		merged = append(merged, cur)
		cur = it
	}
	merged = append(merged, cur)
	g.m[sid] = merged
}

// Merge 并入另一个集合（用于从主库 gtid_executed 初始化）
func (g *GtidSet) Merge(other *GtidSet) {
	for sid, list := range other.m {
		for _, it := range list {
			g.addInterval(sid, it.Start, it.End)
		}
	}
}

// String 序列化为 MySQL 标准格式（SID 按字典序，保证输出稳定）
func (g *GtidSet) String() string {
	if len(g.m) == 0 {
		return ""
	}
	sids := make([]string, 0, len(g.m))
	for sid := range g.m {
		sids = append(sids, sid)
	}
	sort.Strings(sids)

	parts := make([]string, 0, len(sids))
	for _, sid := range sids {
		ranges := make([]string, 0, len(g.m[sid]))
		for _, it := range g.m[sid] {
			if it.Start == it.End {
				ranges = append(ranges, strconv.FormatInt(it.Start, 10))
			} else {
				ranges = append(ranges, fmt.Sprintf("%d-%d", it.Start, it.End))
			}
		}
		parts = append(parts, sid+":"+strings.Join(ranges, ":"))
	}
	return strings.Join(parts, ",")
}

// Count 返回事务总数
func (g *GtidSet) Count() int64 {
	var n int64
	for _, list := range g.m {
		for _, it := range list {
			n += it.End - it.Start + 1
		}
	}
	return n
}

// IsEmpty 判断是否为空
func (g *GtidSet) IsEmpty() bool { return len(g.m) == 0 }
