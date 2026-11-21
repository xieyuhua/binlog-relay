<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

/**
 * GTID 已执行集合
 *
 * MySQL 的 gtid_executed 格式：`uuid1:1-5:7:9-12,uuid2:1-3,uuid3:1-100`
 * 与单个事务 GTID（`uuid:2901`）的区别在于**区间合并**：
 * 处理 1,2,3,5 后会合并成 `1-3:5`，而不是保留 4 个独立项。
 *
 * 为什么需要它：
 *   COM_BINLOG_DUMP_GTID 要求传「已执行的完整集合」，主库据此跳过这些事务。
 *   如果只传最后一个事务（旧实现的做法），主库会认为前面的都没执行过，
 *   于是尝试从头重放——一旦相关 binlog 被清理就报
 *   «master has purged binary logs»。
 *
 * 实现要点：
 * - 按 SID 分组，每个 SID 维护有序的闭区间列表
 * - 插入时做区间合并（相邻/重叠都合并），保证集合紧凑
 * - 序列化为 MySQL 标准格式，可直接喂给 withGtid()
 */
final class GtidSet
{
    /** @var array<string, list<array{0:int,1:int}>> sid => [[start, end], ...] */
    private array $intervals = [];

    /** 集合是否为空 */
    public function isEmpty(): bool
    {
        return $this->intervals === [];
    }

    /** 已包含的事务总数（按区间宽度累加） */
    public function count(): int
    {
        $n = 0;
        foreach ($this->intervals as $list) {
            foreach ($list as [$start, $end]) {
                $n += $end - $start + 1;
            }
        }
        return $n;
    }

    /**
     * 解析 MySQL 格式的 GTID 集合
     *
     * 容忍空串与多余空白；格式非法时抛异常（避免静默吞掉错误数据）。
     */
    public static function parse(string $gtidSet): self
    {
        $set = new self();
        $gtidSet = trim($gtidSet);
        if ($gtidSet === '') {
            return $set;
        }

        foreach (explode(',', $gtidSet) as $chunk) {
            $chunk = trim($chunk);
            if ($chunk === '') {
                continue;
            }
            if (!preg_match('/^([0-9a-fA-F-]{36})((?::[0-9-]+)+)$/', $chunk, $m)) {
                throw new \InvalidArgumentException("非法的 GTID 集合片段：{$chunk}");
            }
            $sid = strtolower($m[1]);
            foreach (array_filter(explode(':', $m[2])) as $interval) {
                $part = explode('-', $interval);
                $start = (int)$part[0];
                $end = isset($part[1]) ? (int)$part[1] : $start;
                if ($end < $start) {
                    [$start, $end] = [$end, $start];
                }
                $set->addInterval($sid, $start, $end);
            }
        }

        return $set;
    }

    /** 加入单个事务（uuid:2901 形式） */
    public function addGtid(string $gtid): void
    {
        if (!preg_match('/^([0-9a-fA-F-]{36}):(\d+)$/', trim($gtid), $m)) {
            throw new \InvalidArgumentException("非法的单个 GTID：{$gtid}");
        }
        $this->addInterval(strtolower($m[1]), (int)$m[2], (int)$m[2]);
    }

    /** 加入一段区间并合并 */
    public function addInterval(string $sid, int $start, int $end): void
    {
        if (!isset($this->intervals[$sid])) {
            $this->intervals[$sid] = [[$start, $end]];
            return;
        }

        $list = $this->intervals[$sid];
        $list[] = [$start, $end];

        // 按起点排序后线性合并：相邻（next.start <= cur.end + 1）即合并
        usort($list, static fn(array $a, array $b): int => $a[0] <=> $b[0]);
        $merged = [];
        [$curStart, $curEnd] = $list[0];
        foreach ($list as $i => $item) {
            if ($i === 0) {
                continue;
            }
            [$s, $e] = $item;
            if ($s <= $curEnd + 1) {
                $curEnd = max($curEnd, $e);
            } else {
                $merged[] = [$curStart, $curEnd];
                [$curStart, $curEnd] = [$s, $e];
            }
        }
        $merged[] = [$curStart, $curEnd];

        $this->intervals[$sid] = $merged;
    }

    /** 合并另一个集合（用于从主库导出的 gtid_executed 初始化） */
    public function merge(self $other): void
    {
        foreach ($other->intervals as $sid => $list) {
            foreach ($list as [$start, $end]) {
                $this->addInterval($sid, $start, $end);
            }
        }
    }

    /** 序列化为 MySQL 标准格式 */
    public function toString(): string
    {
        if ($this->intervals === []) {
            return '';
        }

        ksort($this->intervals);
        $parts = [];
        foreach ($this->intervals as $sid => $list) {
            $ranges = [];
            foreach ($list as [$start, $end]) {
                $ranges[] = $start === $end ? (string)$start : "{$start}-{$end}";
            }
            $parts[] = $sid . ':' . implode(':', $ranges);
        }
        return implode(',', $parts);
    }

    /** 是否包含某个事务 */
    public function contains(string $gtid): bool
    {
        if (!preg_match('/^([0-9a-fA-F-]{36}):(\d+)$/', trim($gtid), $m)) {
            return false;
        }
        $sid = strtolower($m[1]);
        $gno = (int)$m[2];
        foreach ($this->intervals[$sid] ?? [] as [$start, $end]) {
            if ($gno >= $start && $gno <= $end) {
                return true;
            }
        }
        return false;
    }

    /** 去重后的 SID 数量，用于日志展示 */
    public function sidCount(): int
    {
        return count($this->intervals);
    }

    /** 用于持久化的紧凑表示（与 toString 相同，保留接口语义） */
    public function toArray(): array
    {
        return ['gtid_executed' => $this->toString()];
    }

    public static function fromArray(array $data): self
    {
        return self::parse((string)($data['gtid_executed'] ?? ''));
    }

    public function __toString(): string
    {
        return $this->toString();
    }
}
