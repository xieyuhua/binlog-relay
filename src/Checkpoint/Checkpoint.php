<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

/**
 * 消费点位（不可变值对象）
 *
 * 恢复方式有两种，优先级由调用方决定：
 *   - file+pos：精确、无依赖，**默认使用**
 *   - GTID：需要累积的已执行集合，只有显式开启时才用
 *
 * 重要：本类的 gtid 字段来自 binlog 事件里的单个 GTID（如 uuid:2901），
 * 表示「最后处理的那个事务」，**不是** MySQL 意义上的已执行集合。
 * 直接拿去 COM_BINLOG_DUMP_GTID 会让主库报 «master has purged binary logs»，
 * 所以默认必须走 file+pos。仅在文件被清理、需要跨文件跳转时才考虑 GTID 模式，
 * 且那时应当使用 `SELECT @@GLOBAL.gtid_executed` 的结果。
 */
final readonly class Checkpoint
{
    public function __construct(
        public string $binlogFile = '',
        public string $binlogPos = '0',
        public string $gtid = '',
        public int $updatedAt = 0,
        public int $events = 0,
        public string $owner = '',
    ) {
    }

    /** 是否具备 file+pos 点位（可以精确续传） */
    public function hasFilePosition(): bool
    {
        return $this->binlogFile !== '';
    }

    /**
     * 是否应该按 GTID 恢复
     *
     * 只有在**没有** file+pos、但存在 gtid 时才考虑。
     * 两者都有时以 file+pos 为准，因为事件里的单事务 GTID 不是执行集合。
     */
    public function shouldResumeByGtid(): bool
    {
        return !$this->hasFilePosition() && $this->gtid !== '';
    }

    public function withProgress(int $events, string $owner): self
    {
        return new self($this->binlogFile, $this->binlogPos, $this->gtid, time(), $events, $owner);
    }

    /** 用 file+pos 更新（保留 gtid 仅作展示） */
    public function advance(string $file, string $pos): self
    {
        return new self($file, $pos, $this->gtid, $this->updatedAt, $this->events, $this->owner);
    }

    public function toArray(): array
    {
        return [
            'binlog_file' => $this->binlogFile,
            'binlog_pos'  => $this->binlogPos,
            'gtid'        => $this->gtid,
            'updated_at'  => $this->updatedAt,
            'events'      => $this->events,
            'owner'       => $this->owner,
        ];
    }

    public static function fromArray(array $a): self
    {
        return new self(
            (string)($a['binlog_file'] ?? ''),
            (string)($a['binlog_pos'] ?? '0'),
            (string)($a['gtid'] ?? ''),
            (int)($a['updated_at'] ?? 0),
            (int)($a['events'] ?? 0),
            (string)($a['owner'] ?? ''),
        );
    }
}
