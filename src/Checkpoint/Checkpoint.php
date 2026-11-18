<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

/**
 * 消费点位（不可变值对象）
 *
 * GTID 模式与 file+pos 模式二选一，统一用这个结构在存储层流转。
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

    public function isGtidMode(): bool
    {
        return $this->gtid !== '';
    }

    public function withProgress(int $events, string $owner): self
    {
        return new self($this->binlogFile, $this->binlogPos, $this->gtid, time(), $events, $owner);
    }

    /** 用 file+pos 更新（GTID 模式下保留 gtid 不动） */
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
