<?php

declare(strict_types=1);

namespace BinlogRelay\Event;

/**
 * 统一的变更事件（框架对外的稳定契约）
 *
 * 下游 handler 只依赖这个结构，底层 DTO 升级不会影响业务代码。
 */
final readonly class Change
{
    public const INSERT = 'insert';
    public const UPDATE = 'update';
    public const DELETE = 'delete';

    /**
     * @param array<string,mixed>      $row      insert/delete 的整行；update 时为 after 行
     * @param array<string,mixed>|null $before   update 前的行，其他事件为 null
     * @param array<int,string>        $tableKey 主键列名（可用于幂等）
     */
    public function __construct(
        public string $op,
        public string $database,
        public string $table,
        public array $row,
        public ?array $before,
        public array $tableKey,
        public string $binlogFile,
        public string $binlogPos,
        public string $gtid,
        public int $timestamp,
        public int $serverId,
    ) {
    }

    public function toArray(): array
    {
        return [
            'op'          => $this->op,
            'database'    => $this->database,
            'table'       => $this->table,
            'row'         => $this->row,
            'before'      => $this->before,
            'table_key'   => $this->tableKey,
            'binlog_file' => $this->binlogFile,
            'binlog_pos'  => $this->binlogPos,
            'gtid'        => $this->gtid,
            'timestamp'   => $this->timestamp,
            'server_id'   => $this->serverId,
        ];
    }

    /**
     * 替换行数据（列裁剪用）
     *
     * @param array<string,mixed>      $row
     * @param array<string,mixed>|null $before
     */
    public function withRows(array $row, ?array $before): self
    {
        return new self(
            $this->op,
            $this->database,
            $this->table,
            $row,
            $before,
            $this->tableKey,
            $this->binlogFile,
            $this->binlogPos,
            $this->gtid,
            $this->timestamp,
            $this->serverId
        );
    }

    /** 用于下游幂等的定位键：库.表:主键值 */
    public function rowIdentity(): string
    {
        $parts = [];
        foreach ($this->tableKey as $col) {
            $parts[] = (string)($this->row[$col] ?? '');
        }
        return $this->database . '.' . $this->table . ':' . implode(',', $parts);
    }
}
