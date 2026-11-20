<?php

declare(strict_types=1);

namespace BinlogRelay\Queue;

use BinlogRelay\Event\Change;

/**
 * 队列消息契约（生产端与消费端共用）
 *
 * Redis Stream 的 entry 字段：
 *   id    Redis 生成的 时间戳-序号（消费端用来 XACK）
 *   op    insert|update|delete
 *   db    库名
 *   tbl   表名
 *   pk    幂等键 db.table:主键
 *   pos   binlog_file:pos
 *   ts    事件时间戳
 *   data  完整 Change 的 JSON（消费端优先解析它，字段最全）
 *
 * 兼容策略：data 缺失或损坏时，用扁平字段降级重建一条最小消息，
 * 保证老版本生产者 / 手工投递的消息也能消费。
 */
final readonly class Message
{
    public function __construct(
        public string $id,
        public string $op,
        public string $database,
        public string $table,
        public string $pk,
        public string $pos,
        public int $timestamp,
        public array $data,
    ) {
    }

    /** 生产端：Change → Stream 字段 */
    /** json_encode 的固定选项，避免每次调用重新求值常量表达式 */
    private const JSON_FLAGS = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE;

    public static function fromChange(Change $change): array
    {
        return [
            'op'   => $change->op,
            'db'   => $change->database,
            'tbl'  => $change->table,
            'pk'   => $change->rowIdentity(),
            'pos'  => $change->binlogFile . ':' . $change->binlogPos,
            'ts'   => (string)$change->timestamp,
            // 行数据先按 flag 编码，失败时降级为空对象，避免向 Redis 写入 null
            'data' => json_encode($change->toArray(), self::JSON_FLAGS) ?: '{}',
        ];
    }

    /** 消费端：Stream entry → Message */
    public static function fromEntry(string $id, array $fields): self
    {
        $data = [];
        if (isset($fields['data']) && is_string($fields['data'])) {
            $decoded = json_decode($fields['data'], true);
            if (is_array($decoded)) {
                $data = $decoded;
            }
        }

        // 降级：data 损坏时用扁平字段重建最小结构，至少不丢行
        if ($data === []) {
            $data = [
                'op'       => $fields['op'] ?? '',
                'database' => $fields['db'] ?? '',
                'table'    => $fields['tbl'] ?? '',
                'row'      => [],
                'before'   => null,
                'table_key' => [],
            ];
        }

        return new self(
            $id,
            (string)($fields['op'] ?? $data['op'] ?? ''),
            (string)($fields['db'] ?? $data['database'] ?? ''),
            (string)($fields['tbl'] ?? $data['table'] ?? ''),
            (string)($fields['pk'] ?? ''),
            (string)($fields['pos'] ?? ''),
            (int)($fields['ts'] ?? $data['timestamp'] ?? 0),
            $data,
        );
    }

    /**
     * 还原成 Change，便于复用 relay 侧的处理器
     *
     * 注意：binlogFile/binlogPos 已拆在 pos 字段里，这里按 ':' 还原；
     * gtid 与 server_id 在消息中没单独存，从 data 里取（旧消息可能为空）。
     */
    public function toChange(): \BinlogRelay\Event\Change
    {
        [$file, $position] = str_contains($this->pos, ':')
            ? explode(':', $this->pos, 2)
            : [$this->pos, '0'];

        return new \BinlogRelay\Event\Change(
            $this->op,
            $this->database,
            $this->table,
            $this->row(),
            $this->before(),
            $this->tableKey(),
            $file,
            $position,
            (string)($this->data['gtid'] ?? ''),
            $this->timestamp,
            (int)($this->data['server_id'] ?? 0)
        );
    }

    /** @return list<\BinlogRelay\Event\Change> */
    public static function toChanges(array $messages): array
    {
        return array_map(static fn(self $m) => $m->toChange(), $messages);
    }

    public function row(): array
    {
        return is_array($this->data['row'] ?? null) ? $this->data['row'] : [];
    }

    public function before(): ?array
    {
        return is_array($this->data['before'] ?? null) ? $this->data['before'] : null;
    }

    public function tableKey(): array
    {
        return is_array($this->data['table_key'] ?? null) ? $this->data['table_key'] : [];
    }

    public function toArray(): array
    {
        return [
            'id'  => $this->id,
            'op'  => $this->op,
            'db'  => $this->database,
            'tbl' => $this->table,
            'pk'  => $this->pk,
            'pos' => $this->pos,
            'ts'  => $this->timestamp,
            'data' => $this->data,
        ];
    }
}
