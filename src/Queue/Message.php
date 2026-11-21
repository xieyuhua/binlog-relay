<?php

declare(strict_types=1);

namespace BinlogRelay\Queue;

use BinlogRelay\Event\Change;
use BinlogRelay\Event\DdlChange;

/**
 * 队列消息契约（生产端与消费端共用）
 *
 * 采用**扁平结构**，与 Go 版 v2-go 完全一致：
 *
 *   {
 *     "id": "shop.orders:7",          // 幂等键
 *     "kind": "row",                  // row | ddl
 *     "op": "update",                 // insert|update|delete|create_table|...
 *     "db": "shop", "table": "orders",
 *     "pos": "mysql-bin.000012:4321",
 *     "ts": 1795013415,               // 源库事件时间
 *     "emitted_at": 1795013500,       // 中继落地时间
 *     "row": {...}, "before": {...}, "table_key": ["id"],
 *     "gtid": "...", "server_id": 1,
 *     "labels": {"env": "prod"}       // topic 级静态标签
 *   }
 *
 * DDL 事件用 ddl_tables / ddl_query 字段。
 *
 * 为什么不用嵌套的 {meta, data:{...}}：
 *   data 是 JSON 字符串时，消费端要 decode 两次；Redis/Kafka 里也是转义后的
 *   文本，体积膨胀约 30%。扁平化后只序列化一次。
 *
 * 兼容：仍能读取旧格式（带 data 字段的嵌套结构），仅用于平滑升级。
 */
final readonly class Message
{
    /** json_encode 的固定选项 */
    private const JSON_FLAGS = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE;

    /**
     * @param array<string,mixed>      $row
     * @param array<string,mixed>|null $before
     * @param list<string>             $tableKey
     * @param array<string,string>     $labels
     */
    public function __construct(
        /** Redis entry id，消费端用来 XACK；生产端构造时为空 */
        public string $entryId,
        /** 幂等键：库.表:主键值 */
        public string $id,
        public string $kind,
        public string $op,
        public string $database,
        public string $table,
        public string $pos,
        public int $timestamp,
        public int $emittedAt,
        public array $row,
        public ?array $before,
        public array $tableKey,
        public string $gtid,
        public int $serverId,
        public array $labels = [],
        // DDL 专用
        public array $ddlTables = [],
        public string $ddlQuery = '',
    ) {
    }

    /**
     * 生产端：Change → 扁平字段数组
     *
     * @param array<string,string> $labels topic 级静态标签，会合并进消息顶层
     * @return array<string,mixed> 每个值都是标量或数组，Redis/Kafka 可直接存
     */
    public static function fromChange(Change $change, array $labels = []): array
    {
        $fields = [
            'id'         => $change->rowIdentity(),
            'kind'       => 'row',
            'op'         => $change->op,
            'db'         => $change->database,
            'table'      => $change->table,
            'pos'        => $change->binlogFile . ':' . $change->binlogPos,
            'ts'         => $change->timestamp,
            'emitted_at' => time(),
            'row'        => self::jsonSafe($change->row),
            'table_key'  => $change->tableKey,
        ];

        if ($change->before !== null) {
            $fields['before'] = self::jsonSafe($change->before);
        }
        if ($change->gtid !== '') {
            $fields['gtid'] = $change->gtid;
        }
        if ($change->serverId > 0) {
            $fields['server_id'] = $change->serverId;
        }
        if ($labels !== []) {
            $fields['labels'] = $labels;
        }

        return $fields;
    }

    /**
     * 生产端：DDL → 扁平字段数组
     *
     * @param array<string,string> $labels
     * @return array<string,mixed>
     */
    public static function fromDdl(DdlChange $ddl, array $labels = []): array
    {
        $fields = [
            'id'         => $ddl->identity(),
            'kind'       => 'ddl',
            'op'         => $ddl->op,
            'db'         => $ddl->database,
            'table'      => $ddl->tables[0] ?? '',
            'pos'        => $ddl->binlogFile . ':' . $ddl->binlogPos,
            'ts'         => $ddl->timestamp,
            'emitted_at' => time(),
            'ddl_tables' => $ddl->tables,
            'ddl_query'  => $ddl->query,
        ];
        if ($ddl->serverId > 0) {
            $fields['server_id'] = $ddl->serverId;
        }
        if ($labels !== []) {
            $fields['labels'] = $labels;
        }
        return $fields;
    }

    /** 消费端：Stream entry → Message（兼容旧嵌套格式） */
    public static function fromEntry(string $id, array $fields): self
    {
        // 旧格式：行数据在 data 字段里（JSON 字符串）
        if (isset($fields['data']) && is_string($fields['data']) && !isset($fields['row'])) {
            return self::fromLegacyEntry($id, $fields);
        }

        $row = is_array($fields['row'] ?? null) ? $fields['row'] : [];
        $before = is_array($fields['before'] ?? null) ? $fields['before'] : null;
        $tableKey = is_array($fields['table_key'] ?? null) ? array_values($fields['table_key']) : [];
        $labels = is_array($fields['labels'] ?? null) ? $fields['labels'] : [];
        $ddlTables = is_array($fields['ddl_tables'] ?? null) ? array_values($fields['ddl_tables']) : [];

        return new self(
            entryId: $id,
            id: (string)($fields['id'] ?? ''),
            kind: (string)($fields['kind'] ?? 'row'),
            op: (string)($fields['op'] ?? ''),
            database: (string)($fields['db'] ?? ''),
            table: (string)($fields['table'] ?? ''),
            pos: (string)($fields['pos'] ?? ''),
            timestamp: (int)($fields['ts'] ?? 0),
            emittedAt: (int)($fields['emitted_at'] ?? 0),
            row: $row,
            before: $before,
            tableKey: $tableKey,
            gtid: (string)($fields['gtid'] ?? ''),
            serverId: (int)($fields['server_id'] ?? 0),
            labels: array_map('strval', $labels),
            ddlTables: array_map('strval', $ddlTables),
            ddlQuery: (string)($fields['ddl_query'] ?? ''),
        );
    }

    /** 旧格式兜底：data 里是完整的 Change JSON */
    private static function fromLegacyEntry(string $id, array $fields): self
    {
        $data = json_decode((string)$fields['data'], true);
        if (!is_array($data)) {
            $data = [];
        }

        return new self(
            entryId: $id,
            id: (string)($fields['pk'] ?? ''),
            kind: 'row',
            op: (string)($fields['op'] ?? $data['op'] ?? ''),
            database: (string)($fields['db'] ?? $data['database'] ?? ''),
            table: (string)($fields['tbl'] ?? $data['table'] ?? ''),
            pos: (string)($fields['pos'] ?? ''),
            timestamp: (int)($fields['ts'] ?? $data['timestamp'] ?? 0),
            emittedAt: 0,
            row: is_array($data['row'] ?? null) ? $data['row'] : [],
            before: is_array($data['before'] ?? null) ? $data['before'] : null,
            tableKey: is_array($data['table_key'] ?? null) ? array_values($data['table_key']) : [],
            gtid: (string)($data['gtid'] ?? ''),
            serverId: (int)($data['server_id'] ?? 0),
            labels: [],
        );
    }

    /**
     * 还原成 Change，供复用 relay 侧处理器
     */
    public function toChange(): Change
    {
        [$file, $position] = str_contains($this->pos, ':')
            ? explode(':', $this->pos, 2)
            : [$this->pos, '0'];

        return new Change(
            $this->op,
            $this->database,
            $this->table,
            $this->row,
            $this->before,
            $this->tableKey,
            $file,
            $position,
            $this->gtid,
            $this->timestamp,
            $this->serverId
        );
    }

    /** @return list<Change> */
    public static function toChanges(array $messages): array
    {
        return array_map(static fn(self $m) => $m->toChange(), $messages);
    }

    public function row(): array
    {
        return $this->row;
    }

    public function before(): ?array
    {
        return $this->before;
    }

    public function tableKey(): array
    {
        return $this->tableKey;
    }

    public function toArray(): array
    {
        return [
            'id'         => $this->id,
            'kind'       => $this->kind,
            'op'         => $this->op,
            'db'         => $this->database,
            'table'      => $this->table,
            'pos'        => $this->pos,
            'ts'         => $this->timestamp,
            'emitted_at' => $this->emittedAt,
            'row'        => $this->row,
            'before'     => $this->before,
            'table_key'  => $this->tableKey,
            'gtid'       => $this->gtid,
            'server_id'  => $this->serverId,
            'labels'     => $this->labels,
            'ddl_tables' => $this->ddlTables,
            'ddl_query'  => $this->ddlQuery,
        ];
    }

    /** 保证行数据可被 json_encode */
    private static function jsonSafe(array $row): array
    {
        foreach ($row as $k => $v) {
            if (is_resource($v)) {
                $row[$k] = null;
            } elseif (is_object($v) && !$v instanceof \JsonSerializable) {
                $row[$k] = (string)$v;
            }
        }
        return $row;
    }
}
