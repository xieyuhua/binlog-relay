<?php

declare(strict_types=1);

namespace BinlogRelay\FullSync;

use BinlogRelay\Event\Change;
use BinlogRelay\Queue\Message;
use BinlogRelay\Support\Logger;
use PDO;
use RuntimeException;

/**
 * 全量同步：把现存数据以快照形式推送一遍
 *
 * ## 为什么需要它
 *
 * 增量同步只捕获「开始订阅之后」的变化。如果下游是空库、或与源库有历史偏差，
 * 光靠增量永远补不齐。必须先做一次全量，再切到增量。
 *
 * ## 如何保证不丢数据（关键）
 *
 * 采用 **先记位点、后快照** 的顺序：
 *
 *   1. 读取当前 binlog 位点 P，并立刻持久化到点位存储
 *   2. 从 P 开始做全量快照（逐表分批 SELECT）
 *   3. 快照完成后启动 relay，它会从 P 开始回放
 *
 * 这样：
 * - **不丢**：P 之后的所有变更都会被 relay 回放
 * - **可能重复**：快照期间发生变更的行，快照读到的是新值，回放时又写一次
 *
 * 重复由下游幂等吸收（`ON DUPLICATE KEY UPDATE` / ES `_id` / CK 主键去重）。
 * 这是 binlog 全量+增量的标准做法，Maxwell / Canal 也是这个思路。
 *
 * **不要反过来做**（先快照后记位点）：那会丢掉快照期间发生的变更。
 */
final class FullSyncer
{
    public function __construct(
        private readonly PDO $pdo,
        private readonly Logger $logger,
        private readonly int $batchSize = 1000,
    ) {
    }

    /**
     * 列出待同步的表
     *
     * $includeViews 为 true 时把视图也纳入。
     * 注意：视图的数据来自基表，同一份数据会以不同 table 名重复入队，
     * 下游需要按 table 区分处理，或自行去重。
     *
     * @param list<string> $databases    为空表示全部（排除系统库）
     * @param list<string> $tables       为空表示这些库的全部表
     * @param bool         $includeViews 是否包含视图
     * @return list<array{database:string,table:string,type:string}>
     */
    public function listTables(array $databases = [], array $tables = [], bool $includeViews = false): array
    {
        $ignore = ['mysql', 'sys', 'information_schema', 'performance_schema'];

        $types = $includeViews ? ['BASE TABLE', 'VIEW'] : ['BASE TABLE'];

        $sql = 'SELECT TABLE_SCHEMA AS db, TABLE_NAME AS tbl, TABLE_TYPE AS type
                FROM information_schema.TABLES
                WHERE TABLE_TYPE IN (' . implode(',', array_fill(0, count($types), '?')) . ')
                  AND TABLE_SCHEMA NOT IN (' . implode(',', array_fill(0, count($ignore), '?')) . ')';
        $params = array_merge($types, $ignore);

        if ($databases !== []) {
            $sql .= ' AND TABLE_SCHEMA IN (' . implode(',', array_fill(0, count($databases), '?')) . ')';
            array_push($params, ...$databases);
        }
        if ($tables !== []) {
            $sql .= ' AND TABLE_NAME IN (' . implode(',', array_fill(0, count($tables), '?')) . ')';
            array_push($params, ...$tables);
        }
        $sql .= ' ORDER BY TABLE_SCHEMA, TABLE_NAME';

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);

        return array_map(
            static fn(array $r) => [
                'database' => $r['db'],
                'table'    => $r['tbl'],
                'type'     => strtoupper((string)$r['type']) === 'VIEW' ? 'view' : 'table',
            ],
            $stmt->fetchAll()
        );
    }

    /**
     * 同步单张表的全部数据
     *
     * 按主键分页（避免大 OFFSET 的性能问题）。无主键表退化为一次性全读，
     * 并给出告警——这类表无法保证快照期间的一致性，应尽量避免。
     *
     * @param callable(list<Change>):void $sink 收到一批 Change
     * @return int 同步的行数
     */
    public function syncTable(string $database, string $table, callable $sink): int
    {
        $keys = $this->primaryKeys($database, $table);
        $quoted = $this->quote($database) . '.' . $this->quote($table);

        $total = 0;
        $lastKey = null;
        $columns = $this->columns($database, $table);

        if ($keys === []) {
            $this->logger->warning("表 {$database}.{$table} 无主键，将以单次全读方式快照（可能不一致）");
            $stmt = $this->pdo->query("SELECT * FROM {$quoted}");
            while (($row = $stmt->fetch(PDO::FETCH_ASSOC)) !== false) {
                $sink([$this->toChange($database, $table, $row, $keys)]);
                $total++;
            }
            return $total;
        }

        while (true) {
            $rows = $this->fetchPage($quoted, $keys, $columns, $lastKey);
            if ($rows === []) {
                break;
            }

            $changes = [];
            foreach ($rows as $row) {
                $changes[] = $this->toChange($database, $table, $row, $keys);
            }
            $sink($changes);
            $total += count($changes);

            // 用最后一行的主键作为下一页游标
            $lastRow = $rows[array_key_last($rows)];
            $lastKey = array_map(
                static fn(string $k) => $lastRow[$k],
                $keys
            );

            if (count($rows) < $this->batchSize) {
                break;
            }
        }

        return $total;
    }

    /** @return list<array<string,mixed>> */
    private function fetchPage(string $quoted, array $keys, array $columns, ?array $lastKey): array
    {
        $select = implode(',', array_map([$this, 'quote'], $columns));
        $order = implode(',', array_map([$this, 'quote'], $keys));

        if ($lastKey === null) {
            $sql = "SELECT {$select} FROM {$quoted} ORDER BY {$order} LIMIT {$this->batchSize}";
            $stmt = $this->pdo->query($sql);
            return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        }

        // 复合主键的游标比较：(k1,k2) > (v1,v2)
        // 单主键退化为 k1 > v1，明显更快
        if (count($keys) === 1) {
            $k = $this->quote($keys[0]);
            $sql = "SELECT {$select} FROM {$quoted} WHERE {$k} > ? ORDER BY {$order} LIMIT {$this->batchSize}";
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([$lastKey[0]]);
            return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
        }

        $conditions = [];
        $params = [];
        // (a>?) OR (a=? AND b>?) OR (a=? AND b=? AND c>?) ...
        for ($i = 0, $n = count($keys); $i < $n; $i++) {
            $parts = [];
            for ($j = 0; $j < $i; $j++) {
                $parts[] = $this->quote($keys[$j]) . ' = ?';
                $params[] = $lastKey[$j];
            }
            $parts[] = $this->quote($keys[$i]) . ' > ?';
            $params[] = $lastKey[$i];
            $conditions[] = '(' . implode(' AND ', $parts) . ')';
        }
        $sql = "SELECT {$select} FROM {$quoted} WHERE " . implode(' OR ', $conditions)
            . " ORDER BY {$order} LIMIT {$this->batchSize}";

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    private function toChange(string $database, string $table, array $row, array $keys): Change
    {
        return new Change(
            Change::INSERT,   // 快照统一按 insert 处理，下游用幂等写覆盖
            $database,
            $table,
            $row,
            null,
            $keys,
            '',              // 快照不对应具体 binlog 位置
            '0',
            '',
            time(),
            0
        );
    }

    /** @return list<string> */
    private function primaryKeys(string $database, string $table): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = ?
             ORDER BY ORDINAL_POSITION'
        );
        $stmt->execute([$database, $table, 'PRI']);
        return $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
    }

    /** @return list<string> */
    private function columns(string $database, string $table): array
    {
        $stmt = $this->pdo->prepare(
            'SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
             ORDER BY ORDINAL_POSITION'
        );
        $stmt->execute([$database, $table]);
        return $stmt->fetchAll(PDO::FETCH_COLUMN) ?: [];
    }

    private function quote(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    /**
     * 把快照行转成队列消息（与增量完全相同的格式）
     *
     * @param list<Change> $changes
     * @return list<array<string,string>>
     */
    public static function toMessages(array $changes): array
    {
        return array_map(
            static fn(Change $c) => Message::fromChange($c),
            $changes
        );
    }
}
