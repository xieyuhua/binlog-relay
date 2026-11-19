<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;
use PDO;
use RuntimeException;

/**
 * MySQL 落库：批量 upsert / delete
 *
 * 目标表名规则：
 *   table_prefix + 源表名
 *   例如 prefix=mirror_、源表 orders → mirror_orders
 *
 * 要求：
 * - 目标表结构与源表列一致（可用 --init 自动建表）
 * - 主键与源表一致，才能用 ON DUPLICATE KEY UPDATE 保证幂等
 *
 * 一批消息按表分组，每张表执行一条多值 SQL，避免逐行往返。
 */
final class MysqlSink extends AbstractSink
{
    public function __construct(
        private readonly PDO $pdo,
        private readonly string $tablePrefix = '',
        /** @var array<string,list<string>> 显式指定每张表的目标列；不指定则用消息里的 row 键 */
        private readonly array $columns = [],
        array $tables = [],
        array $ops = [],
    ) {
        parent::__construct($tables, $ops);
    }

    public function name(): string
    {
        return 'mysql:' . $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
    }

    public function write(array $messages): void
    {
        $messages = $this->filter($messages);
        if ($messages === []) {
            return;
        }

        // 按表分组：一次事务，一张表一条 SQL
        /** @var array<string,array{upsert:list<array>,delete:list<array>}> $byTable */
        $byTable = [];
        foreach ($messages as $m) {
            $t = $m->table;
            $byTable[$t] ??= ['upsert' => [], 'delete' => []];
            if (self::isUpsert($m)) {
                $byTable[$t]['upsert'][] = $m->row();
            } else {
                $byTable[$t]['delete'][] = $m->row();
            }
        }

        $this->pdo->beginTransaction();
        try {
            foreach ($byTable as $table => $groups) {
                if ($groups['upsert'] !== []) {
                    $this->upsert($table, $groups['upsert']);
                }
                if ($groups['delete'] !== []) {
                    $this->delete($table, $groups['delete']);
                }
            }
            $this->pdo->commit();
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }
    }

    private function targetTable(string $table): string
    {
        return $this->tablePrefix . $table;
    }

    private function qualifiedTable(string $table): string
    {
        return $this->quote($this->targetTable($table));
    }

    /** @param list<array> $rows */
    private function upsert(string $table, array $rows): void
    {
        $cols = $this->columns[$table] ?? array_keys($rows[0]);
        if ($cols === []) {
            throw new RuntimeException("表 {$table} 无法确定列名（row 为空）");
        }
        // 只保留所有行都有的列，避免 NULL 列缺失导致占位符数量不一致
        $cols = array_values(array_filter($cols, function ($c) use ($rows) {
            foreach ($rows as $r) {
                if (!array_key_exists($c, $r)) {
                    return false;
                }
            }
            return true;
        }));
        if ($cols === []) {
            return;
        }

        $colSql = implode(',', array_map([$this, 'quote'], $cols));
        $updates = implode(',', array_map(
            fn($c) => $this->quote($c) . '=VALUES(' . $this->quote($c) . ')',
            $this->nonKeyColumns($table, $cols)
        ));

        $rowPlaceholder = '(' . implode(',', array_fill(0, count($cols), '?')) . ')';
        $values = [];
        $tuples = [];
        foreach ($rows as $r) {
            $tuples[] = $rowPlaceholder;
            foreach ($cols as $c) {
                $values[] = $this->normalizeValue($r[$c]);
            }
        }

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES %s%s',
            $this->qualifiedTable($table),
            $colSql,
            implode(',', $tuples),
            $updates === '' ? '' : ' ON DUPLICATE KEY UPDATE ' . $updates
        );

        $this->pdo->prepare($sql)->execute($values);
    }

    /** @param list<array> $rows */
    private function delete(string $table, array $rows): void
    {
        $keys = $this->primaryKeys($table);
        if ($keys === []) {
            throw new RuntimeException("表 {$table} 缺少主键信息，无法安全删除");
        }

        // 单主键：DELETE ... WHERE id IN (...)
        if (count($keys) === 1) {
            $k = $keys[0];
            $vals = array_map(fn($r) => $this->normalizeValue($r[$k] ?? null), $rows);
            $placeholders = implode(',', array_fill(0, count($vals), '?'));
            $sql = sprintf(
                'DELETE FROM %s WHERE %s IN (%s)',
                $this->qualifiedTable($table),
                $this->quote($k),
                $placeholders
            );
            $this->pdo->prepare($sql)->execute($vals);
            return;
        }

        // 复合主键：逐条执行，仍在一个事务里
        $where = implode(' AND ', array_map(fn($k) => $this->quote($k) . '=?', $keys));
        $stmt = $this->pdo->prepare(sprintf('DELETE FROM %s WHERE %s', $this->qualifiedTable($table), $where));
        foreach ($rows as $r) {
            $stmt->execute(array_map(fn($k) => $this->normalizeValue($r[$k] ?? null), $keys));
        }
    }

    /** @var array<string,list<string>> 主键缓存 */
    private array $pkCache = [];

    /** @return list<string> */
    private function primaryKeys(string $table): array
    {
        $target = $this->targetTable($table);
        if (isset($this->pkCache[$target])) {
            return $this->pkCache[$target];
        }
        $stmt = $this->pdo->prepare(
            'SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_KEY = ?
             ORDER BY ORDINAL_POSITION'
        );
        $stmt->execute([$target, 'PRI']);
        return $this->pkCache[$target] = $stmt->fetchAll(PDO::FETCH_COLUMN);
    }

    /** @param list<string> $cols @return list<string> */
    private function nonKeyColumns(string $table, array $cols): array
    {
        $pk = $this->primaryKeys($table);
        return array_values(array_diff($cols, $pk));
    }

    private function normalizeValue(mixed $v): mixed
    {
        // 二进制列在 Change 中被包成 ['__binary__' => base64]
        if (is_array($v) && isset($v['__binary__'])) {
            return base64_decode((string)$v['__binary__']);
        }
        if (is_array($v)) {
            return json_encode($v, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }
        if (is_bool($v)) {
            return $v ? 1 : 0;
        }
        return $v;
    }

    private function quote(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }
}
