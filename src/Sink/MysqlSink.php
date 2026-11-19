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

    /**
     * 删除目标行
     *
     * 键的优先级与 EventBuilder 一致：主键 → 唯一键 → 整行匹配。
     * 无键表用整行匹配（WHERE col1=? AND col2=? ...），虽然慢，
     * 但比"报错让整批进死信"或"删错行"安全。
     *
     * @param list<array> $rows
     */
    private function delete(string $table, array $rows): void
    {
        $keys = $this->identityKeys($table);

        // 无键：按整行内容匹配
        if ($keys === []) {
            $cols = array_keys($rows[0]);
            if ($cols === []) {
                throw new RuntimeException("表 {$table} 删除事件缺少行数据，无法定位行");
            }
            $where = implode(' AND ', array_map(fn($c) => $this->quote($c) . '<=>?', $cols));
            $stmt = $this->pdo->prepare(sprintf('DELETE FROM %s WHERE %s LIMIT 1', $this->qualifiedTable($table), $where));
            foreach ($rows as $r) {
                // <=> 做 NULL 安全比较；LIMIT 1 避免误删同内容的多行
                $stmt->execute(array_map(fn($c) => $this->normalizeValue($r[$c] ?? null), $cols));
            }
            return;
        }

        // 单键：DELETE ... WHERE id IN (...)
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

        // 复合键：逐条执行，仍在一个事务里
        $where = implode(' AND ', array_map(fn($k) => $this->quote($k) . '=?', $keys));
        $stmt = $this->pdo->prepare(sprintf('DELETE FROM %s WHERE %s', $this->qualifiedTable($table), $where));
        foreach ($rows as $r) {
            $stmt->execute(array_map(fn($k) => $this->normalizeValue($r[$k] ?? null), $keys));
        }
    }

    /** @var array<string,list<string>> 定位键缓存 */
    private array $pkCache = [];

    /**
     * 取目标表的定位键：主键优先，其次唯一键，都没有返回空数组
     *
     * @return list<string>
     */
    private function identityKeys(string $table): array
    {
        $target = $this->targetTable($table);
        if (isset($this->pkCache[$target])) {
            return $this->pkCache[$target];
        }

        $stmt = $this->pdo->prepare(
            'SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_KEY IN (?, ?)
             ORDER BY ORDINAL_POSITION'
        );
        $stmt->execute([$target, 'PRI', 'UNI']);

        $primary = [];
        $unique = [];
        foreach ($stmt->fetchAll() as $row) {
            if ($row['COLUMN_KEY'] === 'PRI') {
                $primary[] = $row['COLUMN_NAME'];
            } else {
                $unique[] = $row['COLUMN_NAME'];
            }
        }

        return $this->pkCache[$target] = $primary !== [] ? $primary : $unique;
    }

    /** @param list<string> $cols @return list<string> */
    private function nonKeyColumns(string $table, array $cols): array
    {
        $pk = $this->identityKeys($table);
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
