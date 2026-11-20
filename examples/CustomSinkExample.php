<?php
/**
 * 自定义 Sink / Handler 示例
 *
 * 同一个类既能当 relay 的 handler，也能当 consumer 的 sink，
 * 因为它实现的是 HandlerInterface（接收 Change）。
 *
 * 用法一：作为 relay 的 handler（config/relay.php）
 *   'handlers' => [App\CustomSinkExample::class],
 *
 * 用法二：作为 consumer 的 sink（config/consumer.php）
 *   'sink' => [
 *       'driver' => 'custom',
 *       'custom' => ['class' => App\CustomSinkExample::class],
 *   ],
 */

declare(strict_types=1);

namespace App;

use BinlogRelay\Event\Change;
use BinlogRelay\Handler\HandlerInterface;

final class CustomSinkExample implements HandlerInterface
{
    private ?\PDO $pdo = null;
    private int $total = 0;

    /**
     * 构造参数由配置的 args 注入，例如：
     *   'args' => ['mysql:host=127.0.0.1;dbname=mirror', 'root', 'pass']
     */
    public function __construct(
        private readonly string $dsn = 'mysql:host=127.0.0.1;dbname=mirror;charset=utf8mb4',
        private readonly string $user = 'root',
        private readonly string $pass = '',
        private readonly string $prefix = 'mirror_',
        private readonly array $tables = ['orders'],
    ) {
    }

    /** @param list<Change> $changes */
    public function handle(array $changes): void
    {
        if ($changes === []) {
            return;
        }

        // 延迟建连接：作为 relay handler 时进程常驻，只连一次
        $this->pdo ??= new \PDO($this->dsn, $this->user, $this->pass, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_EMULATE_PREPARES => false,
        ]);

        /** @var array<string,array{upsert:list<array>,delete:list<array>,keys:list<string>}> $byTable */
        $byTable = [];
        foreach ($changes as $c) {
            if (!in_array($c->table, $this->tables, true)) {
                continue;
            }
            $byTable[$c->table] ??= ['upsert' => [], 'delete' => [], 'keys' => $c->tableKey];
            if ($c->op === Change::DELETE) {
                $byTable[$c->table]['delete'][] = $c->row;
            } else {
                $byTable[$c->table]['upsert'][] = $c->row;
            }
        }

        if ($byTable === []) {
            return;
        }

        $quote = static fn(string $s): string => '`' . str_replace('`', '``', $s) . '`';

        $this->pdo->beginTransaction();
        try {
            foreach ($byTable as $table => $groups) {
                $target = $quote($this->prefix . $table);

                if ($groups['upsert'] !== []) {
                    $cols = array_keys($groups['upsert'][0]);
                    $colSql = implode(',', array_map($quote, $cols));
                    $tuple = '(' . implode(',', array_fill(0, count($cols), '?')) . ')';
                    $tuples = [];
                    $values = [];
                    foreach ($groups['upsert'] as $row) {
                        $tuples[] = $tuple;
                        foreach ($cols as $c) {
                            $values[] = self::normalize($row[$c] ?? null);
                        }
                    }
                    $this->pdo->prepare(sprintf(
                        'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s',
                        $target,
                        $colSql,
                        implode(',', $tuples),
                        implode(',', array_map(
                            static fn($c) => $quote($c) . '=VALUES(' . $quote($c) . ')',
                            array_diff($cols, $groups['keys'])
                        ))
                    ))->execute($values);
                }

                if ($groups['delete'] !== [] && $groups['keys'] !== []) {
                    $keys = $groups['keys'];
                    if (count($keys) === 1) {
                        $k = $keys[0];
                        $vals = array_map(static fn($r) => self::normalize($r[$k] ?? null), $groups['delete']);
                        $this->pdo->prepare(sprintf(
                            'DELETE FROM %s WHERE %s IN (%s)',
                            $target,
                            $quote($k),
                            implode(',', array_fill(0, count($vals), '?'))
                        ))->execute($vals);
                    }
                }
            }
            $this->pdo->commit();
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw $e;
        }

        $this->total += count($changes);
    }

    /** 已处理的变更数，便于测试断言 */
    public function total(): int
    {
        return $this->total;
    }

    private static function normalize(mixed $v): mixed
    {
        if (is_array($v) && isset($v['__binary__'])) {
            return base64_decode((string)$v['__binary__']);
        }
        if (is_array($v)) {
            return json_encode($v, JSON_UNESCAPED_UNICODE);
        }
        return $v;
    }
}
