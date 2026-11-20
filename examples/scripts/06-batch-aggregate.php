<?php
/**
 * 示例 6：攒批聚合后一次性写库（高吞吐）
 *
 * 与 02 的区别：这里在脚本内部再攒一层缓冲，把多个 binlog 批次合并成
 * 一次更大的写入。适合下游是高延迟服务（跨机房 DB、ClickHouse 等）。
 *
 * 权衡：
 *   - 吞吐提升明显（减少往返次数）
 *   - 但会引入额外延迟（最多 BUFFER_MS 毫秒）
 *   - 进程被 kill 时缓冲区里的数据会丢，需要靠重放补偿
 *
 * 环境变量：
 *   FLUSH_ROWS   攒够多少行就刷，默认 500
 *   FLUSH_MS     最多攒多久（毫秒），默认 1000
 *   TARGET_DSN / TARGET_USER / TARGET_PASS  同示例 2
 *
 * 启用：
 *   new BinlogRelay\Handler\ScriptHandler('/abs/path/to/06-batch-aggregate.php'),
 */

declare(strict_types=1);

$flushRows = (int)(getenv('FLUSH_ROWS') ?: 500);
$flushMs = (int)(getenv('FLUSH_MS') ?: 1000);

$dsn = getenv('TARGET_DSN') ?: 'mysql:host=127.0.0.1;dbname=mirror;charset=utf8mb4';
$pdo = new PDO($dsn, getenv('TARGET_USER') ?: 'root', getenv('TARGET_PASS') ?: '', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);

/** @var list<array<string,mixed>> $buffer */
$buffer = [];
$lastFlush = microtime(true);

function flushBuffer(PDO $pdo, array &$buffer): void
{
    if ($buffer === []) {
        return;
    }

    /** @var array<string,array{upsert:list<array>,delete:list<array>,keys:list<string>}> $byTable */
    $byTable = [];
    foreach ($buffer as $c) {
        $t = (string)$c['table'];
        $byTable[$t] ??= ['upsert' => [], 'delete' => [], 'keys' => $c['table_key'] ?? []];
        if ($c['op'] === 'delete') {
            $byTable[$t]['delete'][] = $c['row'];
        } else {
            $byTable[$t]['upsert'][] = $c['row'];
        }
    }

    $quote = static fn(string $c): string => '`' . str_replace('`', '``', $c) . '`';

    $pdo->beginTransaction();
    try {
        foreach ($byTable as $table => $groups) {
            $target = $quote('mirror_' . $table);

            if ($groups['upsert'] !== []) {
                $cols = array_keys($groups['upsert'][0]);
                $colSql = implode(',', array_map($quote, $cols));
                $tuple = '(' . implode(',', array_fill(0, count($cols), '?')) . ')';
                $tuples = [];
                $values = [];
                foreach ($groups['upsert'] as $row) {
                    $tuples[] = $tuple;
                    foreach ($cols as $c) {
                        $v = $row[$c] ?? null;
                        $values[] = is_array($v)
                            ? (isset($v['__binary__']) ? base64_decode((string)$v['__binary__']) : json_encode($v, JSON_UNESCAPED_UNICODE))
                            : $v;
                    }
                }
                $pdo->prepare(sprintf(
                    'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s',
                    $target,
                    $colSql,
                    implode(',', $tuples),
                    implode(',', array_map(static fn($c) => $quote($c) . '=VALUES(' . $quote($c) . ')', $cols))
                ))->execute($values);
            }

            if ($groups['delete'] !== [] && $groups['keys'] !== []) {
                $keys = $groups['keys'];
                if (count($keys) === 1) {
                    $k = $keys[0];
                    $vals = array_map(static fn($r) => $r[$k] ?? null, $groups['delete']);
                    $pdo->prepare(sprintf(
                        'DELETE FROM %s WHERE %s IN (%s)',
                        $target,
                        $quote($k),
                        implode(',', array_fill(0, count($vals), '?'))
                    ))->execute($vals);
                }
            }
        }
        $pdo->commit();
    } catch (Throwable $e) {
        $pdo->rollBack();
        throw $e;
    }

    $buffer = [];
}

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }

    try {
        $changes = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        foreach ($changes as $c) {
            if (!empty($c['table'])) {
                $buffer[] = $c;
            }
        }

        $elapsedMs = (microtime(true) - $lastFlush) * 1000;
        if (count($buffer) >= $flushRows || $elapsedMs >= $flushMs) {
            flushBuffer($pdo, $buffer);
            $lastFlush = microtime(true);
        }

        // 注意：即使还没真正写库也回 ok，因为数据已在脚本缓冲里。
        // 若要求「回 ok 之前必须落盘」，把 flushBuffer 移到回包之前。
        fwrite(STDOUT, json_encode(['ok' => true]) . "\n");
        fflush(STDOUT);
    } catch (Throwable $e) {
        $buffer = [];
        fwrite(STDERR, '[06] ' . $e->getMessage() . PHP_EOL);
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => $e->getMessage()]) . "\n");
        fflush(STDOUT);
    }
}

// 父进程退出：刷完剩余数据
try {
    flushBuffer($pdo, $buffer);
} catch (Throwable $e) {
    fwrite(STDERR, '[06] 退出前刷新失败：' . $e->getMessage() . PHP_EOL);
}
