<?php
/**
 * 示例 2：批量 upsert 到 MySQL 镜像表
 *
 * 特点：
 * - 连接在循环外建立（常驻进程，不重复连库）
 * - 同一批按表分组，每张表一条多值 SQL
 * - ON DUPLICATE KEY UPDATE 保证重放幂等
 *
 * 环境变量：
 *   TARGET_DSN   默认 mysql:host=127.0.0.1;dbname=mirror
 *   TARGET_USER  默认 root
 *   TARGET_PASS  默认空
 *   TABLE_PREFIX 目标表前缀，默认 mirror_
 *
 * 启用：
 *   new BinlogRelay\Handler\ScriptHandler('/abs/path/to/02-mysql-upsert.php'),
 */

declare(strict_types=1);

$dsn = getenv('TARGET_DSN') ?: 'mysql:host=127.0.0.1;dbname=mirror;charset=utf8mb4';
$user = getenv('TARGET_USER') ?: 'root';
$pass = getenv('TARGET_PASS') ?: '';
$prefix = getenv('TABLE_PREFIX') ?: 'mirror_';

$pdo = new PDO($dsn, $user, $pass, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);

/**
 * @param list<array<string,mixed>> $rows
 */
function upsert(PDO $pdo, string $table, array $rows): void
{
    if ($rows === []) {
        return;
    }
    $cols = array_keys($rows[0]);
    if ($cols === []) {
        return;
    }

    $quote = static fn(string $c): string => '`' . str_replace('`', '``', $c) . '`';
    $colSql = implode(',', array_map($quote, $cols));
    $tuple = '(' . implode(',', array_fill(0, count($cols), '?')) . ')';

    $tuples = [];
    $values = [];
    foreach ($rows as $row) {
        $tuples[] = $tuple;
        foreach ($cols as $c) {
            $v = $row[$c] ?? null;
            // 二进制列在消息里被包成 ['__binary__' => base64]
            if (is_array($v) && isset($v['__binary__'])) {
                $v = base64_decode((string)$v['__binary__']);
            } elseif (is_array($v)) {
                $v = json_encode($v, JSON_UNESCAPED_UNICODE);
            }
            $values[] = $v;
        }
    }

    $updates = implode(',', array_map(
        static fn(string $c): string => $quote($c) . '=VALUES(' . $quote($c) . ')',
        $cols
    ));

    $sql = sprintf(
        'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s',
        $quote($table),
        $colSql,
        implode(',', $tuples),
        $updates
    );
    $pdo->prepare($sql)->execute($values);
}

/**
 * @param list<array<string,mixed>> $rows
 */
function removeRows(PDO $pdo, string $table, array $rows, array $keyCols): void
{
    if ($rows === [] || $keyCols === []) {
        return;
    }
    $quote = static fn(string $c): string => '`' . str_replace('`', '``', $c) . '`';

    if (count($keyCols) === 1) {
        $k = $keyCols[0];
        $vals = array_map(static fn(array $r) => $r[$k] ?? null, $rows);
        $sql = sprintf(
            'DELETE FROM %s WHERE %s IN (%s)',
            $quote($table),
            $quote($k),
            implode(',', array_fill(0, count($vals), '?'))
        );
        $pdo->prepare($sql)->execute($vals);
        return;
    }

    $where = implode(' AND ', array_map(static fn($c) => $quote($c) . '=?', $keyCols));
    $stmt = $pdo->prepare(sprintf('DELETE FROM %s WHERE %s', $quote($table), $where));
    foreach ($rows as $r) {
        $stmt->execute(array_map(static fn($c) => $r[$c] ?? null, $keyCols));
    }
}

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }

    try {
        $changes = json_decode($line, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,array{upsert:list<array>,delete:list<array>,keys:list<string>}> $byTable */
        $byTable = [];
        foreach ($changes as $c) {
            $t = (string)($c['table'] ?? '');
            if ($t === '') {
                continue;
            }
            $byTable[$t] ??= ['upsert' => [], 'delete' => [], 'keys' => $c['table_key'] ?? []];
            if (($c['op'] ?? '') === 'delete') {
                $byTable[$t]['delete'][] = $c['row'] ?? [];
            } else {
                $byTable[$t]['upsert'][] = $c['row'] ?? [];
            }
        }

        $pdo->beginTransaction();
        try {
            foreach ($byTable as $table => $groups) {
                $target = $prefix . $table;
                upsert($pdo, $target, $groups['upsert']);
                removeRows($pdo, $target, $groups['delete'], $groups['keys']);
            }
            $pdo->commit();
        } catch (Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }

        fwrite(STDOUT, json_encode(['ok' => true]) . "\n");
        fflush(STDOUT);
    } catch (Throwable $e) {
        fwrite(STDERR, '[02] ' . $e->getMessage() . PHP_EOL);
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => $e->getMessage()]) . "\n");
        fflush(STDOUT);
    }
}
