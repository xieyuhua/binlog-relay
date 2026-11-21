<?php
/**
 * 全量同步分页逻辑测试
 *
 * 用 SQLite 内存库验证游标分页不重不漏（不需要 MySQL）。
 * 注意：FullSyncer 用的是 information_schema，这里只测分页算法的等价实现。
 *
 *   php tests/FullSyncTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Event\Change;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

$pdo = new PDO('sqlite::memory:', null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

// ---- 单主键：分页不应重不漏 ----
$pdo->exec('CREATE TABLE t_single (id INTEGER PRIMARY KEY, name TEXT)');
$insert = $pdo->prepare('INSERT INTO t_single (id, name) VALUES (?, ?)');
for ($i = 1; $i <= 25; $i++) {
    $insert->execute([$i, "row{$i}"]);
}

/**
 * 游标分页（与 FullSyncer::fetchPage 的单主键分支等价）
 * @return list<int>
 */
function pageSingle(PDO $pdo, int $batch): array
{
    $seen = [];
    $last = null;
    while (true) {
        if ($last === null) {
            $stmt = $pdo->query("SELECT id FROM t_single ORDER BY id LIMIT {$batch}");
        } else {
            $stmt = $pdo->prepare("SELECT id FROM t_single WHERE id > ? ORDER BY id LIMIT {$batch}");
            $stmt->execute([$last]);
        }
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        if ($rows === []) {
            break;
        }
        foreach ($rows as $id) {
            $seen[] = (int)$id;
        }
        $last = (int)$rows[array_key_last($rows)];
        if (count($rows) < $batch) {
            break;
        }
    }
    return $seen;
}

foreach ([1, 7, 25, 100] as $batch) {
    $ids = pageSingle($pdo, $batch);
    check("单主键 batch={$batch} 行数正确", count($ids) === 25, (string)count($ids));
    check("单主键 batch={$batch} 无重复", count($ids) === count(array_unique($ids)));
    check("单主键 batch={$batch} 覆盖 1..25", $ids === range(1, 25));
}

// ---- 复合主键：游标比较不能漏 ----
$pdo->exec('CREATE TABLE t_multi (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b))');
$ins = $pdo->prepare('INSERT INTO t_multi (a, b, v) VALUES (?,?,?)');
$expected = [];
for ($a = 1; $a <= 4; $a++) {
    for ($b = 1; $b <= 6; $b++) {
        $ins->execute([$a, $b, "v{$a}{$b}"]);
        $expected[] = "{$a},{$b}";
    }
}
sort($expected);

/** 复合主键游标分页（与 FullSyncer::fetchPage 复合分支等价） */
function pageComposite(PDO $pdo, int $batch): array
{
    $seen = [];
    $last = null;
    while (true) {
        if ($last === null) {
            $stmt = $pdo->query("SELECT a, b FROM t_multi ORDER BY a, b LIMIT {$batch}");
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $params = [];
        } else {
            $sql = 'SELECT a, b FROM t_multi WHERE (a > ?) OR (a = ? AND b > ?) ORDER BY a, b LIMIT ' . $batch;
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$last[0], $last[0], $last[1]]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        }
        if ($rows === []) {
            break;
        }
        foreach ($rows as $r) {
            $seen[] = $r['a'] . ',' . $r['b'];
        }
        $lastRow = $rows[array_key_last($rows)];
        $last = [$lastRow['a'], $lastRow['b']];
        if (count($rows) < $batch) {
            break;
        }
    }
    return $seen;
}

foreach ([1, 5, 24, 100] as $batch) {
    $got = pageComposite($pdo, $batch);
    $sorted = $got;
    sort($sorted);
    check("复合主键 batch={$batch} 行数正确", count($got) === 24, (string)count($got));
    check("复合主键 batch={$batch} 无重复", count($got) === count(array_unique($got)));
    check("复合主键 batch={$batch} 覆盖全部", $sorted === $expected);
}

// ---- 快照行转 Change 的结构 ----
$change = new Change(
    Change::INSERT,
    'demo',
    'orders',
    ['id' => 1, 'name' => 'a'],
    null,
    ['id'],
    '',      // 快照没有 binlog 位置
    '0',
    '',
    time(),
    0
);
check('快照 Change 是 insert', $change->op === Change::INSERT);
check('快照 Change 带主键列', $change->tableKey === ['id']);
check('快照 rowIdentity 可用', $change->rowIdentity() === 'demo.orders:1');
check('快照无 before', $change->before === null);

$fields = \BinlogRelay\Queue\Message::fromChange($change);
check('快照消息为扁平结构',
    isset($fields['id'], $fields['kind'], $fields['row']) && !isset($fields['data']));
check('快照消息 op 为 insert', $fields['op'] === 'insert');

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
