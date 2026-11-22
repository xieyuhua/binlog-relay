<?php
/**
 * DDL 识别与表名提取测试
 *
 *   php tests/DdlChangeTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Event\DdlChange;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

// --- 类型识别 ---
$cases = [
    'CREATE TABLE `orders` (id INT)'            => DdlChange::CREATE_TABLE,
    'create table orders (id int)'              => DdlChange::CREATE_TABLE,
    'ALTER TABLE orders ADD COLUMN x INT'       => DdlChange::ALTER_TABLE,
    'DROP TABLE IF EXISTS `shop`.`orders`'      => DdlChange::DROP_TABLE,
    'RENAME TABLE a TO b'                       => DdlChange::RENAME_TABLE,
    'TRUNCATE TABLE logs'                       => DdlChange::TRUNCATE_TABLE,
    'CREATE INDEX idx ON orders(id)'            => DdlChange::CREATE_INDEX,
    'CREATE UNIQUE INDEX uk ON orders(code)'    => DdlChange::CREATE_INDEX,
    'DROP INDEX idx ON orders'                  => DdlChange::DROP_INDEX,
    'CREATE DATABASE demo'                      => DdlChange::CREATE_DATABASE,
    'DROP SCHEMA demo'                          => DdlChange::DROP_DATABASE,
    'CREATE VIEW v AS SELECT 1'                 => DdlChange::CREATE_VIEW,
    'CREATE OR REPLACE VIEW v AS SELECT 1'      => DdlChange::CREATE_VIEW,
    'ALTER VIEW v AS SELECT 2'                  => DdlChange::ALTER_VIEW,
    'DROP VIEW IF EXISTS v'                     => DdlChange::DROP_VIEW,
    'CREATE TRIGGER t BEFORE INSERT ON o'       => DdlChange::OTHER,
    '/* comment */ ALTER TABLE t ADD c INT'     => DdlChange::ALTER_TABLE,
];
foreach ($cases as $sql => $want) {
    $got = DdlChange::classify($sql);
    check('识别：' . substr($sql, 0, 34), $got === $want, '得到 ' . var_export($got, true));
}

// --- 非 DDL 应返回 null ---
$nonDdl = ['BEGIN', 'COMMIT', 'ROLLBACK', 'INSERT INTO t VALUES(1)', 'UPDATE t SET a=1', 'DELETE FROM t', ''];
foreach ($nonDdl as $sql) {
    check('非 DDL：' . (substr($sql, 0, 20) ?: '(空)'), DdlChange::classify($sql) === null);
}

// --- 表名提取 ---
$tableCases = [
    'CREATE TABLE `orders` (id INT)'        => ['orders'],
    'DROP TABLE IF EXISTS `shop`.`orders`'  => ['orders'],
    'ALTER TABLE orders ADD COLUMN x INT'   => ['orders'],
    'TRUNCATE TABLE logs'                   => ['logs'],
    'RENAME TABLE a TO b, c TO d'           => ['a', 'b', 'c', 'd'],
    'CREATE TABLE IF NOT EXISTS `t1` (id INT)' => ['t1'],
    'CREATE VIEW v_orders AS SELECT 1'          => ['v_orders'],
    'ALTER VIEW v AS SELECT 2'                  => ['v'],
    'DROP VIEW IF EXISTS v3'                    => ['v3'],
];
foreach ($tableCases as $sql => $want) {
    $got = DdlChange::extractTables($sql);
    sort($got); $sorted = $want; sort($sorted);
    check('表名：' . substr($sql, 0, 34), $got === $sorted, json_encode($got));
}

// 解析不出表名时返回空数组（调用方可退化为整库刷新）
check('CREATE DATABASE 无表名', DdlChange::extractTables('CREATE DATABASE demo') === []);

// --- identity 唯一性 ---
$ddl1 = new DdlChange(DdlChange::CREATE_TABLE, 'demo', ['t'], 'CREATE TABLE t (id INT)', 'bin.1', '100', 1, 1);
$ddl2 = new DdlChange(DdlChange::CREATE_TABLE, 'demo', ['t'], 'CREATE TABLE t (id BIGINT)', 'bin.1', '100', 1, 1);
check('同位置不同语句的 identity 不同', $ddl1->identity() !== $ddl2->identity());
check('相同 DDL 的 identity 稳定', $ddl1->identity() === $ddl1->identity());

// --- toArray 结构 ---
$arr = $ddl1->toArray();
check('toArray 含 type=ddl', ($arr['type'] ?? '') === 'ddl');
check('toArray 含 op/database/tables/query',
    isset($arr['op'], $arr['database'], $arr['tables'], $arr['query']));
check('toArray 可 JSON 序列化', json_encode($arr) !== false);

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
