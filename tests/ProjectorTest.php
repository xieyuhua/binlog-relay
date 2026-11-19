<?php
/**
 * 列裁剪测试
 *
 *   php tests/ProjectorTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use BinlogRelay\Event\Change;
use BinlogRelay\Event\ColumnProjector;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

function makeChange(string $table, array $row, ?array $before = null, array $key = ['id']): Change
{
    return new Change(Change::UPDATE, 'shop', $table, $row, $before, $key, 'bin.1', '100', '', 1, 1);
}

$row = ['id' => 1, 'status' => 'paid', 'content' => str_repeat('x', 1000), 'meta' => ['a' => 1]];

// 1. 只保留配置列
$p = ColumnProjector::fromConfig(['orders' => ['id', 'status']]);
$out = $p->apply([makeChange('orders', $row)])[0];
check('只保留配置的列', array_keys($out->row) === ['id', 'status'], json_encode(array_keys($out->row)));

// 2. 主键自动保留
$p = ColumnProjector::fromConfig(['orders' => ['status']]);
$out = $p->apply([makeChange('orders', $row)])[0];
check('未写进配置的主键自动保留', $out->row === ['id' => 1, 'status' => 'paid'], json_encode($out->row));

// 3. before 同样裁剪
$out = $p->apply([makeChange('orders', $row, ['id' => 1, 'status' => 'new', 'content' => 'old'])])[0];
check('before 同样裁剪', array_keys($out->before) === ['id', 'status'], json_encode(array_keys($out->before)));

// 4. 未配置的表原样
$p = ColumnProjector::fromConfig(['orders' => ['id']]);
$out = $p->apply([makeChange('users', $row)])[0];
check('未配置的表不裁剪', count($out->row) === 4);

// 5. 通配规则
$p = ColumnProjector::fromConfig(['orders' => ['id', 'status'], '*' => ['id']]);
$outOrders = $p->apply([makeChange('orders', $row)])[0];
$outUsers = $p->apply([makeChange('users', $row)])[0];
check('显式配置优先于通配', array_keys($outOrders->row) === ['id', 'status']);
check('通配规则作用于其它表', array_keys($outUsers->row) === ['id']);

// 6. 空配置零开销（返回同一对象引用）
$p = ColumnProjector::fromConfig([]);
$changes = [makeChange('orders', $row)];
check('空配置直接返回原数组', $p->isEmpty() && $p->apply($changes) === $changes);

// 7. 全部保留时不重建数组
$p = ColumnProjector::fromConfig(['orders' => ['id', 'status', 'content', 'meta']]);
$out = $p->apply([makeChange('orders', $row)])[0];
check('全保留语义不变', $out->row === $row);

// 8. 体积显著下降
$bigRow = ['id' => 1, 'status' => 'paid', 'content' => str_repeat('x', 50000)];
$before = strlen(json_encode(makeChange('cms', $bigRow)->toArray()));
$p = ColumnProjector::fromConfig(['cms' => ['id', 'status']]);
$after = strlen(json_encode($p->apply([makeChange('cms', $bigRow)])[0]->toArray()));
$ratio = $after / max(1, $before);
check('大字段裁剪后体积下降 >90%', $ratio < 0.1, sprintf('%.1f%%', $ratio * 100));

// 9. 其它字段（op/pos/tableKey）不受影响
$p = ColumnProjector::fromConfig(['orders' => ['id']]);
$out = $p->apply([makeChange('orders', $row)])[0];
check('op/pos/tableKey 保持不变',
    $out->op === 'update' && $out->binlogPos === '100' && $out->tableKey === ['id']);

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
