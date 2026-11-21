<?php
/**
 * GTID 集合测试
 *
 *   php tests/GtidSetTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Checkpoint\GtidSet;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

$U = 'c8fbcbb8-a012-11ef-85be-525400bcd3ec';
$V = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';

// 1. 逐个添加应合并成区间
$s = new GtidSet();
foreach ([1, 2, 3, 5, 6, 10] as $n) {
    $s->addGtid("{$U}:{$n}");
}
check('连续序号合并为区间', $s->toString() === "{$U}:1-3:5-6:10", $s->toString());
check('事务总数正确', $s->count() === 6, (string)$s->count());

// 2. 乱序添加也能正确合并
$s = new GtidSet();
foreach ([5, 1, 3, 2] as $n) {
    $s->addGtid("{$U}:{$n}");
}
check('乱序添加后合并为 1-3:5', $s->toString() === "{$U}:1-3:5", $s->toString());

// 3. 跨段合并
$s = GtidSet::parse("{$U}:1-5:10,{$U}:6-9");
check('解析时跨段合并为 1-10', $s->toString() === "{$U}:1-10", $s->toString());

// 4. 相邻区间合并（6 紧接 5）
$s = GtidSet::parse("{$U}:1-5");
$s->addGtid("{$U}:6");
check('相邻区间合并为 1-6', $s->toString() === "{$U}:1-6", $s->toString());

// 5. 多 SID
$s = new GtidSet();
$s->addGtid("{$U}:1");
$s->addInterval($V, 1, 3);
check('多 SID 都保留', $s->sidCount() === 2 && $s->count() === 4);
check('多 SID 序列化含两个', substr_count($s->toString(), ',') === 1);

// 6. contains
$s = GtidSet::parse("{$U}:1-10");
check('contains 命中区间内', $s->contains("{$U}:7"));
check('contains 未命中区间外', !$s->contains("{$U}:11"));
check('contains 未命中其它 SID', !$s->contains("{$V}:7"));

// 7. 空集合
check('空串解析为空集合', GtidSet::parse('')->isEmpty());
check('空集合序列化为空串', GtidSet::parse('')->toString() === '');
check('空串带空白也能解析', GtidSet::parse("  \n ")->isEmpty());

// 8. 非法输入
foreach (['not-a-gtid', 'zzz:1-2', ''] as $bad) {
    try {
        GtidSet::parse($bad);
        $ok = ($bad === '');   // 空串合法
    } catch (InvalidArgumentException) {
        $ok = ($bad !== '');
    }
    check('非法输入处理：' . substr($bad, 0, 12), $ok);
}

// 9. round-trip（输出按 SID 字典序，$V 在前）
$original = "{$V}:1-100,{$U}:1-3:8:10-12";
check('round-trip 保持一致', GtidSet::parse($original)->toString() === $original,
    GtidSet::parse($original)->toString());

// 10. merge
$a = GtidSet::parse("{$U}:1-5");
$b = GtidSet::parse("{$U}:4-10,{$V}:1");
$a->merge($b);
check('merge 后区间合并', $a->toString() === "{$V}:1,{$U}:1-10", $a->toString());

// 11. 单事务边界
$s = new GtidSet();
$s->addGtid("{$U}:1");
check('单个事务不加短横线', $s->toString() === "{$U}:1", $s->toString());

// 12. 大小写归一化
$s = GtidSet::parse(strtoupper($U) . ':1');
check('SID 统一小写', $s->toString() === "{$U}:1", $s->toString());

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
