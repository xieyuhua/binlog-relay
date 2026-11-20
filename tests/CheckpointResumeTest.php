<?php
/**
 * 点位恢复策略测试
 *
 * 覆盖真实故障：点位里同时有 binlogFile 和 gtid 时，
 * 必须用 file+pos 恢复，不能拿单事务 GTID 去 COM_BINLOG_DUMP_GTID。
 *
 *   php tests/CheckpointResumeTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Checkpoint\Checkpoint;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

// 故障现场：file+pos 和单事务 gtid 同时存在
$cp = new Checkpoint(
    'mysql-bin.000028',
    '2639136',
    'c8fbcbb8-a012-11ef-85be-525400bcd3ec:2901'
);
check('有 binlog 文件时 hasFilePosition 为 true', $cp->hasFilePosition());
check('有文件时不走 GTID（这是原故障点）', !$cp->shouldResumeByGtid());
check('describe 展示 file:pos', $cp->binlogFile . ':' . $cp->binlogPos === 'mysql-bin.000028:2639136');

// 只有 GTID（文件被清理等极端情况）
$gtidOnly = new Checkpoint('', '0', 'uuid:1-100');
check('无文件时不走 file 分支', !$gtidOnly->hasFilePosition());
check('无文件且有 gtid 才回退 GTID', $gtidOnly->shouldResumeByGtid());

// 全新点位
$empty = new Checkpoint();
check('空点位两者都为 false', !$empty->hasFilePosition() && !$empty->shouldResumeByGtid());

// 序列化往返
$round = Checkpoint::fromArray($cp->toArray());
check('toArray/fromArray 往返一致',
    $round->binlogFile === $cp->binlogFile
    && $round->binlogPos === $cp->binlogPos
    && $round->gtid === $cp->gtid);
check('往返后恢复策略不变', !$round->shouldResumeByGtid());

// advance 保留 gtid 且更新位置
$moved = $cp->advance('mysql-bin.000029', '4');
check('advance 更新文件与位置', $moved->binlogFile === 'mysql-bin.000029' && $moved->binlogPos === '4');
check('advance 保留 gtid 作参考', $moved->gtid === $cp->gtid);
check('advance 后仍走 file+pos', !$moved->shouldResumeByGtid());

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
