<?php

declare(strict_types=1);

namespace BinlogRelay\Console;

use BinlogRelay\Checkpoint\Checkpoint;
use BinlogRelay\Checkpoint\StoreInterface;

/**
 * 点位相关的 CLI 命令（status / reset-position）
 *
 * relay、binlog2redis 两个入口共用，避免实现漂移。
 */
final class CheckpointCommand
{
    public static function status(StoreInterface $store, string $emptyHint = '首次启动将从最新 binlog 末尾开始'): void
    {
        $cp = $store->load();
        echo '存储后端：' . $store->name() . "\n";

        if ($cp === null) {
            echo "尚无点位（{$emptyHint}）\n";
            return;
        }
        // 续传用的是 file+pos；GTID 仅作参考（它是最后一个事务，不是执行集合）
        echo "binlog：{$cp->binlogFile}:{$cp->binlogPos}\n";
        if ($cp->gtid !== '') {
            echo "最后事务 GTID：{$cp->gtid}（仅供参考，不用于续传）\n";
        }
        echo '最后更新：' . ($cp->updatedAt ? date('Y-m-d H:i:s', $cp->updatedAt) : '-') . "\n";
        echo "累计事件：{$cp->events}\n";
        echo "持有者：{$cp->owner}\n";
    }

    /**
     * 重置点位。会先抢锁，避免覆盖正在运行进程写入的点位。
     *
     * @return int 退出码：0 成功，2 缺少 --force，3 有进程在跑
     */
    public static function reset(StoreInterface $store, bool $force): int
    {
        $cp = $store->load();
        if ($cp !== null && !$force) {
            fwrite(STDERR, "点位存在（{$cp->binlogFile}:{$cp->binlogPos}），确认重置请加 --force\n");
            return 2;
        }

        $owner = 'reset:' . getmypid();
        if (!$store->acquireLock($owner, 10)) {
            fwrite(STDERR, "服务正在运行并持有锁，请先停止进程再重置\n");
            return 3;
        }
        $store->save(new Checkpoint());
        $store->releaseLock($owner);
        echo "点位已重置\n";
        return 0;
    }
}
