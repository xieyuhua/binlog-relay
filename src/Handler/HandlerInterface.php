<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;

/**
 * 业务处理器：接收一批变更（同一 binlog 事务内可能多行）
 *
 * 约定：
 * - handle() 返回即视为该批已处理完成，框架随后推进点位
 * - 抛异常会中断当前 relay（默认），避免静默丢数据
 * - 必须幂等：崩溃后最多会重放 since-last-checkpoint 的事件
 */
interface HandlerInterface
{
    /** @param list<Change> $changes */
    public function handle(array $changes): void;
}
