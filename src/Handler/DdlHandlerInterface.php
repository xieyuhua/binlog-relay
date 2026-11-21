<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\DdlChange;

/**
 * 可选接口：需要接收 DDL 事件的处理器实现它
 *
 * 为什么单独定义而不是加到 HandlerInterface：
 *   DDL 是低频事件（建表/改表不常见），大多数处理器只关心行变更。
 *   放到主接口会强迫所有实现写一个空方法。用独立接口可选择性实现，
 *   且完全不破坏已有的 HandlerInterface 实现。
 *
 * 启用方式：
 *   relay 配置里 'filter.emit_ddl' => true，同时处理器实现本接口。
 */
interface DdlHandlerInterface
{
    /**
     * 处理一条 DDL 变更
     *
     * 与 handle() 的约定一致：抛异常会中断 relay，点位停在上一批。
     */
    public function handleDdl(DdlChange $ddl): void;
}
