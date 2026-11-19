<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

/**
 * 事件基类（替代 symfony/event-dispatcher 的 GenericEvent）
 *
 * 解析库的 DTO 继承它，只用到基类本身，不依赖任何方法。
 */
class EventBase
{
}
