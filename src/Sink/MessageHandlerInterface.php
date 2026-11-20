<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;

/**
 * 直接接收队列消息的自定义 Sink 接口
 *
 * 与 HandlerInterface（接收 Change）互斥，按需二选一：
 * - 需要 Redis entry id 或原始 data 时用本接口
 * - 想复用 relay 侧已有处理器时用 HandlerInterface
 */
interface MessageHandlerInterface
{
    /** @param list<Message> $messages */
    public function handle(array $messages): void;
}
