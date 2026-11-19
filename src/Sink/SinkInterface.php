<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;

/**
 * 下游写入器
 *
 * 契约：
 * - write() 收到一批消息，全部写入成功后正常返回
 * - 抛异常表示整批失败，消费端会保持 pending 并在稍后重试
 * - 实现应尽量批量写，这是吞吐的关键
 */
interface SinkInterface
{
    /** @param list<Message> $messages */
    public function write(array $messages): void;

    /** 启动时预热连接、建表等 */
    public function boot(): void;

    /** 优雅退出前 flush 缓冲、关闭连接 */
    public function close(): void;

    public function name(): string;
}
