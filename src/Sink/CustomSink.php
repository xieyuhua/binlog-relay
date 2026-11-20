<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Queue\Message;
use RuntimeException;

/**
 * 自定义 Sink：把消费到的消息交给用户提供的实现
 *
 * 支持两种接口，按需选择：
 *
 *   1. MessageHandlerInterface —— 直接拿到 Message（含 Redis entry id，便于调试）
 *   2. HandlerInterface       —— 收到 Change，可复用 relay 侧的处理器
 *
 * 配置：
 *   'sink' => [
 *       'driver' => 'custom',
 *       'custom' => [
 *           'class' => App\Sink\OrderMirror::class,
 *           // 可选：按顺序传给构造函数
 *           'args'  => ['mysql:host=...', 'user', 'pass'],
 *       ],
 *   ],
 *
 * 示例实现：
 *
 *   final class OrderMirror implements HandlerInterface
 *   {
 *       public function handle(array $changes): void   // Change[]
 *       {
 *           foreach ($changes as $c) { ... }
 *       }
 *   }
 */
final class CustomSink implements SinkInterface
{
    private readonly object $handler;
    private readonly bool $wantsMessages;

    /** @param list<mixed> $args */
    public function __construct(string $class, array $args = [])
    {
        if (!class_exists($class)) {
            throw new RuntimeException("自定义 Sink 类不存在：{$class}");
        }

        $handler = $args === [] ? new $class() : new $class(...$args);

        if ($handler instanceof MessageHandlerInterface) {
            $this->wantsMessages = true;
        } elseif ($handler instanceof HandlerInterface) {
            $this->wantsMessages = false;
        } else {
            throw new RuntimeException(
                "自定义 Sink 需实现 MessageHandlerInterface 或 HandlerInterface：{$class}"
            );
        }

        $this->handler = $handler;
    }

    public function boot(): void
    {
    }

    public function close(): void
    {
    }

    public function name(): string
    {
        return 'custom:' . $this->handler::class;
    }

    public function write(array $messages): void
    {
        if ($messages === []) {
            return;
        }

        if ($this->wantsMessages) {
            $this->handler->handle($messages);
            return;
        }

        // 还原成 Change，让复用 relay 侧逻辑成为可能
        $this->handler->handle(Message::toChanges($messages));
    }
}
