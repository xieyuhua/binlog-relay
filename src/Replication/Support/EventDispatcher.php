<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

/**
 * 轻量事件分发器（替代 symfony/event-dispatcher）
 *
 * 解析库的用法非常固定：按事件名注册 subscriber，dispatch 时把 DTO 传给对应方法。
 * 这里实现同样的语义，不引入 Symfony。
 */
final class EventDispatcher
{
    /** @var array<string, list<array{0:object,1:string}>> */
    private array $listeners = [];

    /**
     * 注册订阅者：读取静态方法 getSubscribedEvents() 返回的 [事件名 => 方法名]
     */
    public function addSubscriber(object $subscriber): void
    {
        if (!method_exists($subscriber, 'getSubscribedEvents')) {
            return;
        }
        foreach ($subscriber::getSubscribedEvents() as $eventName => $method) {
            // 兼容 symfony 中 [方法名, 优先级] 的写法
            if (is_array($method)) {
                $method = $method[0];
            }
            $this->listeners[$eventName][] = [$subscriber, (string)$method];
        }
    }

    public function removeSubscriber(object $subscriber): void
    {
        foreach ($this->listeners as $name => $list) {
            $this->listeners[$name] = array_values(array_filter(
                $list,
                static fn(array $pair) => $pair[0] !== $subscriber
            ));
            if ($this->listeners[$name] === []) {
                unset($this->listeners[$name]);
            }
        }
    }

    public function dispatch(object $event, string $eventName): object
    {
        foreach ($this->listeners[$eventName] ?? [] as [$subscriber, $method]) {
            $subscriber->{$method}($event);
        }
        return $event;
    }
}
