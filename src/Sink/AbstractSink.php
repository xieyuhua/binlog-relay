<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;

/**
 * Sink 公共能力：表名过滤、事件类型过滤
 */
abstract class AbstractSink implements SinkInterface
{
    /**
     * @param list<string> $tables 为空表示全部
     * @param list<string> $ops    为空表示全部（insert/update/delete）
     */
    public function __construct(
        protected readonly array $tables = [],
        protected readonly array $ops = [],
    ) {
    }

    public function boot(): void
    {
    }

    public function close(): void
    {
    }

    public function name(): string
    {
        return static::class;
    }

    protected function accepts(Message $m): bool
    {
        if ($this->tables !== [] && !in_array($m->table, $this->tables, true)) {
            return false;
        }
        if ($this->ops !== [] && !in_array($m->op, $this->ops, true)) {
            return false;
        }
        return true;
    }

    /**
     * @param list<Message> $messages
     * @return list<Message>
     */
    protected function filter(array $messages): array
    {
        if ($this->tables === [] && $this->ops === []) {
            return $messages;
        }
        return array_values(array_filter($messages, fn(Message $m) => $this->accepts($m)));
    }

    /** 便捷方法：判断是否为 insert/update（需要写目标表） */
    protected static function isUpsert(Message $m): bool
    {
        return $m->op === 'insert' || $m->op === 'update';
    }
}
