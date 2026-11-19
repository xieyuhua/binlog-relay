<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event\DTO;

use JsonSerializable;
use BinlogRelay\Replication\Event\EventInfo;
use BinlogRelay\Replication\Support\EventBase;

/**
 * @see https://dev.mysql.com/doc/internals/en/event-meanings.html
 */
abstract class EventDTO extends EventBase implements JsonSerializable
{
    public function __construct(
        protected EventInfo $eventInfo
    ) {
        // 内置 EventBase 无构造参数，无需调用父类构造函数
    }

    abstract public function __toString(): string;

    public function getEventInfo(): EventInfo
    {
        return $this->eventInfo;
    }

    abstract public function getType(): string;
}
