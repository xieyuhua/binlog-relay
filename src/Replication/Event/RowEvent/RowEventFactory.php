<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event\RowEvent;

use BinlogRelay\Replication\BinaryDataReader\BinaryDataReader;
use BinlogRelay\Replication\Event\EventInfo;

readonly class RowEventFactory
{
    public function __construct(
        private RowEventBuilder $rowEventBuilder
    ) {
    }

    public function makeRowEvent(BinaryDataReader $binaryDataReader, EventInfo $eventInfo): RowEvent
    {
        $this->rowEventBuilder->withBinaryDataReader($binaryDataReader);
        $this->rowEventBuilder->withEventInfo($eventInfo);

        return $this->rowEventBuilder->build();
    }
}
