<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event;

use BinlogRelay\Replication\BinaryDataReader\BinaryDataReader;
use BinlogRelay\Replication\BinLog\BinLogServerInfo;

abstract class EventCommon
{
    public function __construct(
        protected EventInfo $eventInfo,
        protected BinaryDataReader $binaryDataReader,
        protected BinLogServerInfo $binLogServerInfo
    ) {
    }
}
