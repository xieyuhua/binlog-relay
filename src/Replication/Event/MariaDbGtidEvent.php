<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event;

use BinlogRelay\Replication\Event\DTO\MariaDbGtidLogDTO;

class MariaDbGtidEvent extends EventCommon
{
    public function makeMariaDbGTIDLogDTO(): MariaDbGtidLogDTO
    {
        $mariaDbGtid = $this->binaryDataReader->readUInt64();
        $domainId = $this->binaryDataReader->readUInt32();
        $flag = $this->binaryDataReader->readUInt8();

        $this->eventInfo->binLogCurrent
            ->setMariaDbGtid($mariaDbGtid);

        return new MariaDbGtidLogDTO($this->eventInfo, $flag, $domainId, $mariaDbGtid);
    }
}
