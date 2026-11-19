<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event;

use BinlogRelay\Replication\Event\DTO\XidDTO;

/**
 * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_xid
 */
class XidEvent extends EventCommon
{
    public function makeXidDTO(): XidDTO
    {
        return new XidDTO($this->eventInfo, (string)$this->binaryDataReader->readUInt64());
    }
}
