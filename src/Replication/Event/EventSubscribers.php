<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event;

use BinlogRelay\Replication\Definitions\ConstEventsNames;
use BinlogRelay\Replication\Event\DTO\DeleteRowsDTO;
use BinlogRelay\Replication\Event\DTO\EventDTO;
use BinlogRelay\Replication\Event\DTO\FormatDescriptionEventDTO;
use BinlogRelay\Replication\Event\DTO\GTIDLogDTO;
use BinlogRelay\Replication\Event\DTO\HeartbeatDTO;
use BinlogRelay\Replication\Event\DTO\MariaDbGtidLogDTO;
use BinlogRelay\Replication\Event\DTO\QueryDTO;
use BinlogRelay\Replication\Event\DTO\RotateDTO;
use BinlogRelay\Replication\Event\DTO\RowsQueryDTO;
use BinlogRelay\Replication\Event\DTO\TableMapDTO;
use BinlogRelay\Replication\Event\DTO\UpdateRowsDTO;
use BinlogRelay\Replication\Event\DTO\WriteRowsDTO;
use BinlogRelay\Replication\Event\DTO\XidDTO;


class EventSubscribers
{
    public static function getSubscribedEvents(): array
    {
        return [
            ConstEventsNames::TABLE_MAP->value => 'onTableMap',
            ConstEventsNames::UPDATE->value => 'onUpdate',
            ConstEventsNames::DELETE->value => 'onDelete',
            ConstEventsNames::GTID->value => 'onGTID',
            ConstEventsNames::QUERY->value => 'onQuery',
            ConstEventsNames::ROTATE->value => 'onRotate',
            ConstEventsNames::XID->value => 'onXID',
            ConstEventsNames::WRITE->value => 'onWrite',
            ConstEventsNames::MARIADB_GTID->value => 'onMariaDbGtid',
            ConstEventsNames::FORMAT_DESCRIPTION->value => 'onFormatDescription',
            ConstEventsNames::HEARTBEAT->value => 'onHeartbeat',
            ConstEventsNames::ROWS_QUERY->value => 'onRowsQuery',
        ];
    }

    public function onUpdate(UpdateRowsDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onTableMap(TableMapDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onDelete(DeleteRowsDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onGTID(GTIDLogDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onQuery(QueryDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onRotate(RotateDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onXID(XidDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onWrite(WriteRowsDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onMariaDbGtid(MariaDbGtidLogDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onFormatDescription(FormatDescriptionEventDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onHeartbeat(HeartbeatDTO $event): void
    {
        $this->allEvents($event);
    }

    public function onRowsQuery(RowsQueryDTO $event): void
    {
        $this->allEvents($event);
    }

    protected function allEvents(EventDTO $event): void
    {
    }
}
