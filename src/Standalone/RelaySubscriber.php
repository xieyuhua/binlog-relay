<?php

declare(strict_types=1);

namespace BinlogRelay\Standalone;

use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Event\ColumnProjector;
use BinlogRelay\Event\EventBuilder;
use BinlogRelay\Event\Filter;
use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Replication\Definitions\ConstEventsNames;
use BinlogRelay\Replication\Event\DTO\DeleteRowsDTO;
use BinlogRelay\Replication\Event\DTO\EventDTO;
use BinlogRelay\Replication\Event\DTO\UpdateRowsDTO;
use BinlogRelay\Replication\Event\DTO\WriteRowsDTO;
use BinlogRelay\Replication\Event\EventSubscribers;
use BinlogRelay\Support\Logger;

/**
 * binlog 事件订阅：过滤 → 归一化 → 列裁剪 → 攒批 → 写入
 */
final class RelaySubscriber extends EventSubscribers
{
    private const MAX_ROWS = 2000;

    private array $buffer = [];
    private int $bufferedRows = 0;
    private float $lastFlushAt;

    public function __construct(
        private readonly HandlerInterface $writer,
        private readonly Filter $filter,
        private readonly PositionManager $positions,
        private readonly Logger $logger,
        private readonly int $batchSize = 200,
        private readonly ?ColumnProjector $projector = null,
    ) {
        $this->lastFlushAt = microtime(true);
    }

    public static function getSubscribedEvents(): array
    {
        return [
            // 注意：派发用的名字来自 ConstEventsNames（'write'），不是 ConstEventType（'WRITE_ROWS_EVENT_V2'）
            ConstEventsNames::WRITE->value  => 'onWrite',
            ConstEventsNames::UPDATE->value => 'onUpdate',
            ConstEventsNames::DELETE->value => 'onDelete',
            ConstEventsNames::XID->value    => 'onXID',
            ConstEventsNames::ROTATE->value => 'onRotate',
            ConstEventsNames::GTID->value   => 'onGTID',
        ];
    }

    public function onWrite(WriteRowsDTO $event): void
    {
        $this->ingest($event);
    }

    public function onUpdate(UpdateRowsDTO $event): void
    {
        $this->ingest($event);
    }

    public function onDelete(DeleteRowsDTO $event): void
    {
        $this->ingest($event);
    }

    public function onXID(EventDTO $event): void
    {
        $this->flush();
        $this->positions->track($event->getEventInfo()->binLogCurrent);
    }

    public function onRotate(EventDTO $event): void
    {
        $this->positions->track($event->getEventInfo()->binLogCurrent);
    }

    public function onGTID(EventDTO $event): void
    {
        $this->positions->track($event->getEventInfo()->binLogCurrent);
    }

    private function ingest(WriteRowsDTO|UpdateRowsDTO|DeleteRowsDTO $dto): void
    {
        if (!$this->filter->allows($dto->tableMap->database, $dto->tableMap->table)) {
            return;
        }

        $changes = EventBuilder::fromRowsDTO($dto);
        if ($this->projector !== null) {
            $changes = $this->projector->apply($changes);
        }
        foreach ($changes as $c) {
            $this->buffer[] = $c;
        }
        $this->bufferedRows += count($changes);

        if ($this->bufferedRows >= $this->batchSize
            || $this->bufferedRows >= self::MAX_ROWS
            || microtime(true) - $this->lastFlushAt >= 1.0) {
            $this->flush();
            $this->positions->track($dto->getEventInfo()->binLogCurrent);
        }
    }

    private function flush(): void
    {
        if ($this->buffer === []) {
            return;
        }
        $batch = $this->buffer;
        $this->buffer = [];
        $this->bufferedRows = 0;
        $this->lastFlushAt = microtime(true);
        $this->writer->handle($batch);
    }
}
