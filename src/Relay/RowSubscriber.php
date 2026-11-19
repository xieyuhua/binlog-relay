<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

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
use BinlogRelay\Replication\Event\DTO\RowsDTO;
use BinlogRelay\Replication\Event\EventSubscribers;
use BinlogRelay\Support\Logger;

/**
 * binlog 行事件订阅：过滤 → 归一化 → 列裁剪 → 攒批 → 交给 handler
 *
 * 生产端（Relay）与单体（binlog2redis）共用这一份实现，
 * 差异只在注入的 handler 是 HandlerChain 还是单个 StreamWriter。
 *
 * 语义：
 * - 行变更先进缓冲，攒够 batchSize 行或超过 1 秒就提交
 * - 事务结束（XID）必定提交，保证一个事务不会被无限延后
 * - handler 抛异常直接向上抛，进程退出，点位停在上一批
 */
class RowSubscriber extends EventSubscribers
{
    /** 缓冲行数上限，防止单事务过大撑爆内存 */
    private const MAX_BUFFERED_ROWS = 20000;

    /** @var list<\BinlogRelay\Event\Change> */
    private array $buffer = [];
    private int $bufferedRows = 0;
    private float $lastFlushAt;

    public function __construct(
        protected readonly HandlerInterface $handler,
        protected readonly Filter $filter,
        protected readonly PositionManager $positions,
        protected readonly Logger $logger,
        protected readonly int $batchSize = 200,
        protected readonly ?ColumnProjector $projector = null,
    ) {
        $this->lastFlushAt = microtime(true);
    }

    public static function getSubscribedEvents(): array
    {
        // 注意：事件名来自 ConstEventsNames（'write'），不是 ConstEventType（'WRITE_ROWS_EVENT_V2'）
        return [
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

    /** 事务提交：把缓冲全部交给 handler，然后推进点位 */
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

    private function ingest(RowsDTO $dto): void
    {
        $tableMap = $dto->tableMap;
        if (!$this->filter->allows($tableMap->database, $tableMap->table)) {
            return;
        }

        $changes = EventBuilder::fromRowsDTO($dto);
        if ($this->projector !== null) {
            $changes = $this->projector->apply($changes);
        }

        foreach ($changes as $change) {
            $this->buffer[] = $change;
        }
        $this->bufferedRows += count($changes);

        if ($this->bufferedRows >= $this->batchSize
            || $this->bufferedRows >= self::MAX_BUFFERED_ROWS
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
        $this->handler->handle($batch);
    }
}
