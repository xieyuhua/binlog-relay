<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Event\EventBuilder;
use BinlogRelay\Event\Filter;
use BinlogRelay\Handler\HandlerChain;
use BinlogRelay\Support\Logger;
use MySQLReplication\Definitions\ConstEventsNames;
use MySQLReplication\Event\DTO\EventDTO;
use MySQLReplication\Event\DTO\RowsDTO;
use MySQLReplication\Event\EventSubscribers;

/**
 * 事件订阅入口：只处理行变更，其余事件仅用于维护点位
 *
 * 背压策略：
 * - 单个事务/事件内的变更攒到 batch_size 就提交一次 handler
 * - handler 抛异常 → 立即向上抛，进程退出，点位停在上一批
 */
final class Subscriber extends EventSubscribers
{
    private array $buffer = [];
    private int $bufferedBytes = 0;
    private int $bufferedRows = 0;
    private float $lastFlushAt = 0.0;

    private const MAX_BUFFERED_BYTES = 8 * 1024 * 1024;   // 单批内存上限，防止大事务撑爆
    private const AVG_ROW_BYTES = 96;                     // 粗估每行 JSON 体积，只用于背压

    public function __construct(
        private readonly HandlerChain $handlers,
        private readonly Filter $filter,
        private readonly PositionManager $positions,
        private readonly Logger $logger,
        private readonly int $batchSize = 200,
    ) {
        $this->lastFlushAt = microtime(true);
    }

    public static function getSubscribedEvents(): array
    {
        return [
            ConstEventsNames::WRITE->value  => 'onWrite',
            ConstEventsNames::UPDATE->value => 'onUpdate',
            ConstEventsNames::DELETE->value => 'onDelete',
            ConstEventsNames::XID->value    => 'onXID',
            ConstEventsNames::ROTATE->value => 'onRotate',
            ConstEventsNames::GTID->value   => 'onGTID',
        ];
    }

    public function onWrite(\MySQLReplication\Event\DTO\WriteRowsDTO $event): void
    {
        $this->ingest($event);
    }

    public function onUpdate(\MySQLReplication\Event\DTO\UpdateRowsDTO $event): void
    {
        $this->ingest($event);
    }

    public function onDelete(\MySQLReplication\Event\DTO\DeleteRowsDTO $event): void
    {
        $this->ingest($event);
    }

    /** 事务提交：把当前缓冲全部交给 handler，然后推进点位 */
    public function onXID(EventDTO $event): void
    {
        $this->flush();
        $this->positions->track($event->getEventInfo()->binLogCurrent);
    }

    /** binlog 切换文件：点位只在 Rotate 时更新文件名，不需要立即落盘 */
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
        $database = $dto->tableMap->database;
        $table = $dto->tableMap->table;
        if (!$this->filter->allows($database, $table)) {
            return;
        }

        $changes = EventBuilder::fromRowsDTO($dto);
        foreach ($changes as $change) {
            $this->buffer[] = $change;
        }
        // 用行数粗估内存，避免为背压额外做一次 json_encode（实测 1.2µs/事件）
        $this->bufferedRows += count($changes);
        $this->bufferedBytes = $this->bufferedRows * self::AVG_ROW_BYTES;

        if ($this->bufferedRows >= $this->batchSize
            || $this->bufferedBytes >= self::MAX_BUFFERED_BYTES
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
        $this->bufferedBytes = 0;
        $this->bufferedRows = 0;
        $this->lastFlushAt = microtime(true);
        $this->handlers->handle($batch);
    }
}
