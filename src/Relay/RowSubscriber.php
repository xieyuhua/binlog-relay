<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Event\ColumnProjector;
use BinlogRelay\Event\DdlChange;
use BinlogRelay\Event\EventBuilder;
use BinlogRelay\Event\Filter;
use BinlogRelay\Handler\DdlHandlerInterface;
use BinlogRelay\Handler\HandlerChain;
use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Replication\Definitions\ConstEventsNames;
use BinlogRelay\Replication\Event\DTO\DeleteRowsDTO;
use BinlogRelay\Replication\Event\DTO\EventDTO;
use BinlogRelay\Replication\Event\DTO\QueryDTO;
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
        /** 是否把 DDL 事件交给 handler（需要额外订阅 QUERY 事件） */
        protected readonly bool $emitDdl = false,
    ) {
        $this->lastFlushAt = microtime(true);
    }

    public static function getSubscribedEvents(): array
    {
        // 注意：事件名来自 ConstEventsNames（'write'），不是 ConstEventType（'WRITE_ROWS_EVENT_V2'）
        $events = [
            ConstEventsNames::WRITE->value  => 'onWrite',
            ConstEventsNames::UPDATE->value => 'onUpdate',
            ConstEventsNames::DELETE->value => 'onDelete',
            ConstEventsNames::XID->value    => 'onXID',
            ConstEventsNames::ROTATE->value => 'onRotate',
            ConstEventsNames::GTID->value   => 'onGTID',
        ];

        // DDL 需要 QUERY 事件；订阅它是可选的，因为大多数 QUERY 是 BEGIN/COMMIT，
        // 解析它们有成本，未开启时不订阅
        if (self::$emitDdlStatic) {
            $events[ConstEventsNames::QUERY->value] = 'onQuery';
        }

        return $events;
    }

    /** 静态开关：getSubscribedEvents() 是静态方法，无法读实例属性 */
    private static bool $emitDdlStatic = false;

    /** 在注册订阅者之前调用，决定是否订阅 QUERY 事件 */
    public static function enableDdl(bool $enabled = true): void
    {
        self::$emitDdlStatic = $enabled;
    }

    /**
     * QUERY 事件：挑出 DDL 交给 handler
     *
     * 非 DDL 语句（BEGIN/COMMIT/INSERT 等）直接忽略。
     */
    public function onQuery(QueryDTO $event): void
    {
        if (!$this->emitDdl) {
            return;
        }

        $op = DdlChange::classify($event->query);
        if ($op === null) {
            return;   // 不是 DDL
        }

        $info = $event->getEventInfo();
        $current = $info->binLogCurrent;

        $ddl = new DdlChange(
            $op,
            (string)$event->database,
            DdlChange::extractTables($event->query),
            $event->query,
            $this->safeStr($current, 'getBinFileName'),
            $this->safeStr($current, 'getBinLogPosition', '0'),
            (int)$info->timestamp,
            (int)$info->serverId
        );

        // DDL 单独分发：只有实现 DdlHandlerInterface 的处理器会收到
        if ($this->handler instanceof HandlerChain) {
            $this->handler->handleDdl($ddl);
        } elseif ($this->handler instanceof DdlHandlerInterface) {
            $this->handler->handleDdl($ddl);
        }
        $this->positions->track($current);
    }

    private function safeStr(object $obj, string $method, string $default = ''): string
    {
        try {
            return (string)$obj->{$method}();
        } catch (\Throwable) {
            return $default;
        }
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

        // 列裁剪在构造 Change 时就完成（比事后 apply() 重建对象快约 48%）
        $changes = EventBuilder::fromRowsDTO($dto, $this->projector);

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
