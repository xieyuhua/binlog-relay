<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Event\RowEvent;

use BinlogRelay\Replication\BinaryDataReader\BinaryDataReader;
use BinlogRelay\Replication\BinLog\BinLogServerInfo;
use BinlogRelay\Replication\Config\Config;
use BinlogRelay\Replication\Event\EventInfo;
use BinlogRelay\Replication\Repository\RepositoryInterface;
use BinlogRelay\Replication\Support\LoggerInterface;
use BinlogRelay\Replication\Support\CacheInterface;

class RowEventBuilder
{
    private BinaryDataReader $binaryDataReader;
    private EventInfo $eventInfo;

    public function __construct(
        private readonly RepositoryInterface $repository,
        private readonly CacheInterface $cache,
        private readonly Config $config,
        private readonly BinLogServerInfo $binLogServerInfo,
        private readonly LoggerInterface $logger
    ) {
    }

    public function withBinaryDataReader(BinaryDataReader $binaryDataReader): void
    {
        $this->binaryDataReader = $binaryDataReader;
    }

    public function build(): RowEvent
    {
        return new RowEvent(
            $this->repository,
            $this->binaryDataReader,
            $this->eventInfo,
            new TableMapCache($this->cache),
            $this->config,
            $this->binLogServerInfo,
            $this->logger
        );
    }

    public function withEventInfo(EventInfo $eventInfo): void
    {
        $this->eventInfo = $eventInfo;
    }
}
