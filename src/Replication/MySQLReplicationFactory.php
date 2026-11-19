<?php

declare(strict_types=1);

namespace BinlogRelay\Replication;

use BinlogRelay\Replication\Support\PdoConnection;

use BinlogRelay\Replication\BinLog\BinLogServerInfo;
use BinlogRelay\Replication\BinLog\BinLogSocketConnect;
use BinlogRelay\Replication\Cache\ArrayCache;
use BinlogRelay\Replication\Config\Config;
use BinlogRelay\Replication\Event\Event;
use BinlogRelay\Replication\Event\RowEvent\RowEventBuilder;
use BinlogRelay\Replication\Event\RowEvent\RowEventFactory;
use BinlogRelay\Replication\Repository\MySQLRepository;
use BinlogRelay\Replication\Repository\RepositoryInterface;
use BinlogRelay\Replication\Socket\Socket;
use BinlogRelay\Replication\Socket\SocketInterface;
use BinlogRelay\Replication\Support\LoggerInterface;
use BinlogRelay\Replication\Support\NullLogger;
use BinlogRelay\Replication\Support\CacheInterface;

use BinlogRelay\Replication\Support\EventDispatcher;


class MySQLReplicationFactory
{
    private ?PdoConnection $connection = null;
    private EventDispatcher $eventDispatcher;
    private Event $event;
    private BinLogSocketConnect $binLogSocketConnect;

    public function __construct(
        Config $config,
        ?RepositoryInterface $repository = null,
        ?CacheInterface $cache = null,
        ?EventDispatcher $eventDispatcher = null,
        ?SocketInterface $socket = null,
        ?LoggerInterface $logger = null
    ) {
        $config->validate();

        if ($repository === null) {
            $this->connection = new PdoConnection(
                $config->host,
                $config->port,
                $config->user,
                $config->password,
                $config->charset
            );
            $repository = new MySQLRepository($this->connection);
        }

        $cache = $cache ?: new ArrayCache($config->tableCacheSize);
        $logger = $logger ?: new NullLogger();
        $socket = $socket ?: new Socket();

        $this->eventDispatcher = $eventDispatcher ?: new EventDispatcher();

        $this->binLogSocketConnect = new BinLogSocketConnect($repository, $socket, $logger, $config);

        $this->event = new Event(
            $this->binLogSocketConnect,
            new RowEventFactory(
                new RowEventBuilder(
                    $repository,
                    $cache,
                    $config,
                    $this->binLogSocketConnect->getBinLogServerInfo(),
                    $logger
                )
            ),
            $this->eventDispatcher,
            $cache,
            $config,
            $this->binLogSocketConnect->getBinLogServerInfo()
        );
    }

    public function registerSubscriber(object $eventSubscribers): void
    {
        $this->eventDispatcher->addSubscriber($eventSubscribers);
    }

    public function unregisterSubscriber(object $eventSubscribers): void
    {
        $this->eventDispatcher->removeSubscriber($eventSubscribers);
    }

    public function getDbConnection(): ?Connection
    {
        return $this->connection;
    }

    public function run(): void
    {
        /** @phpstan-ignore-next-line */
        while (1) {
            $this->consume();
        }
    }

    /**
     * Run replication, checking $shouldStop callback on each iteration, to be able to gracefully stop the process.
     *
     * @param callable $shouldStop Returns true if the process should stop
     */
    public function runWithStopCheck(callable $shouldStop): void
    {
        while (true) {
            if ($shouldStop()) {
                break;
            }

            $this->consume();
        }
    }

    public function consume(): void
    {
        $this->event->consume();
    }

    public function getServerInfo(): BinLogServerInfo
    {
        return $this->binLogSocketConnect->getBinLogServerInfo();
    }
}
