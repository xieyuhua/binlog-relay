<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

use BinlogRelay\Checkpoint\Checkpoint;
use BinlogRelay\Checkpoint\FileStore;
use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Checkpoint\RedisStore;
use BinlogRelay\Event\Filter;
use BinlogRelay\Handler\HandlerChain;
use BinlogRelay\Support\Logger;
use BinlogRelay\Replication\Config\ConfigBuilder;
use BinlogRelay\Replication\Definitions\ConstEventType;
use BinlogRelay\Replication\MySQLReplicationFactory;
use RuntimeException;

/**
 * 装配 + 运行
 */
final class Relay
{
    private array $config;
    private Logger $logger;
    private PositionManager $positions;
    private HandlerChain $handlers;
    private RowSubscriber $subscriber;
    private bool $running = true;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->logger = new Logger(
            (string)($config['log']['level'] ?? 'info'),
            (string)($config['log']['file'] ?? '')
        );
    }

    public function logger(): Logger
    {
        return $this->logger;
    }

    public function positionManager(): PositionManager
    {
        return $this->positions;
    }

    public function boot(): void
    {
        $this->positions = $this->makePositionManager();
        $this->positions->lock();

        $this->handlers = new HandlerChain($this->logger);
        $this->handlers->build($this->config['handlers'] ?? []);

        $this->subscriber = new RowSubscriber(
            $this->handlers,
            Filter::fromConfig($this->config['filter'] ?? []),
            $this->positions,
            $this->logger,
            (int)($this->config['batch_size'] ?? 200),
            Connector::makeProjector($this->config, $this->logger)
        );
    }

    /** 停止信号（SIGTERM / SIGINT / Ctrl+C），最多让当前批次跑完 */
    public function stop(): void
    {
        $this->running = false;
    }

    public function run(): void
    {
        $this->runUntil(null);
    }

    /** @param float|null $deadline 到达该 microtime 后停止；null 表示一直运行 */
    public function runUntil(?float $deadline): void
    {
        $mysql = $this->config['mysql'];
        $builder = Connector::build($this->config, $this->positions->load(), $this->logger);

        $factory = new MySQLReplicationFactory($builder->build(), logger: $this->logger);
        $factory->registerSubscriber($this->subscriber);

        $this->logger->info(sprintf(
            'relay 启动：%s@%s:%d slave_id=%d 点位存储=%s',
            $mysql['user'],
            $mysql['host'],
            $mysql['port'],
            $mysql['slave_id'],
            $this->positions->store()->name()
        ));

        $factory->runWithStopCheck(function () use ($deadline): bool {
            return !$this->running || ($deadline !== null && microtime(true) >= $deadline);
        });
    }

    private function makePositionManager(): PositionManager
    {
        $cfg = $this->config['checkpoint'];
        $driver = $cfg['driver'] ?? 'file';

        if ($driver === 'redis') {
            if (!class_exists(\Redis::class)) {
                throw new RuntimeException('checkpoint.driver=redis 需要安装 ext-redis 扩展');
            }
            $store = RedisStore::connect($cfg['redis'], (string)$cfg['key']);
        } else {
            $store = new FileStore(
                (string)$cfg['file']['path'],
                (bool)($cfg['file']['fsync'] ?? false)
            );
        }

        return new PositionManager(
            $store,
            $this->logger,
            (int)($cfg['every_events'] ?? 100),
            (float)($cfg['every_seconds'] ?? 1.0)
        );
    }
}
