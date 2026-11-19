<?php

declare(strict_types=1);

namespace BinlogRelay\Standalone;

use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Relay\Connector;
use BinlogRelay\Replication\MySQLReplicationFactory;
use BinlogRelay\Support\Logger;

/**
 * 单体服务的运行器：装配连接并启动 binlog 消费
 *
 * 连接参数、事件白名单、过滤下推、点位恢复全部复用 Relay\Connector，
 * 与生产端保持同一套行为。
 */
final class Runner
{
    private bool $running = true;

    public function __construct(
        private readonly array $config,
        private readonly Logger $logger,
        private readonly PositionManager $positions,
        private readonly RelaySubscriber $subscriber,
    ) {
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function run(?float $deadline): void
    {
        $mysql = $this->config['mysql'];

        $builder = Connector::build($this->config, $this->positions->load(), $this->logger);
        $factory = new MySQLReplicationFactory($builder->build(), logger: $this->logger);
        $factory->registerSubscriber($this->subscriber);

        $this->logger->info(sprintf(
            '已连接 %s@%s:%d slave_id=%d',
            $mysql['user'],
            $mysql['host'],
            $mysql['port'],
            $mysql['slave_id']
        ));

        $factory->runWithStopCheck(function () use ($deadline): bool {
            return !$this->running || ($deadline !== null && microtime(true) >= $deadline);
        });
    }
}
