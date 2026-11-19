<?php

declare(strict_types=1);

namespace BinlogRelay\Standalone;

use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Event\Filter;
use BinlogRelay\Replication\Config\ConfigBuilder;
use BinlogRelay\Replication\Definitions\ConstEventType;
use BinlogRelay\Replication\MySQLReplicationFactory;
use BinlogRelay\Support\Logger;

/**
 * 装配并运行 MySQLReplicationFactory
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
        $filter = $this->config['filter'] ?? [];

        $builder = (new ConfigBuilder())
            ->withUser((string)$mysql['user'])
            ->withHost((string)$mysql['host'])
            ->withPort((int)$mysql['port'])
            ->withPassword((string)$mysql['password'])
            ->withCharset((string)($mysql['charset'] ?? 'utf8mb4'))
            ->withSlaveId((int)$mysql['slave_id'])
            ->withHeartbeatPeriod((float)($mysql['heartbeat'] ?? 30))
            ->withTableCacheSize((int)($mysql['table_cache_size'] ?? 256))
            ->withEventsOnly([
                ConstEventType::WRITE_ROWS_EVENT_V1->value,
                ConstEventType::WRITE_ROWS_EVENT_V2->value,
                ConstEventType::UPDATE_ROWS_EVENT_V1->value,
                ConstEventType::UPDATE_ROWS_EVENT_V2->value,
                ConstEventType::DELETE_ROWS_EVENT_V1->value,
                ConstEventType::DELETE_ROWS_EVENT_V2->value,
                ConstEventType::XID_EVENT->value,
                ConstEventType::ROTATE_EVENT->value,
                ConstEventType::GTID_LOG_EVENT->value,
            ]);

        $filterObj = Filter::fromConfig($filter);
        if (($filter['databases'] ?? []) !== []) {
            $builder = $builder->withDatabasesOnly($filter['databases']);
        }
        // 表过滤下推到 TABLE_MAP 阶段，省掉未订阅表的行解码
        $tables = $filterObj->isOnlyConfigured()
            ? $filterObj->configuredTables()
            : ($filter['tables'] ?? []);
        if ($tables !== []) {
            $builder = $builder->withTablesOnly($tables);
        }

        $checkpoint = $this->positions->load();
        if ($checkpoint !== null) {
            if ($checkpoint->isGtidMode()) {
                $builder = $builder->withGtid($checkpoint->gtid);
            } elseif ($checkpoint->binlogFile !== '') {
                $builder = $builder
                    ->withBinLogFileName($checkpoint->binlogFile)
                    ->withBinLogPosition($checkpoint->binlogPos);
            }
            $this->logger->info('从点位恢复：' . ($checkpoint->binlogFile !== '' ? $checkpoint->binlogFile . ':' . $checkpoint->binlogPos : 'GTID ' . $checkpoint->gtid));
        } else {
            $this->logger->info('无历史点位，从当前 binlog 末尾开始');
        }

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
