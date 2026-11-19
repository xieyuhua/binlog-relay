<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

use BinlogRelay\Checkpoint\Checkpoint;
use BinlogRelay\Checkpoint\FileStore;
use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Checkpoint\RedisStore;
use BinlogRelay\Event\ColumnProjector;
use BinlogRelay\Event\Filter;
use BinlogRelay\Handler\HandlerChain;
use BinlogRelay\Support\Logger;
use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Definitions\ConstEventType;
use MySQLReplication\MySQLReplicationFactory;
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
    private Subscriber $subscriber;
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

        $filter = Filter::fromConfig($this->config['filter'] ?? []);
        $projector = null;
        $columnCfg = $this->config['filter']['columns'] ?? [];
        if (is_array($columnCfg) && $columnCfg !== []) {
            $projector = ColumnProjector::fromConfig($columnCfg);
            $this->logger->info('列裁剪已启用，配置表数：' . count($columnCfg));
        }

        $this->subscriber = new Subscriber(
            $this->handlers,
            $filter,
            $this->positions,
            $this->logger,
            (int)($this->config['batch_size'] ?? 200),
            $projector
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
        $builder = (new ConfigBuilder())
            ->withUser((string)$mysql['user'])
            ->withHost((string)$mysql['host'])
            ->withPort((int)$mysql['port'])
            ->withPassword((string)$mysql['password'])
            ->withCharset((string)($mysql['charset'] ?? 'utf8mb4'))
            ->withSlaveId((int)$mysql['slave_id'])
            ->withHeartbeatPeriod((float)($mysql['heartbeat'] ?? 30))
            ->withTableCacheSize((int)($mysql['table_cache_size'] ?? 256))
            // 只订阅行变更与事务边界；DDL（Query）等事件不进入业务管道
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

        $filter = $this->config['filter'] ?? [];
        $builder = $this->applyFilter($builder, $filter);

        $checkpoint = $this->positions->load();
        if ($checkpoint !== null) {
            $builder = $this->applyCheckpoint($builder, $checkpoint);
            $this->logger->info(sprintf(
                '从已保存点位恢复：%s',
                $checkpoint->binlogFile !== ''
                    ? $checkpoint->binlogFile . ':' . $checkpoint->binlogPos
                    : 'GTID ' . $checkpoint->gtid
            ));
        } else {
            $this->logger->info('未找到历史点位，将从当前最新 binlog 末尾开始');
        }

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

    private function applyFilter(ConfigBuilder $builder, array $filter): ConfigBuilder
    {
        $databases = $filter['databases'] ?? [];
        $tables = $filter['tables'] ?? [];

        // v9 只支持白名单（Only）；忽略规则由 BinlogRelay\Event\Filter 在业务侧兜底
        if ($databases !== []) {
            $builder = $builder->withDatabasesOnly($databases);
        }
        if ($tables !== []) {
            $builder = $builder->withTablesOnly($tables);
        }
        return $builder;
    }

    private function applyCheckpoint(ConfigBuilder $builder, Checkpoint $checkpoint): ConfigBuilder
    {
        if ($checkpoint->isGtidMode()) {
            return $builder->withGtid($checkpoint->gtid);
        }
        if ($checkpoint->binlogFile !== '') {
            return $builder
                ->withBinLogFileName($checkpoint->binlogFile)
                ->withBinLogPosition($checkpoint->binlogPos);
        }
        return $builder;
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
