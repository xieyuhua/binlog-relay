<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

use BinlogRelay\Alert\NotificationManager;
use BinlogRelay\Checkpoint\Checkpoint;
use BinlogRelay\Checkpoint\FileStore;
use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Checkpoint\RedisStore;
use BinlogRelay\Event\EventBuilder;
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
    private NotificationManager $alerter;
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
        // 告警通道：配置了企微/钉钉才会真正发送，否则所有 alert() 是空操作
        $this->alerter = NotificationManager::fromConfig(
            $this->config['alert'] ?? [],
            $this->logger
        );
        if (!$this->alerter->isEmpty()) {
            $this->logger->info('告警已启用', ['channels' => $this->config['alert']['_channels'] ?? '']);
        }

        $this->positions = $this->makePositionManager();
        $this->positions->lock();

        $this->handlers = new HandlerChain($this->logger);
        $this->handlers->build($this->config['handlers'] ?? []);

        // 无主键表：日志 + 告警（去重由 NotificationManager 负责）
        EventBuilder::onMissingKey(function (string $msg): void {
            $this->logger->warning($msg);
            $this->alerter->alert(
                'missing-key',
                '存在无主键表',
                $msg . "\n\n幂等键会退化为行内容哈希，建议为该表添加主键。",
                ['影响' => '下游按主键去重会失效']
            );
        });

        // DDL 需要额外订阅 QUERY 事件；getSubscribedEvents() 是静态的，
        // 必须在构造订阅者之前设置开关
        $emitDdl = (bool)($this->config['filter']['emit_ddl'] ?? false);
        RowSubscriber::enableDdl($emitDdl);

        $this->subscriber = new RowSubscriber(
            $this->handlers,
            Filter::fromConfig($this->config['filter'] ?? []),
            $this->positions,
            $this->logger,
            (int)($this->config['batch_size'] ?? 200),
            Connector::makeProjector($this->config, $this->logger),
            $emitDdl
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
        $checkpoint = $this->positions->load();

        // 可选：维护完整的 GTID 执行集合（跨 binlog 文件续传时需要）
        if (!empty($this->config['gtid_set'])) {
            $this->positions->enableGtidSet((string)($checkpoint?->gtid ?? ''));
            $this->logger->info('已启用 GTID 集合累积');
        }

        $builder = Connector::build($this->config, $checkpoint, $this->logger);

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
