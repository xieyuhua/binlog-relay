<?php

declare(strict_types=1);

namespace BinlogRelay\Relay;

use BinlogRelay\Checkpoint\Checkpoint;
use BinlogRelay\Event\Filter;
use BinlogRelay\Replication\Config\ConfigBuilder;
use BinlogRelay\Replication\Definitions\ConstEventType;
use BinlogRelay\Support\Logger;

/**
 * 把配置翻译成 MySQLReplicationFactory 需要的 Config
 *
 * 生产端（Relay）与单体（binlog2redis）共用，避免两处重复维护
 * 连接参数、事件白名单、过滤下推、点位恢复逻辑。
 */
final class Connector
{
    /**
     * @param array<string,mixed> $config 完整配置（含 mysql / filter）
     */
    public static function build(array $config, ?Checkpoint $checkpoint, Logger $logger): ConfigBuilder
    {
        $mysql = $config['mysql'];
        $filter = $config['filter'] ?? [];

        $builder = (new ConfigBuilder())
            ->withUser((string)$mysql['user'])
            ->withHost((string)$mysql['host'])
            ->withPort((int)$mysql['port'])
            ->withPassword((string)$mysql['password'])
            ->withCharset((string)($mysql['charset'] ?? 'utf8mb4'))
            ->withSlaveId((int)$mysql['slave_id'])
            ->withHeartbeatPeriod((float)($mysql['heartbeat'] ?? 30))
            ->withTableCacheSize((int)($mysql['table_cache_size'] ?? 256))
            // 只订阅行变更与事务边界；DDL 等事件不进入业务管道
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

        // 库过滤
        if (($filter['databases'] ?? []) !== []) {
            $builder = $builder->withDatabasesOnly($filter['databases']);
        }

        // 表过滤下推到 TABLE_MAP 阶段：未订阅的表跳过 information_schema 查询与行解码
        $filterObj = Filter::fromConfig($filter);
        $tables = $filterObj->isOnlyConfigured()
            ? $filterObj->configuredTables()
            : ($filter['tables'] ?? []);
        if ($tables !== []) {
            $builder = $builder->withTablesOnly($tables);
        }

        // 点位恢复
        if ($checkpoint !== null) {
            if ($checkpoint->isGtidMode()) {
                $builder = $builder->withGtid($checkpoint->gtid);
            } elseif ($checkpoint->binlogFile !== '') {
                $builder = $builder
                    ->withBinLogFileName($checkpoint->binlogFile)
                    ->withBinLogPosition($checkpoint->binlogPos);
            }
            $logger->info('从已保存点位恢复：' . self::describe($checkpoint));
        } else {
            $logger->info('未找到历史点位，将从当前最新 binlog 末尾开始');
        }

        return $builder;
    }

    /** 按配置构造列裁剪器；未配置时返回 null（零开销路径） */
    public static function makeProjector(array $config, Logger $logger): ?\BinlogRelay\Event\ColumnProjector
    {
        $cols = $config['filter']['columns'] ?? [];
        if (!is_array($cols) || $cols === []) {
            return null;
        }
        $logger->info('列裁剪已启用，配置表数：' . count($cols));
        return \BinlogRelay\Event\ColumnProjector::fromConfig($cols);
    }

    public static function describe(Checkpoint $checkpoint): string
    {
        return $checkpoint->binlogFile !== ''
            ? $checkpoint->binlogFile . ':' . $checkpoint->binlogPos
            : 'GTID ' . $checkpoint->gtid;
    }
}
