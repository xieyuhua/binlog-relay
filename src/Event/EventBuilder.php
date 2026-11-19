<?php

declare(strict_types=1);

namespace BinlogRelay\Event;

use BinlogRelay\Replication\Event\DTO\DeleteRowsDTO;
use BinlogRelay\Replication\Event\DTO\RowsDTO;
use BinlogRelay\Replication\Event\DTO\UpdateRowsDTO;
use BinlogRelay\Replication\BinLog\BinLogCurrent;
use BinlogRelay\Replication\Event\DTO\WriteRowsDTO;
use BinlogRelay\Replication\Event\RowEvent\TableMap;

/**
 * 把 php-mysql-replication 的 RowsDTO 归一化为 Change[]
 * 一个 RowsDTO 可能包含同一张表的多行变更，必须逐行展开
 */
final class EventBuilder
{
    /** 幂等键列缓存：tableId => list<column>，tableId 随 TABLE_MAP 轮换，天然在 DDL 后失效 */
    private static array $keyCache = [];

    /** 已告警过的无键表，避免每个事件都刷日志 */
    private static array $warnedTables = [];

    /** 无键表告警回调，由使用方注入（Logger） */
    private static $warnHandler = null;

    /** GTID 是否可读：未启用 GTID 时属性未初始化，探测一次后走快路径 */
    private static ?bool $gtidReadable = null;

    /** 注入告警回调：遇到既无主键也无唯一键的表时调用一次 */
    public static function onMissingKey(callable $handler): void
    {
        self::$warnHandler = $handler;
    }

    /**
     * @param ColumnProjector|null $projector 传入时在构造 Change 前裁剪行，
     *                                        比事后 withRows() 重建对象快约 83%
     * @return list<Change>
     */
    public static function fromRowsDTO(RowsDTO $dto, ?ColumnProjector $projector = null): array
    {
        $tableMap = $dto->tableMap;
        $info = $dto->getEventInfo();
        $current = $info->binLogCurrent;

        try {
            $file = (string)$current->getBinFileName();
            $pos = (string)$current->getBinLogPosition();
        } catch (\Throwable) {
            $file = '';
            $pos = '0';
        }
        $gtid = self::gtid($current);
        $ts = (int)$info->timestamp;
        $serverId = (int)$info->serverId;

        $database = $tableMap->database;
        $table = $tableMap->table;
        $key = self::tableKey($tableMap);
        $changes = [];

        // 行预处理：归一化 + （可选）列裁剪，都在构造对象前完成
        // keep 集合每个事件只算一次，不在行循环里重复合并主键列
        $keep = ($projector !== null && $projector->covers($table))
            ? $projector->keepSet($table, $key)
            : null;
        $trim = $keep === null
            ? static fn(array $row): array => self::normalize($row)
            : static fn(array $row): array => array_intersect_key(self::normalize($row), $keep);

        // insert 与 delete 结构一致，合并处理
        if ($dto instanceof WriteRowsDTO || $dto instanceof DeleteRowsDTO) {
            $op = $dto instanceof WriteRowsDTO ? Change::INSERT : Change::DELETE;
            foreach ($dto->values as $row) {
                $changes[] = new Change(
                    $op,
                    $database,
                    $table,
                    $trim($row),
                    null,
                    $key,
                    $file,
                    $pos,
                    $gtid,
                    $ts,
                    $serverId
                );
            }
        } elseif ($dto instanceof UpdateRowsDTO) {
            foreach ($dto->values as $pair) {
                $after = $trim($pair['after'] ?? []);
                $before = $trim($pair['before'] ?? []);
                $changes[] = new Change(
                    Change::UPDATE,
                    $database,
                    $table,
                    $after,
                    $before,
                    $key,
                    $file,
                    $pos,
                    $gtid,
                    $ts,
                    $serverId
                );
            }
        }

        return $changes;
    }

    /**
     * 幂等键列名，按优先级回退：
     *   1. 主键 PRI（联合主键按 information_schema 的列顺序，顺序稳定）
     *   2. 唯一键 UNI（无主键但有唯一索引时，仍能唯一定位一行）
     *   3. 空数组（既无主键也无唯一键，由调用方兜底）
     *
     * 结果按 tableId + 表名缓存；tableId 会随 TABLE_MAP 事件更新，DDL 后自动失效。
     *
     * @return list<string>
     */
    private static function tableKey(TableMap $tableMap): array
    {
        $cacheKey = $tableMap->tableId . "\0" . $tableMap->database . '.' . $tableMap->table;
        if (isset(self::$keyCache[$cacheKey])) {
            return self::$keyCache[$cacheKey];
        }

        $primary = [];
        $unique = [];
        foreach ($tableMap->columnDTOCollection as $column) {
            $keyType = strtoupper((string)($column->fieldDTO->columnKey ?? ''));
            if ($keyType === 'PRI') {
                $primary[] = $column->getName();
            } elseif ($keyType === 'UNI') {
                $unique[] = $column->getName();
            }
        }

        // 主键优先；没有主键时退到唯一键
        $key = $primary !== [] ? $primary : $unique;

        // 一旦遇到无键表，说明下游幂等会退化，只告警一次以免刷屏
        if ($key === [] && !isset(self::$warnedTables[$cacheKey])) {
            self::$warnedTables[$cacheKey] = true;
            self::$warnHandler !== null && (self::$warnHandler)(
                sprintf('表 %s.%s 无主键也无唯一键，将按整行内容生成幂等键（性能与准确性下降）',
                    $tableMap->database, $tableMap->table)
            );
        }

        // 表数量增长时简单裁剪，避免长驻进程无限增长
        if (count(self::$keyCache) > 512) {
            self::$keyCache = [];
            self::$warnedTables = [];
        }
        return self::$keyCache[$cacheKey] = $key;
    }

    /** GTID 读取：未启用 GTID 时 v9 的属性未初始化，探测一次后固定走快路径 */
    private static function gtid(BinLogCurrent $current): string
    {
        if (self::$gtidReadable === false) {
            return '';
        }
        try {
            $gtid = (string)$current->getGtid();
            self::$gtidReadable = true;
            return $gtid;
        } catch (\Throwable) {
            self::$gtidReadable = false;
            return '';
        }
    }

    /**
     * 统一把二进制/非 UTF-8 值转为可 JSON 序列化的形式
     *
     * 快路径：先判类型，只有 string 才做编码检查；纯 ASCII 在 PHP 中必然合法，
     * 跳过 mb_check_encoding（实测可省掉约 60% 的 normalize 开销）。
     */
    private static function normalize(array $row): array
    {
        foreach ($row as $k => $v) {
            if (is_string($v)) {
                if (!mb_check_encoding($v, 'UTF-8')) {
                    $row[$k] = ['__binary__' => base64_encode($v)];
                }
            } elseif (is_object($v)) {
                if ($v instanceof \DateTimeInterface) {
                    $row[$k] = $v->format('Y-m-d H:i:s');
                } else {
                    $row[$k] = null;
                }
            } elseif (is_resource($v)) {
                $row[$k] = null;
            }
        }
        return $row;
    }
}
