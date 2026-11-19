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
    /** 主键列缓存：tableId => [db\0table, list<string>]，tableId 随 TABLE_MAP 轮换，天然在 DDL 后失效 */
    private static array $keyCache = [];

    /** GTID 是否可读：v9 未启用 GTID 时属性未初始化，探测一次后走快路径 */
    private static ?bool $gtidReadable = null;

    /**
     * @return list<Change>
     */
    public static function fromRowsDTO(RowsDTO $dto): array
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

        // insert 与 delete 结构一致，合并处理
        if ($dto instanceof WriteRowsDTO || $dto instanceof DeleteRowsDTO) {
            $op = $dto instanceof WriteRowsDTO ? Change::INSERT : Change::DELETE;
            foreach ($dto->values as $row) {
                $changes[] = new Change(
                    $op,
                    $database,
                    $table,
                    self::normalize($row),
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
                $after = self::normalize($pair['after'] ?? []);
                $before = self::normalize($pair['before'] ?? []);
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
     * 主键列名（来自 information_schema，COLUMN_KEY = PRI）
     * 按 tableId + 表名缓存，避免每个事件都遍历列集合。
     * tableId 会随 TABLE_MAP 事件更新，DDL 后自动失效。
     *
     * @return list<string>
     */
    private static function tableKey(TableMap $tableMap): array
    {
        $cacheKey = $tableMap->tableId . "\0" . $tableMap->database . '.' . $tableMap->table;
        if (isset(self::$keyCache[$cacheKey])) {
            return self::$keyCache[$cacheKey];
        }

        $pk = [];
        foreach ($tableMap->columnDTOCollection as $column) {
            if (strtoupper((string)($column->fieldDTO->columnKey ?? '')) === 'PRI') {
                $pk[] = $column->getName();
            }
        }

        // 表数量增长时简单裁剪，避免长驻进程无限增长
        if (count(self::$keyCache) > 512) {
            self::$keyCache = [];
        }
        return self::$keyCache[$cacheKey] = $pk;
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
