<?php

declare(strict_types=1);

namespace BinlogRelay\Event;

use MySQLReplication\Event\DTO\DeleteRowsDTO;
use MySQLReplication\Event\DTO\RowsDTO;
use MySQLReplication\Event\DTO\UpdateRowsDTO;
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\Event\RowEvent\TableMap;

/**
 * 把 php-mysql-replication 的 RowsDTO 归一化为 Change[]
 * 一个 RowsDTO 可能包含同一张表的多行变更，必须逐行展开
 */
final class EventBuilder
{
    /**
     * @return list<Change>
     */
    public static function fromRowsDTO(RowsDTO $dto): array
    {
        $tableMap = $dto->tableMap;
        $info = $dto->getEventInfo();
        $current = $info->binLogCurrent;

        // v9 的 BinLogCurrent 在未启用 GTID 时属性未初始化，直接 getter 会抛错
        $file = self::safeGet($current, 'getBinFileName');
        $pos = self::safeGet($current, 'getBinLogPosition', '0');
        $gtid = self::safeGet($current, 'getGtid');
        $ts = (int)$info->timestamp;
        $serverId = (int)$info->serverId;

        $key = self::tableKey($tableMap);
        $changes = [];

        if ($dto instanceof WriteRowsDTO) {
            foreach ($dto->values as $row) {
                $changes[] = new Change(
                    Change::INSERT,
                    $tableMap->database,
                    $tableMap->table,
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
        } elseif ($dto instanceof DeleteRowsDTO) {
            foreach ($dto->values as $row) {
                $changes[] = new Change(
                    Change::DELETE,
                    $tableMap->database,
                    $tableMap->table,
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
                    $tableMap->database,
                    $tableMap->table,
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
     * 取不到时返回空数组，下游按业务主键自行处理。
     *
     * @return list<string>
     */
    private static function tableKey(TableMap $tableMap): array
    {
        $pk = [];
        foreach ($tableMap->columnDTOCollection as $column) {
            if (strtoupper((string)($column->fieldDTO->columnKey ?? '')) === 'PRI') {
                $pk[] = $column->getName();
            }
        }
        return $pk;
    }

    /** 属性未初始化时 getter 会抛 Error，这里统一兜底为空串 */
    private static function safeGet(object $obj, string $method, string $default = ''): string
    {
        try {
            return (string)$obj->{$method}();
        } catch (\Throwable) {
            return $default;
        }
    }

    /** 统一把二进制/非 UTF-8 值转为可 JSON 序列化的形式 */
    private static function normalize(array $row): array
    {
        foreach ($row as $k => $v) {
            if (is_resource($v)) {
                $row[$k] = null;
                continue;
            }
            if (is_string($v) && !mb_check_encoding($v, 'UTF-8')) {
                $row[$k] = ['__binary__' => base64_encode($v)];
            } elseif (is_object($v) && $v instanceof \DateTimeInterface) {
                $row[$k] = $v->format('Y-m-d H:i:s');
            }
        }
        return $row;
    }
}
