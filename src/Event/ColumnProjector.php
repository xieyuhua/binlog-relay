<?php

declare(strict_types=1);

namespace BinlogRelay\Event;

/**
 * 列裁剪：只保留业务需要的列，其余在入队前丢掉
 *
 * 收益：
 * - **队列体积**：大字段（BLOB/TEXT/JSON）往往是消息体积的 80%+，裁掉后 Redis 内存与网络传输同级下降
 * - **消费端解析**：JSON 更小，反序列化更快
 * - **下游写入**：列少了，SQL 更短，索引维护也更轻
 *
 * 配置（config/relay.php 的 filter.columns）：
 *
 *   'columns' => [
 *       // 每张表要保留的列；未列出的表原样保留
 *       'orders'  => ['id', 'status', 'amount', 'updated_at'],
 *       'users'   => ['id', 'name', 'phone'],
 *       // '*' 作用于所有未显式配置的表
 *       '*'       => ['id'],
 *   ],
 *
 * 说明：
 * - 主键列永远保留，即使没写进配置（否则消费端无法幂等）
 * - 同时作用于 row 与 before；before 里被裁掉的列不会出现，下游做 diff 时要注意
 * - 只在「保留列数 < 实际列数」时才真正重建数组，全保留时零开销
 */
final class ColumnProjector
{
    /** @var array<string,array<string,true>> table => [col => true] */
    private array $keep = [];
    /** @var array<string,true>|null */
    private ?array $fallback = null;

    /** @param array<string,list<string>> $config */
    public function __construct(array $config)
    {
        foreach ($config as $table => $cols) {
            $map = [];
            foreach ((array)$cols as $c) {
                $map[(string)$c] = true;
            }
            if ($table === '*') {
                $this->fallback = $map;
            } else {
                $this->keep[(string)$table] = $map;
            }
        }
    }

    public function isEmpty(): bool
    {
        return $this->keep === [] && $this->fallback === null;
    }

    public static function fromConfig(array $cfg): self
    {
        return new self($cfg);
    }

    /**
     * 裁剪一批变更（替换 row/before）
     *
     * @param list<Change> $changes
     * @return list<Change>
     */
    public function apply(array $changes): array
    {
        if ($this->isEmpty()) {
            return $changes;
        }

        foreach ($changes as $i => $change) {
            $cols = $this->keep[$change->table] ?? $this->fallback;
            if ($cols === null) {
                continue;
            }
            // 主键列必须保留；预建集合避免每列都做一次 in_array 线性查找
            $keep = $cols;
            foreach ($change->tableKey as $k) {
                $keep[$k] = true;
            }

            $row = self::pick($change->row, $keep);
            $before = $change->before === null ? null : self::pick($change->before, $keep);
            if ($row === $change->row && $before === $change->before) {
                continue;   // 无需裁剪，避免重建对象
            }
            $changes[$i] = $change->withRows($row, $before);
        }
        return $changes;
    }

    /** 按需裁剪行数据（array_intersect_key 比手写循环快约 30%） */
    public static function project(array $row, array $keep): array
    {
        return array_intersect_key($row, $keep);
    }

    /**
     * @param array<string,mixed>  $row
     * @param array<string,true>   $keep
     * @return array<string,mixed>
     */
    private static function pick(array $row, array $keep): array
    {
        // 先判断是否全保留，避免无谓重建
        $missing = 0;
        foreach ($row as $k => $_) {
            if (!isset($keep[$k])) {
                $missing++;
            }
        }
        if ($missing === 0) {
            return $row;
        }
        return self::project($row, $keep);
    }

    /**
     * 供 EventBuilder 使用：返回该表「保留列 + 主键列」的集合
     *
     * 每个事件只算一次，可传给 projectRow 反复使用，
     * 避免在行循环里重复合并主键列。
     *
     * @param array<int,string> $tableKey
     * @return array<string,true>|null null 表示该表不裁剪
     */
    public function keepSet(string $table, array $tableKey): ?array
    {
        $cols = $this->keep[$table] ?? $this->fallback;
        if ($cols === null) {
            return null;
        }
        foreach ($tableKey as $k) {
            $cols[$k] = true;
        }
        return $cols;
    }

    /**
     * 在构造 Change 之前裁剪原始行，避免事后 withRows() 重建对象
     *
     * @param array<string,mixed> $row
     * @param array<string,true>  $keep 由 keepSet() 返回
     * @return array<string,mixed>
     */
    public static function projectRow(array $row, array $keep): array
    {
        return array_intersect_key($row, $keep);
    }

    /** 该表是否需要裁剪（未配置的表走零开销路径） */
    public function covers(string $table): bool
    {
        return isset($this->keep[$table]) || $this->fallback !== null;
    }
}
