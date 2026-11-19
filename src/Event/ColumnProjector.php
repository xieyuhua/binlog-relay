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
     * 裁剪一批变更（原地替换 row/before）
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
            $changes[$i] = $change->withRows(
                self::pick($change->row, $cols, $change->tableKey),
                $change->before === null
                    ? null
                    : self::pick($change->before, $cols, $change->tableKey)
            );
        }
        return $changes;
    }

    /**
     * @param array<string,mixed>  $row
     * @param array<string,true>   $keep
     * @param array<int,string>    $tableKey
     * @return array<string,mixed>
     */
    private static function pick(array $row, array $keep, array $tableKey): array
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

        $out = [];
        foreach ($row as $k => $v) {
            if (isset($keep[$k]) || in_array($k, $tableKey, true)) {
                $out[$k] = $v;
            }
        }
        return $out;
    }
}
