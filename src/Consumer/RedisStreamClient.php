<?php

declare(strict_types=1);

namespace BinlogRelay\Consumer;

use Redis;
use RedisException;

/**
 * Redis Stream 实现（phpredis）
 */
final class RedisStreamClient implements StreamInterface
{
    public function __construct(private readonly Redis $redis)
    {
    }

    public function redis(): Redis
    {
        return $this->redis;
    }

    public function ensureGroup(string $stream, string $group): void
    {
        try {
            $this->redis->xGroup('CREATE', $stream, $group, '0', true);
        } catch (RedisException $e) {
            // BUSYGROUP：组已存在
            if (!str_contains($e->getMessage(), 'BUSYGROUP')) {
                throw $e;
            }
        }
    }

    public function readGroup(string $stream, string $group, string $consumer, int $count, int $blockMs): ?array
    {
        $res = $this->redis->xReadGroup($group, $consumer, [$stream => '>'], 1, $count, $blockMs);
        if (!is_array($res) || !isset($res[$stream]) || !is_array($res[$stream])) {
            return null;
        }
        return $res[$stream];
    }

    public function ack(string $stream, string $group, array $ids): void
    {
        if ($ids === []) {
            return;
        }
        $this->redis->xAck($stream, $group, $ids);
    }

    public function autoClaim(string $stream, string $group, string $consumer, int $minIdleMs, int $count): array
    {
        if ($minIdleMs <= 0 || !method_exists($this->redis, 'xAutoClaim')) {
            return [];
        }
        $res = $this->redis->xAutoClaim($stream, $group, $consumer, $minIdleMs, '0-0', $count);
        if (!is_array($res) || count($res) < 2) {
            return [];
        }
        // phpredis 返回 [nextCursor, [id => fields]]；某些版本顺序相反
        $entries = is_array($res[1]) ? $res[1] : (is_array($res[0]) ? $res[0] : []);
        $out = [];
        foreach ($entries as $id => $fields) {
            if (is_string($id) && is_array($fields)) {
                $out[] = [$id, $fields];
            }
        }
        return $out;
    }

    public function add(string $stream, array $fields): string
    {
        return (string)$this->redis->xAdd($stream, '*', $fields);
    }

    public function lag(string $stream, string $group): array
    {
        $len = (int)$this->redis->xLen($stream);
        $pending = 0;
        try {
            $p = $this->redis->xPending($stream, $group);
            if (is_array($p)) {
                $pending = (int)($p['pending'] ?? ($p[0] ?? 0));
            }
        } catch (RedisException) {
            // group 不存在
        }
        return [
            'stream_len' => $len,
            'pending'    => $pending,
            'lag'        => max(0, $len - $pending),
        ];
    }
}
