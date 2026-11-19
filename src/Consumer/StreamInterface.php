<?php

declare(strict_types=1);

namespace BinlogRelay\Consumer;

/**
 * 队列客户端抽象
 *
 * 生产环境由 RedisStreamClient 实现；测试可注入内存假对象，
 * 无需启动 Redis 也能验证 ACK / 重试 / 死信语义。
 */
interface StreamInterface
{
    public function ensureGroup(string $stream, string $group): void;

    /** @return array<string,array<string,string>>|null [id => fields] */
    public function readGroup(string $stream, string $group, string $consumer, int $count, int $blockMs): ?array;

    /** @param list<string> $ids */
    public function ack(string $stream, string $group, array $ids): void;

    /** @return list<array{0:string,1:array<string,string>}> [id, fields] */
    public function autoClaim(string $stream, string $group, string $consumer, int $minIdleMs, int $count): array;

    /** @param array<string,string> $fields */
    public function add(string $stream, array $fields): string;

    /** @return array{stream_len:int,pending:int,lag:int} */
    public function lag(string $stream, string $group): array;
}
