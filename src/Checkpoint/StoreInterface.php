<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

interface StoreInterface
{
    public function load(): ?Checkpoint;

    public function save(Checkpoint $checkpoint): void;

    /** 独占锁：同一份流只允许一个 relay 实例消费，返回 false 表示已被占用 */
    public function acquireLock(string $owner, int $ttl): bool;

    public function refreshLock(string $owner, int $ttl): void;

    public function releaseLock(string $owner): void;

    public function name(): string;
}
