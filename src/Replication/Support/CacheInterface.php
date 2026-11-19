<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

/**
 * 内置缓存接口（替代 psr/simple-cache）
 *
 * 解析库只用到 get/set/has/clear，这里只保留这四个方法。
 */
interface CacheInterface
{
    public function get(string $key, mixed $default = null): mixed;

    public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool;

    public function has(string $key): bool;

    public function clear(): bool;
}
