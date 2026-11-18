<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

use Redis;
use RuntimeException;

/**
 * Redis 点位存储
 *
 * - 点位用 HSET 存 hash，字段更新原子，读取一次拿全
 * - 锁用 SET NX PX，带 TTL，进程崩溃后自动过期
 * - 锁续期用 Lua CAS（只续自己的锁）
 */
final class RedisStore implements StoreInterface
{
    private Redis $redis;
    private string $key;

    public function __construct(Redis $redis, string $key)
    {
        $this->redis = $redis;
        $this->key = $key;
    }

    public static function connect(array $cfg, string $key): self
    {
        $redis = new Redis();
        $ok = $redis->connect(
            (string)($cfg['host'] ?? '127.0.0.1'),
            (int)($cfg['port'] ?? 6379),
            (float)($cfg['timeout'] ?? 2.0)
        );
        if (!$ok) {
            throw new RuntimeException('Redis 连接失败');
        }
        $pass = (string)($cfg['password'] ?? '');
        if ($pass !== '') {
            $redis->auth($pass);
        }
        $db = (int)($cfg['database'] ?? 0);
        if ($db > 0) {
            $redis->select($db);
        }
        return new self($redis, $key);
    }

    public function name(): string
    {
        return 'redis:' . $this->key;
    }

    public function load(): ?Checkpoint
    {
        $data = $this->redis->hGetAll($this->key);
        if (!$data && !$this->redis->exists($this->key)) {
            return null;
        }
        // 锁存在但点位 hash 不存在时会返回空数组
        if (!$data) {
            return null;
        }
        return Checkpoint::fromArray($data);
    }

    public function save(Checkpoint $checkpoint): void
    {
        $this->redis->hMSet($this->key, $checkpoint->toArray());
    }

    public function acquireLock(string $owner, int $ttl): bool
    {
        return (bool)$this->redis->set(
            $this->key . ':lock',
            $owner,
            ['nx', 'px' => $ttl * 1000]
        );
    }

    public function refreshLock(string $owner, int $ttl): void
    {
        $lua = <<<'LUA'
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
LUA;
        $this->redis->eval($lua, [$this->key . ':lock', $owner, $ttl * 1000], 1);
    }

    public function releaseLock(string $owner): void
    {
        $lua = <<<'LUA'
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
LUA;
        $this->redis->eval($lua, [$this->key . ':lock', $owner], 1);
    }
}
