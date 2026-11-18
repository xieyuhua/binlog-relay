<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;

// env() 定义在 src/Support/env.php，CLI 入口已加载；单独实例化时兜底引入

/**
 * 方式 A：把变更批量推进 Redis Stream（pipeline，一次 RTT）
 *
 * relay 只负责搬运，写库/缓存等业务在消费端完成：
 *   XGROUP CREATE binlog:events cg:worker 0 MKSTREAM
 *   XREADGROUP GROUP cg:worker worker-1 COUNT 1000 BLOCK 5000 STREAMS binlog:events >
 *   ... 写库成功后 XACK binlog:events cg:worker <id>
 *
 * maxLen 使用近似裁剪（O(1)），防止消费端宕机时 Redis 无限增长。
 */
final class RedisStreamHandler implements HandlerInterface
{
    private const MAX_LEN_APPROX = 200000;

    private \Redis $redis;

    public function __construct(
        private readonly string $stream = 'binlog:events',
        ?\Redis $redis = null,
    ) {
        if ($redis !== null) {
            $this->redis = $redis;
            return;
        }
        $this->redis = new \Redis();
        $this->redis->connect(
            (string)env('RELAY_REDIS_HOST', '127.0.0.1'),
            (int)env('RELAY_REDIS_PORT', 6379),
            (float)env('RELAY_REDIS_TIMEOUT', 2.0)
        );
        $pass = (string)env('RELAY_REDIS_PASSWORD', '');
        if ($pass !== '') {
            $this->redis->auth($pass);
        }
        $db = (int)env('RELAY_REDIS_DB', 0);
        if ($db > 0) {
            $this->redis->select($db);
        }
    }

    public function handle(array $changes): void
    {
        if ($changes === []) {
            return;
        }

        // pipeline：整批一次网络往返
        $this->redis->multi(\Redis::PIPELINE);
        foreach ($changes as $c) {
            $this->redis->xAdd(
                $this->stream,
                '*',
                [
                    'op'   => $c->op,
                    'db'   => $c->database,
                    'tbl'  => $c->table,
                    'pk'   => $c->rowIdentity(),
                    'pos'  => $c->binlogFile . ':' . $c->binlogPos,
                    'ts'   => (string)$c->timestamp,
                    'data' => json_encode(
                        $c->toArray(),
                        JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE
                    ),
                ],
                self::MAX_LEN_APPROX,
                true
            );
        }
        $result = $this->redis->exec();

        // 任一条失败：抛出让 relay 停止推进点位，重启后重放
        if (!is_array($result) || in_array(false, $result, true)) {
            throw new \RuntimeException('XADD 失败，Redis 可能不可用或内存已满');
        }
    }
}
