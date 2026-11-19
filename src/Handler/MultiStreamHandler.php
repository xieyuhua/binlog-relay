<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;
use BinlogRelay\Queue\Message;
use Redis;
use RuntimeException;

/**
 * 同一批数据分发到多个 Redis Stream（一个业务一个 topic）
 *
 * 相比 FanoutHandler 的好处：
 * - 只用一个 Redis 连接，整批所有 topic 的 XADD 走同一个 pipeline：一次网络往返
 * - 单条变更命中多个 topic 时，写入顺序稳定（按规则声明顺序）
 *
 * 路由规则（config/relay.php 的 handlers.fanout）：
 *   [
 *     ['stream' => 'topic:order',  'tables' => ['orders']],
 *     ['stream' => 'topic:stock',  'tables' => ['stock']],
 *     ['stream' => 'topic:search', 'tables' => ['*']],           // 全部
 *     ['stream' => 'topic:audit',  'tables' => ['orders'], 'ops' => ['delete']],
 *   ]
 *
 * 设计要点（对业务影响最小）：
 * - 一条变更可命中多条规则 → 各 topic 独立消费，互不阻塞
 * - 某个 topic 的消费端挂掉不会影响其它 topic（各自 group 各自的 pending）
 * - XADD 失败 → 抛异常，relay 停止推进点位，重启后重放；不会出现"部分 topic 有数据、部分没有"
 *   这种长期不一致（短暂不一致窗口存在，由下游幂等覆盖）
 */
final class MultiStreamHandler implements HandlerInterface
{
    private const MAX_LEN_APPROX = 200000;

    private Redis $redis;
    /** @var list<array{stream:string,tables:list<string>,ops:list<string>,databases:list<string>}> */
    private array $routes;

    public function __construct(array $routes, ?Redis $redis = null, array $redisConfig = [])
    {
        $this->routes = array_map(function (array $r): array {
            if (empty($r['stream'])) {
                throw new RuntimeException('fanout 规则缺少 stream');
            }
            return [
                'stream'    => (string)$r['stream'],
                'tables'    => array_map('strval', (array)($r['tables'] ?? [])),
                'ops'       => array_map('strval', (array)($r['ops'] ?? [])),
                'databases' => array_map('strval', (array)($r['databases'] ?? [])),
            ];
        }, $routes);

        if ($this->routes === []) {
            throw new RuntimeException('fanout 至少需要一条路由规则');
        }

        if ($redis !== null) {
            $this->redis = $redis;
            return;
        }

        $this->redis = new Redis();
        $this->redis->connect(
            (string)($redisConfig['host'] ?? '127.0.0.1'),
            (int)($redisConfig['port'] ?? 6379),
            (float)($redisConfig['timeout'] ?? 2.0)
        );
        $pass = (string)($redisConfig['password'] ?? '');
        if ($pass !== '') {
            $this->redis->auth($pass);
        }
        $db = (int)($redisConfig['database'] ?? 0);
        if ($db > 0) {
            $this->redis->select($db);
        }
    }

    public function handle(array $changes): void
    {
        if ($changes === []) {
            return;
        }

        // 先算好每条变更要进哪些 topic，再一次性 pipeline 全部 XADD
        $plan = [];
        foreach ($changes as $c) {
            foreach ($this->routes as $route) {
                if ($this->matches($route, $c)) {
                    $plan[] = [$route['stream'], Message::fromChange($c)];
                }
            }
        }

        if ($plan === []) {
            return;
        }

        $this->redis->multi(Redis::PIPELINE);
        foreach ($plan as [$stream, $fields]) {
            $this->redis->xAdd($stream, '*', $fields, self::MAX_LEN_APPROX, true);
        }
        $result = $this->redis->exec();

        if (!is_array($result) || in_array(false, $result, true)) {
            throw new RuntimeException('fanout XADD 失败，Redis 可能不可用或内存已满');
        }
    }

    /** @param array{stream:string,tables:list<string>,ops:list<string>,databases:list<string>} $route */
    private function matches(array $route, Change $c): bool
    {
        if ($route['databases'] !== [] && !in_array($c->database, $route['databases'], true)) {
            return false;
        }
        if ($route['tables'] !== [] && !in_array('*', $route['tables'], true)
            && !in_array($c->table, $route['tables'], true)) {
            return false;
        }
        if ($route['ops'] !== [] && !in_array($c->op, $route['ops'], true)) {
            return false;
        }
        return true;
    }
}
