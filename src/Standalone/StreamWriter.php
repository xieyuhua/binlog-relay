<?php

declare(strict_types=1);

namespace BinlogRelay\Standalone;

use BinlogRelay\Event\Change;
use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Queue\Message;
use BinlogRelay\Support\Logger;
use Redis;
use RuntimeException;

/**
 * 只做一件事的写入器：把 Change[] 批量 XADD 到 Redis Stream
 *
 * 单 topic：redis.stream
 * 多 topic：redis.topics（按库/表/操作扇出）
 */
final class StreamWriter implements HandlerInterface
{
    private Redis $redis;
    /** @var list<array{stream:string,tables:list<string>,ops:list<string>,databases:list<string>}> */
    private array $routes;

    public function __construct(private readonly array $config, private readonly Logger $logger)
    {
        $cfg = $config['redis'];
        $this->redis = new Redis();
        $this->redis->connect(
            (string)$cfg['host'],
            (int)$cfg['port'],
            (float)($cfg['timeout'] ?? 2.0)
        );
        $pass = (string)($cfg['password'] ?? '');
        if ($pass !== '') {
            $this->redis->auth($pass);
        }
        $db = (int)($cfg['database'] ?? 0);
        if ($db > 0) {
            $this->redis->select($db);
        }

        $topics = $cfg['topics'] ?? [];
        if ($topics === []) {
            $topics = [['stream' => (string)($cfg['stream'] ?? 'binlog:events')]];
        }
        $this->routes = array_map(static function (array $t): array {
            // labels：该 topic 专属静态标签，合并进消息顶层
            $labels = [];
            foreach ((array)($t['labels'] ?? []) as $k => $v) {
                $labels[(string)$k] = (string)$v;
            }
            return [
                'stream'    => (string)$t['stream'],
                'tables'    => array_map('strval', (array)($t['tables'] ?? [])),
                'ops'       => array_map('strval', (array)($t['ops'] ?? [])),
                'databases' => array_map('strval', (array)($t['databases'] ?? [])),
                'labels'    => $labels,
            ];
        }, $topics);
    }

    public function boot(): void
    {
        $this->logger->info('写入目标：' . implode(', ', array_column($this->routes, 'stream')));
    }

    public function handle(array $changes): void
    {
        if ($changes === []) {
            return;
        }

        $maxLen = (int)($this->config['redis']['max_len'] ?? 200000);
        $plan = [];
        foreach ($changes as $c) {
            foreach ($this->routes as $route) {
                if ($this->matches($route, $c)) {
                    $plan[] = [$route['stream'], Message::fromChange($c, $route['labels'])];
                }
            }
        }
        if ($plan === []) {
            return;
        }

        $this->redis->multi(Redis::PIPELINE);
        foreach ($plan as [$stream, $fields]) {
            $this->redis->xAdd($stream, '*', $fields, $maxLen, true);
        }
        $result = $this->redis->exec();
        if (!is_array($result) || in_array(false, $result, true)) {
            throw new RuntimeException('XADD 失败，Redis 不可用或内存已满');
        }
    }

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
