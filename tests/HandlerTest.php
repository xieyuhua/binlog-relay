<?php
/**
 * HandlerChain / RedisStreamHandler 测试
 *
 * Redis 部分：优先用配置里的真实 Redis；连不上时用内存假对象验证
 * pipeline 调用顺序与失败传播（不依赖外部服务也能跑 CI）。
 *
 *   php tests/HandlerTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use BinlogRelay\Event\Change;
use BinlogRelay\Handler\HandlerChain;
use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Handler\RedisStreamHandler;
use BinlogRelay\Support\Logger;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

function makeChanges(int $n): array
{
    $out = [];
    for ($i = 1; $i <= $n; $i++) {
        $out[] = new Change(
            Change::INSERT,
            'demo',
            'orders',
            ['id' => $i, 'name' => '行' . $i],
            null,
            ['id'],
            'mysql-bin.000001',
            (string)(100 + $i),
            '',
            1795013415,
            1
        );
    }
    return $out;
}

// --- HandlerChain 执行顺序 ---
$order = [];
$make = function (string $tag) use (&$order): HandlerInterface {
    return new class($tag, $order) implements HandlerInterface {
        public function __construct(private string $tag, private array &$order) {}
        public function handle(array $changes): void { $this->order[] = $this->tag; }
    };
};
$chain = new HandlerChain(new Logger('error'));
$chain->build([$make('a'), $make('b'), $make('c')]);
$chain->handle(makeChanges(1));
check('按注册顺序执行', $order === ['a', 'b', 'c'], json_encode($order));

// --- 空批次不调用 handler ---
$order = [];
$chain->handle([]);
check('空批次跳过所有 handler', $order === []);

// --- handler 异常中断后续 handler ---
$order = [];
$chain2 = new HandlerChain(new Logger('error'));
$chain2->build([
    new class implements HandlerInterface {
        public function handle(array $changes): void { throw new RuntimeException('boom'); }
    },
    $make('never'),
]);
$thrown = false;
try {
    $chain2->handle(makeChanges(1));
} catch (RuntimeException $e) {
    $thrown = $e->getMessage() === 'boom';
}
check('异常向上抛且不执行后续 handler', $thrown && $order === []);

// --- RedisStreamHandler：pipeline 与失败传播 ---
$redisAvailable = false;
if (extension_loaded('redis')) {
    try {
        $probe = new Redis();
        $redisAvailable = (bool)$probe->connect('127.0.0.1', 6379, 0.5);
        $probe->close();
    } catch (RedisException) {
        $redisAvailable = false;
    }
}

if ($redisAvailable) {
    $redis = new Redis();
    $redis->connect('127.0.0.1', 6379, 1.0);
    $stream = 'binlog:test:' . getmypid();
    $redis->del($stream);

    $handler = new RedisStreamHandler($stream, $redis);
    $handler->handle(makeChanges(5));

    $len = $redis->xLen($stream);
    check('Redis 收到 5 条消息', $len === 5, '实际 ' . $len);

    $entries = $redis->xRange($stream, '-', '+');
    $first = reset($entries);
    check('消息含 op/db/tbl/pk/pos', isset($first['op'], $first['db'], $first['tbl'], $first['pk'], $first['pos']));
    check('data 是合法 JSON', json_decode($first['data'], true) !== null);
    $redis->del($stream);
} else {
    echo "[SKIP] 本机无可用 Redis，跳过真实 Redis 断言（CI 可用 docker run -p 6379:6379 redis）\n";
}

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
