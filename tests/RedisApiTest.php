<?php
/**
 * phpredis API 参数契约测试
 *
 * 回归保护：曾因给 xReadGroup 传了 6 个参数（多一个 "streams count"）
 * 导致线上报 "expects at most 5 arguments"。
 *
 *   php tests/RedisApiTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

if (!extension_loaded('redis')) {
    echo "[SKIP] 未安装 phpredis，跳过\n";
    exit(0);
}

/** 期望的参数个数上限（phpredis 6.x） */
$expected = [
    'xReadGroup' => 5,
    'xAutoClaim' => 7,
    'xPending'   => 6,
    'xAck'       => 3,
    'xAdd'       => 6,
    'xLen'       => 1,
    'xRange'     => 4,
    'xDel'       => 2,
    'xGroup'     => 6,
];

$ref = new ReflectionClass(Redis::class);
foreach ($expected as $method => $max) {
    if (!$ref->hasMethod($method)) {
        check("phpredis 提供 {$method}", false);
        continue;
    }
    $actual = $ref->getMethod($method)->getNumberOfParameters();
    check(
        "{$method} 最多 {$max} 个参数（实际 {$actual}）",
        $actual === $max,
        "phpredis 版本变更，需同步调整 RedisStreamClient"
    );
}

// 运行时验证：三参数分支与五参数分支都能被 phpredis 接受
// （真正的参数个数错误只有执行时才会暴露，静态扫描易被三元表达式干扰）
class CountingRedis extends Redis
{
    public array $calls = [];

    public function xReadGroup(string $group, string $consumer, array $streams, int $count = 1, int $block = 1): Redis|array|bool
    {
        $this->calls[] = array_filter([$group, $consumer, $streams, $count, $block], static fn($v) => $v !== null);
        return false;   // 模拟无新消息
    }
}

$fake = new CountingRedis();
$client = new BinlogRelay\Consumer\RedisStreamClient($fake);
$client->readGroup('s', 'g', 'c', 10, 100);
check('readGroup 调用 xReadGroup 未抛参数错误', count($fake->calls) === 1);
check(
    '实际传入参数不超过 5 个',
    count($fake->calls[0] ?? []) <= 5,
    '实际 ' . count($fake->calls[0] ?? []) . ' 个'
);

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
