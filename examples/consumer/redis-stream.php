<?php
/**
 * 消费示例 1：Redis Stream（consumer group）
 *
 * 适用：本仓库 PHP 版默认输出（RedisStreamHandler / StreamWriter）。
 *
 * 可靠性要点：
 * - XREADGROUP 用 '>' 只取未投递的消息
 * - 处理成功后才 XACK，崩溃只会重放，不会丢
 * - 启动/空闲时用 XAUTOCLAIM 接管超时未确认的消息
 *
 * 幂等：下游按 msg.id（幂等键）去重；重放是常态，不是异常。
 *
 *   php examples/consumer/redis-stream.php
 */

declare(strict_types=1);

require __DIR__ . '/../../src/bootstrap.php';

$host = getenv('REDIS_HOST') ?: '127.0.0.1';
$port = (int)(getenv('REDIS_PORT') ?: 6379);
$stream = getenv('STREAM') ?: 'binlog:events';
$group = getenv('GROUP') ?: 'cg:default';
$consumer = gethostname() . '-' . getmypid();
$batch = (int)(getenv('BATCH') ?: 100);

$redis = new Redis();
$redis->connect($host, $port, 2.0);
if ($pass = getenv('REDIS_PASSWORD')) {
    $redis->auth($pass);
}

// mkstream=true：流不存在时自动创建
try {
    $redis->xGroup('CREATE', $stream, $group, '0', true);
} catch (RedisException $e) {
    if (!str_contains($e->getMessage(), 'BUSYGROUP')) {
        throw $e;   // BUSYGROUP = 组已存在，正常
    }
}

fwrite(STDERR, "消费 {$stream} / {$group} as {$consumer}\n");

$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    $stop = static function () use (&$running): void {
        fwrite(STDERR, "收到停止信号，处理完当前批次后退出\n");
        $running = false;
    };
    pcntl_signal(SIGTERM, $stop);
    pcntl_signal(SIGINT, $stop);
}

while ($running) {
    // 先接管本消费者超时未确认的消息（崩溃重启后遗留）
    $claimed = $redis->xAutoClaim($stream, $group, $consumer, 60_000, '0-0', $batch);
    if (is_array($claimed) && !empty($claimed[1])) {
        foreach ($claimed[1] as $id => $fields) {
            process($id, $fields);
            $redis->xAck($stream, $group, [$id]);
        }
    }

    // phpredis 签名只有 5 个参数：group, consumer, streams, count, block
    $res = $redis->xReadGroup($group, $consumer, [$stream => '>'], $batch, 5000);
    if (!is_array($res) || !isset($res[$stream])) {
        continue;
    }

    foreach ($res[$stream] as $id => $fields) {
        try {
            process($id, $fields);
            // 只有处理成功才 ACK；抛异常会跳过 ACK，消息留在 pending 等重试
            $redis->xAck($stream, $group, [$id]);
        } catch (Throwable $e) {
            fwrite(STDERR, "处理 {$id} 失败: {$e->getMessage()}\n");
        }
    }
}

/**
 * 处理单条消息
 *
 * @param array<string,string> $fields Redis Stream 的字段（全部是字符串）
 */
function process(string $id, array $fields): void
{
    $kind = $fields['kind'] ?? 'row';

    if ($kind === 'ddl') {
        printf(
            "[DDL] %s %s tables=%s\n  %s\n",
            $fields['op'] ?? '',
            $fields['db'] ?? '',
            $fields['ddl_tables'] ?? '[]',
            $fields['ddl_query'] ?? ''
        );
        return;
    }

    // Redis 里每个字段都是字符串，row/before/table_key 需要 decode 一次
    $row = json_decode($fields['row'] ?? '{}', true) ?: [];
    $before = isset($fields['before']) && $fields['before'] !== ''
        ? json_decode($fields['before'], true)
        : null;

    printf(
        "[%s] %s.%s id=%s\n  row=%s\n",
        $fields['op'] ?? '',
        $fields['db'] ?? '',
        $fields['table'] ?? '',
        $fields['id'] ?? '',
        json_encode($row, JSON_UNESCAPED_UNICODE)
    );

    if ($before !== null) {
        printf("  before=%s\n", json_encode($before, JSON_UNESCAPED_UNICODE));
    }

    // labels 是 topic 级静态标签
    if (isset($fields['labels']) && $fields['labels'] !== '') {
        $labels = json_decode($fields['labels'], true) ?: [];
        if (($labels['env'] ?? '') === 'prod') {
            // 生产环境专属逻辑
        }
    }
}
