<?php
/**
 * 示例 4：HTTP 推送到业务接口（带签名与幂等键）
 *
 * 与内置 HttpSink 的区别：
 * - 可按表路由到不同 URL
 * - 可自定义请求体格式（业务系统往往有自己的报文结构）
 * - 可带 HMAC 签名
 *
 * 环境变量：
 *   WEBHOOK_URL       推送地址
 *   WEBHOOK_SECRET    HMAC-SHA256 密钥，为空则不签名
 *   WEBHOOK_TIMEOUT   超时秒数，默认 5
 *
 * 启用：
 *   new BinlogRelay\Handler\ScriptHandler('/abs/path/to/04-webhook-push.php'),
 */

declare(strict_types=1);

$url = getenv('WEBHOOK_URL') ?: '';
$secret = getenv('WEBHOOK_SECRET') ?: '';
$timeout = (float)(getenv('WEBHOOK_TIMEOUT') ?: 5);

if ($url === '') {
    fwrite(STDERR, "[04] 未配置 WEBHOOK_URL，脚本无法工作\n");
    // 保持进程存活，让父进程收到明确错误而不是无响应
    while (($line = fgets(STDIN)) !== false) {
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => 'WEBHOOK_URL 未配置']) . "\n");
        fflush(STDOUT);
    }
    exit(1);
}

/**
 * @param list<array<string,mixed>> $events
 */
function push(string $url, array $events, string $secret, float $timeout): void
{
    $body = json_encode(['events' => $events], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($body === false) {
        throw new RuntimeException('请求体序列化失败');
    }

    $headers = ['Content-Type: application/json; charset=utf-8'];
    if ($secret !== '') {
        // 接收方用同样的算法校验，防止伪造请求
        $headers[] = 'X-Signature: ' . hash_hmac('sha256', $body, $secret);
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $body,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => (int)ceil($timeout),
        CURLOPT_CONNECTTIMEOUT => (int)ceil($timeout),
    ]);
    $resp = curl_exec($ch);
    $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    $err = $resp === false ? curl_error($ch) : null;
    curl_close($ch);

    if ($err !== null) {
        throw new RuntimeException('网络错误：' . $err);
    }
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException(sprintf('接口返回 %d：%s', $status, substr((string)$resp, 0, 200)));
    }
}

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }

    try {
        $changes = json_decode($line, true, 512, JSON_THROW_ON_ERROR);

        // 只推送关心的事件，减少下游压力
        $events = [];
        foreach ($changes as $c) {
            $table = $c['table'] ?? '';
            if (!in_array($table, ['orders', 'order_items'], true)) {
                continue;
            }
            $events[] = [
                'id'   => $c['pk'] ?? '',
                'op'   => $c['op'] ?? '',
                'pos'  => $c['binlog_file'] . ':' . $c['binlog_pos'],
                'ts'   => $c['timestamp'] ?? 0,
                'data' => $c['row'] ?? [],
                'prev' => $c['before'] ?? null,
            ];
        }

        if ($events !== []) {
            push($url, $events, $secret, $timeout);
        }

        fwrite(STDOUT, json_encode(['ok' => true]) . "\n");
        fflush(STDOUT);
    } catch (Throwable $e) {
        fwrite(STDERR, '[04] ' . $e->getMessage() . PHP_EOL);
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => $e->getMessage()]) . "\n");
        fflush(STDOUT);
    }
}
