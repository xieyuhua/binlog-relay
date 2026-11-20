<?php
/**
 * 示例 1：最小可用的处理脚本
 *
 * 把收到的变更格式化后打到 **stderr**（不是 stdout！）。
 *
 * 重要：stdout 是协议通道，只能回 {"ok":...} 这类响应，
 * 任何业务输出都必须走 stderr，否则会破坏通信。
 *
 * 启用：
 *   new BinlogRelay\Handler\ScriptHandler('/abs/path/to/01-stdout-echo.php'),
 */

declare(strict_types=1);

$total = 0;

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }

    try {
        $changes = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($changes)) {
            throw new RuntimeException('输入不是 JSON 数组');
        }

        foreach ($changes as $c) {
            $total++;
            // 业务输出必须走 STDERR，stdout 留给协议响应
            fprintf(
                STDERR,
                "[%d] %-6s %s.%s %s\n",
                $total,
                $c['op'] ?? '?',
                $c['database'] ?? '',
                $c['table'] ?? '',
                json_encode($c['row'] ?? [], JSON_UNESCAPED_UNICODE)
            );
        }
        fflush(STDERR);

        // 成功必须回一行
        fwrite(STDOUT, json_encode(['ok' => true]) . "\n");
        fflush(STDOUT);
    } catch (Throwable $e) {
        fwrite(STDERR, '[01] ' . $e->getMessage() . PHP_EOL);
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => $e->getMessage()]) . "\n");
        fflush(STDOUT);
    }
}
