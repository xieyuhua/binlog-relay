<?php
/**
 * 示例 5：审计日志（按表/操作过滤后追加到文件）
 *
 * 适合只关心"谁在什么时候改了哪些表"的合规场景。
 * 用 JSON Lines 输出，方便后续用 jq / 日志系统消费。
 *
 * 环境变量：
 *   AUDIT_FILE     输出文件，默认 ./audit.log
 *   AUDIT_TABLES   只记录这些表，逗号分隔，默认全部
 *   AUDIT_OPS      只记录这些操作，默认 delete（审计一般最关心删除）
 *
 * 启用：
 *   new BinlogRelay\Handler\ScriptHandler('/abs/path/to/05-filter-and-forward.php'),
 */

declare(strict_types=1);

$file = getenv('AUDIT_FILE') ?: (__DIR__ . '/audit.log');
$tables = array_filter(array_map('trim', explode(',', getenv('AUDIT_TABLES') ?: '')));
$ops = array_filter(array_map('trim', explode(',', getenv('AUDIT_OPS') ?: 'delete')));

$fh = fopen($file, 'ab');
if ($fh === false) {
    fwrite(STDERR, "[05] 无法打开审计文件：{$file}\n");
    while (($line = fgets(STDIN)) !== false) {
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => '审计文件不可写']) . "\n");
        fflush(STDOUT);
    }
    exit(1);
}

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }

    try {
        $changes = json_decode($line, true, 512, JSON_THROW_ON_ERROR);

        $out = '';
        foreach ($changes as $c) {
            $table = (string)($c['table'] ?? '');
            $op = (string)($c['op'] ?? '');

            if ($tables !== [] && !in_array($table, $tables, true)) {
                continue;
            }
            if ($ops !== [] && !in_array($op, $ops, true)) {
                continue;
            }

            $record = [
                'at'    => date('c'),
                'op'    => $op,
                'table' => $c['database'] . '.' . $table,
                'key'   => $c['table_key'] ?? [],
                'row'   => $c['row'] ?? [],
                'prev'  => $c['before'] ?? null,
                'pos'   => ($c['binlog_file'] ?? '') . ':' . ($c['binlog_pos'] ?? ''),
            ];
            $out .= json_encode($record, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . "\n";
        }

        if ($out !== '') {
            fwrite($fh, $out);
            fflush($fh);   // 审计场景宁可慢一点也要保证落盘
        }

        fwrite(STDOUT, json_encode(['ok' => true]) . "\n");
        fflush(STDOUT);
    } catch (Throwable $e) {
        fwrite(STDERR, '[05] ' . $e->getMessage() . PHP_EOL);
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => $e->getMessage()]) . "\n");
        fflush(STDOUT);
    }
}

fclose($fh);
