<?php
/**
 * 示例 3：缓存失效（binlog → Redis DEL）
 *
 * 最典型的 binlog 用途：数据变更后删掉对应缓存，让下次读请求回源。
 * 相比业务代码里手动 del，这里能覆盖所有写入路径（含直接改库、批量脚本）。
 *
 * 环境变量：
 *   CACHE_REDIS_HOST  默认 127.0.0.1
 *   CACHE_REDIS_PORT  默认 6379
 *   CACHE_REDIS_PASS  默认空
 *   CACHE_PREFIX      默认 ''（key 前缀）
 *
 * 启用：
 *   new BinlogRelay\Handler\ScriptHandler('/abs/path/to/03-redis-cache-invalidate.php'),
 */

declare(strict_types=1);

$host = getenv('CACHE_REDIS_HOST') ?: '127.0.0.1';
$port = (int)(getenv('CACHE_REDIS_PORT') ?: 6379);
$pass = getenv('CACHE_REDIS_PASS') ?: '';
$prefix = getenv('CACHE_PREFIX') ?: '';

$redis = new Redis();
$redis->connect($host, $port, 2.0);
if ($pass !== '') {
    $redis->auth($pass);
}

/**
 * 生成该行可能对应的缓存 key
 *
 * 这里演示三种命名约定，实际按你的业务改：
 *   {prefix}{table}:{id}
 *   {prefix}{table}:detail:{id}
 *   {prefix}{table}:list
 *
 * @return list<string>
 */
function cacheKeys(string $table, array $row, array $keyCols, string $prefix): array
{
    if ($keyCols === []) {
        // 无主键：只能按表整体失效
        return [$prefix . $table . ':list'];
    }

    $id = implode(':', array_map(static fn($c) => (string)($row[$c] ?? ''), $keyCols));

    return [
        $prefix . $table . ':' . $id,
        $prefix . $table . ':detail:' . $id,
        $prefix . $table . ':list',
    ];
}

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }

    try {
        $changes = json_decode($line, true, 512, JSON_THROW_ON_ERROR);

        $keys = [];
        foreach ($changes as $c) {
            $table = (string)($c['table'] ?? '');
            if ($table === '') {
                continue;
            }
            // update 时 row 是 after；delete 时 row 就是被删的行
            foreach (cacheKeys($table, $c['row'] ?? [], $c['table_key'] ?? [], $prefix) as $k) {
                $keys[$k] = true;
            }
        }

        if ($keys !== []) {
            // 一次 DEL 多个 key，底层的 UNLINK 更省
            $list = array_keys($keys);
            foreach (array_chunk($list, 500) as $chunk) {
                $redis->del($chunk);
            }
        }

        fwrite(STDOUT, json_encode(['ok' => true]) . "\n");
        fflush(STDOUT);
    } catch (Throwable $e) {
        fwrite(STDERR, '[03] ' . $e->getMessage() . PHP_EOL);
        fwrite(STDOUT, json_encode(['ok' => false, 'error' => $e->getMessage()]) . "\n");
        fflush(STDOUT);
    }
}
