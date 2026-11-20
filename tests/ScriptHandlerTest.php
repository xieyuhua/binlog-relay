<?php
/**
 * ScriptHandler 与示例脚本测试
 *
 * 启动真实子进程，验证协议：stdin 收 JSON 数组、stdout 回 {"ok":true}。
 * 不依赖 MySQL/Redis 的示例（01、05）会做断言；依赖外部服务的只验证能启动。
 *
 *   php tests/ScriptHandlerTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Event\Change;
use BinlogRelay\Handler\ScriptHandler;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

$scripts = __DIR__ . '/../examples/scripts';

function makeChanges(): array
{
    return [
        new Change('insert', 'demo', 'orders', ['id' => 1, 'status' => 'new', 'amount' => 99.5],
            null, ['id'], 'mysql-bin.000001', '100', '', 1795013415, 1),
        new Change('update', 'demo', 'orders', ['id' => 1, 'status' => 'paid', 'amount' => 99.5],
            ['id' => 1, 'status' => 'new', 'amount' => 99.5], ['id'], 'mysql-bin.000001', '200', '', 1795013416, 1),
        new Change('delete', 'demo', 'orders', ['id' => 2, 'status' => 'x', 'amount' => 1],
            null, ['id'], 'mysql-bin.000001', '300', '', 1795013417, 1),
    ];
}

// ---- 协议基础：脚本不存在时立即报错 ----
try {
    new ScriptHandler('/nonexistent/script.php');
    check('不存在的脚本应抛异常', false);
} catch (RuntimeException $e) {
    check('不存在的脚本立即抛异常', str_contains($e->getMessage(), '脚本不存在'));
}

// ---- 01：最小示例，不依赖外部服务 ----
$out = tempnam(sys_get_temp_dir(), 'sh01');
$handler = new ScriptHandler("{$scripts}/01-stdout-echo.php");
$ok1 = false;
try {
    $handler->handle(makeChanges());
    $ok1 = true;
} catch (Throwable $e) {
    $ok1 = false;
}
check('01-stdout-echo 处理成功', $ok1);
unset($handler);
@unlink($out);

// ---- 05：审计日志，只保留 delete ----
$auditFile = sys_get_temp_dir() . '/relay-audit-' . getmypid() . '.log';
@unlink($auditFile);
putenv("AUDIT_FILE={$auditFile}");
putenv('AUDIT_OPS=delete');

$handler = new ScriptHandler("{$scripts}/05-filter-and-forward.php");
try {
    $handler->handle(makeChanges());
    $handler->handle([
        new Change('insert', 'demo', 'other', ['id' => 9], null, ['id'], 'b', '400', '', 1, 1),
    ]);
    $lines = is_file($auditFile) ? file($auditFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) : [];
    check('05 只写入了 delete 事件', count($lines) === 1, '实际 ' . count($lines) . ' 行');

    $record = $lines === [] ? [] : json_decode($lines[0], true);
    check('05 记录含 op/table/pos', ($record['op'] ?? '') === 'delete'
        && ($record['table'] ?? '') === 'demo.orders'
        && ($record['pos'] ?? '') === 'mysql-bin.000001:300');
} catch (Throwable $e) {
    check('05 处理成功', false, $e->getMessage());
}
unset($handler);
@unlink($auditFile);
putenv('AUDIT_FILE');
putenv('AUDIT_OPS');

// ---- 脚本抛异常时应把 ok=false 传回来 ----
$badScript = sys_get_temp_dir() . '/relay-bad-' . getmypid() . '.php';
file_put_contents($badScript, <<<'PHP'
<?php
while (($line = fgets(STDIN)) !== false) {
    fwrite(STDOUT, json_encode(['ok' => false, 'error' => '模拟业务失败']) . "\n");
    fflush(STDOUT);
}
PHP);

$handler = new ScriptHandler($badScript);
try {
    $handler->handle(makeChanges());
    check('脚本返回 ok=false 时抛出异常', false);
} catch (RuntimeException $e) {
    check('脚本返回 ok=false 时抛出异常', str_contains($e->getMessage(), '模拟业务失败'), $e->getMessage());
}
unset($handler);
@unlink($badScript);

// ---- 脚本无响应时应超时而不是永久挂起 ----
// 只在 Linux 验证：Windows 的 proc_open 管道既不支持非阻塞也不支持超时，
// fwrite/fgets 在满管道/EOF 上会阻塞十几秒，无法测出真实行为。
if (PHP_OS_FAMILY === 'Windows') {
    echo "[SKIP] Windows 管道无超时能力，跳过（生产环境请用 Linux）\n";
} else {
    $hangScript = sys_get_temp_dir() . '/relay-hang-' . getmypid() . '.php';
    file_put_contents($hangScript, "<?php\nwhile (true) { usleep(100000); }\n");

    $handler = new ScriptHandler($hangScript, timeoutSeconds: 1.0);
    $start = microtime(true);
    try {
        $handler->handle(makeChanges());
        check('无响应脚本应超时', false);
    } catch (RuntimeException $e) {
        $elapsed = microtime(true) - $start;
        check('无响应脚本在 ~1 秒后超时', $elapsed < 5.0, sprintf('耗时 %.1f 秒', $elapsed));
    }
    unset($handler);
    @unlink($hangScript);
}

// ---- 依赖外部服务的脚本：只验证能启动并给出明确错误 ----
foreach (['03-redis-cache-invalidate.php'] as $name) {
    $handler = new ScriptHandler("{$scripts}/{$name}");
    $start = microtime(true);
    try {
        $handler->handle(makeChanges());
        check("{$name} 可执行", true);
    } catch (Throwable $e) {
        // 本机无 Redis 时脚本启动就失败，属于预期
        $elapsed = microtime(true) - $start;
        check("{$name} 无外部依赖时快速失败（不挂起）", $elapsed < 5.0, $e->getMessage());
    }
    unset($handler);
}

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
