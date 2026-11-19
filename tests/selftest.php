<?php
/**
 * 零依赖自测：点位批量落盘、锁互斥、过滤器、handler 异常传播
 *
 *   php tests/selftest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';   // env() 由 composer autoload files 加载

use BinlogRelay\Checkpoint\{FileStore, PositionManager};
use BinlogRelay\Event\Change;
use BinlogRelay\Event\Filter;
use BinlogRelay\Handler\{HandlerChain, HandlerInterface};
use BinlogRelay\Support\Logger;
use BinlogRelay\Replication\BinLog\BinLogCurrent;

$failed = 0;
function check(string $name, bool $ok): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . "\n";
    if (!$ok) {
        $failed++;
    }
}

$path = __DIR__ . '/../var/test-cp.json';
@unlink($path);
$store = new FileStore($path);           // 默认不 fsync
$storeDurable = new FileStore($path . '.dur', true);   // 强制 fsync 也要能正常写入
$logger = new Logger('error');
$pm = new PositionManager($store, $logger, 3, 0.2);

// --- 锁互斥 ---
check('第一个实例拿到锁', $store->acquireLock('a', 5) === true);
$s2 = new FileStore($path);
check('第二个实例被拒绝', $s2->acquireLock('b', 5) === false);
$store->releaseLock('a');
check('释放后可再次获取', $s2->acquireLock('b', 5) === true);
$s2->releaseLock('b');

// --- 批量落盘 ---
$pm->lock();
$cur = new BinLogCurrent();
$cur->setBinFileName('bin.000001');
$cur->setBinLogPosition('100');
for ($i = 1; $i <= 4; $i++) {
    $pm->track($cur);
}
$saved = $store->load();
check('每 3 个事件落盘一次', $saved !== null && $saved->events === 3);
check('文件与位置正确', $saved->binlogFile === 'bin.000001' && $saved->binlogPos === '100');
$pm->flush();
check('flush 后累计计数为 4', $store->load()->events === 4);
$pm->unlock();

// --- fsync 模式仍可正常落盘 ---
$storeDurable->save(new BinlogRelay\Checkpoint\Checkpoint('bin.000002', '200', '', time(), 1, 'x'));
$durable = $storeDurable->load();
check('fsync 模式落盘正确', $durable !== null && $durable->binlogPos === '200');
@unlink($path . '.dur');
@unlink($path . '.dur.lock');

// --- 过滤器 ---
$f = Filter::fromConfig([
    'databases' => ['shop'],
    'tables' => [],
    'ignore_databases' => ['mysql'],
    'ignore_tables' => ['logs'],
]);
check('白名单库通过', $f->allows('shop', 'orders'));
check('忽略表拦截', !$f->allows('shop', 'logs'));
check('忽略库拦截', !$f->allows('mysql', 'user'));
check('白名单外的库拦截', !$f->allows('other', 't'));

// --- only_configured：未配置的表整表丢弃 ---
$fCfg = Filter::fromConfig([
    'tables' => ['orders'],
    'columns' => ['users' => ['id', 'name']],
    'only_configured' => true,
]);
check('only_configured 放行 tables 里的表', $fCfg->allows('shop', 'orders'));
check('only_configured 放行 columns 里的表', $fCfg->allows('shop', 'users'));
check('only_configured 丢弃未配置的表', !$fCfg->allows('shop', 'audit_log'));
check('resolvedTablesOnly 含并集', $fCfg->configuredTables() === ['orders', 'users'], json_encode($fCfg->configuredTables()));

// 忽略规则优先于 only_configured
$fIgnore = Filter::fromConfig([
    'tables' => ['orders', 'logs'],
    'ignore_tables' => ['logs'],
    'only_configured' => true,
]);
check('忽略规则优先', !$fIgnore->allows('shop', 'logs') && $fIgnore->allows('shop', 'orders'));

// 关闭时保持原语义：tables 白名单仍然生效
$fOpen = Filter::fromConfig(['tables' => ['orders']]);
check('关闭 only_configured 时 tables 白名单仍生效',
    $fOpen->allows('shop', 'orders') && !$fOpen->allows('shop', 'anything'));

// --- handler 异常必须冒泡 ---
$chain = new HandlerChain($logger);
$chain->build([new class implements HandlerInterface {
    public function handle(array $changes): void
    {
        throw new RuntimeException('boom');
    }
}]);
$thrown = false;
try {
    $chain->handle([new Change('insert', 'd', 't', [], null, [], 'f', '1', '', 0, 1)]);
} catch (RuntimeException $e) {
    $thrown = $e->getMessage() === 'boom';
}
check('handler 异常向上抛出', $thrown);

@unlink($path);
@unlink($path . '.lock');

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
