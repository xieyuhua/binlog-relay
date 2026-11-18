<?php
/**
 * 不依赖 MySQL/Redis 的微基准：验证框架自身开销
 *
 *   php tests/bench.php [iterations]
 *
 * 测量项：
 *   1. EventBuilder：RowsDTO → Change[]（含主键解析）
 *   2. JSON 序列化：handler 输出前的编码成本
 *   3. FileStore：点位原子写吞吐
 *   4. HandlerChain：链式调度开销（空 handler）
 *
 * 注意：这是框架开销基准，不含 binlog 网络解析；真实端到端吞吐受
 * 库本身（约 7–8k events/s 历史基准）与 handler 影响。
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use BinlogRelay\Checkpoint\FileStore;
use BinlogRelay\Checkpoint\PositionManager;
use BinlogRelay\Event\EventBuilder;
use BinlogRelay\Handler\HandlerChain;
use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Support\Logger;
use MySQLReplication\BinLog\BinLogCurrent;
use MySQLReplication\Definitions\{ConstEventType, ConstFieldType};
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\Event\EventInfo;
use MySQLReplication\Event\RowEvent\{ColumnDTO, ColumnDTOCollection, TableMap};
use MySQLReplication\Repository\FieldDTO;

$iterations = (int)($argv[1] ?? 20000);

function stat_line(string $name, float $seconds, int $ops): void
{
    $opsPerSec = $ops / max($seconds, 1e-9);
    printf("%-34s %9.0f 次/秒   %8.2f µs/次   （%d 次 / %.3f 秒）\n",
        $name, $opsPerSec, $seconds * 1e6 / max($ops, 1), $ops, $seconds);
}

/* ---------- 构造可复用的 DTO ---------- */
$columns = new ColumnDTOCollection();
foreach ([['id', 'PRI'], ['name', ''], ['amount', '']] as [$col, $key]) {
    $columns->add(new ColumnDTO(
        new FieldDTO($col, null, null, '', 'int unsigned', $key),
        ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0
    ));
}
$tableMap = new TableMap('demo', 'orders', '42', 3, $columns);
$current = new BinLogCurrent();
$current->setBinFileName('mysql-bin.000001');
$current->setBinLogPosition('1000');
$info = new EventInfo(1795013415, ConstEventType::WRITE_ROWS_EVENT_V2->value, 1, 64, '1000', 0, false, $current);

// 每批 10 行，模拟中等事务
$rows = [];
for ($i = 1; $i <= 10; $i++) {
    $rows[] = ['id' => $i, 'name' => '顾客' . $i, 'amount' => $i * 10];
}
$dto = new WriteRowsDTO($info, $tableMap, count($rows), $rows);

echo "PHP " . PHP_VERSION . " · 每批 10 行 · 迭代 {$iterations}\n\n";

/* ---------- 1. EventBuilder ---------- */
$t = hrtime(true);
$count = 0;
for ($i = 0; $i < $iterations; $i++) {
    $changes = EventBuilder::fromRowsDTO($dto);
    $count += count($changes);
}
stat_line('EventBuilder（行展开）', (hrtime(true) - $t) / 1e9, $iterations);
echo "                                 （等效 " . round($count / max($iterations, 1)) . " 行/批）\n";

/* ---------- 2. JSON 序列化 ---------- */
$t = hrtime(true);
for ($i = 0; $i < $iterations; $i++) {
    foreach ($changes as $c) {
        json_encode($c->toArray(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
}
stat_line('JSON 序列化', (hrtime(true) - $t) / 1e9, $iterations * count($changes));

/* ---------- 3. FileStore 点位写 ---------- */
$path = __DIR__ . '/../var/bench-cp.json';
@unlink($path);
$store = new FileStore($path);
$pm = new PositionManager($store, new Logger('error'), 1, 0.001);   // 每个事件都落盘
$n = min($iterations, 2000);   // 写盘较慢，限次
$t = hrtime(true);
for ($i = 0; $i < $n; $i++) {
    $current->setBinLogPosition((string)(1000 + $i));
    $pm->track($current);
}
$pm->flush();
stat_line('FileStore 点位落盘（fsync+rename）', (hrtime(true) - $t) / 1e9, $n);
@unlink($path);
@unlink($path . '.lock');

/* ---------- 4. HandlerChain 调度 ---------- */
$chain = new HandlerChain(new Logger('error'));
$chain->build([new class implements HandlerInterface {
    public function handle(array $changes): void { /* 空实现，只测调度 */ }
}]);
$t = hrtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $chain->handle($changes);
}
stat_line('HandlerChain 调度（1 个空 handler）', (hrtime(true) - $t) / 1e9, $iterations);

echo "\n说明：以上为纯框架开销；端到端吞吐还取决于 binlog 解析与 handler 内 IO。\n";
