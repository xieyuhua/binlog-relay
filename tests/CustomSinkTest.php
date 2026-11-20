<?php
/**
 * 自定义 Sink 测试
 *
 *   php tests/CustomSinkTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Handler\HandlerInterface;
use BinlogRelay\Queue\Message;
use BinlogRelay\Sink\CustomSink;
use BinlogRelay\Sink\MessageHandlerInterface;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

/** 收集 Change 的假 handler */
final class CollectChangeSink implements HandlerInterface
{
    public array $got = [];

    public function __construct(public readonly string $tag = '')
    {
    }

    public function handle(array $changes): void
    {
        foreach ($changes as $c) {
            $this->got[] = "{$c->op}:{$c->table}:{$c->row['id']}";
        }
    }
}

/** 收集 Message 的假 sink */
final class CollectMessageSink implements MessageHandlerInterface
{
    public array $ids = [];

    public function handle(array $messages): void
    {
        foreach ($messages as $m) {
            $this->ids[] = $m->id;
        }
    }
}

final class NotASink
{
}

function sampleMessages(): array
{
    $f1 = [
        'op' => 'insert', 'db' => 'demo', 'tbl' => 'orders', 'pk' => 'demo.orders:1',
        'pos' => 'mysql-bin.000001:100', 'ts' => '1795013415',
        'data' => '{"op":"insert","database":"demo","table":"orders","row":{"id":1,"status":"new"},'
            . '"before":null,"table_key":["id"],"gtid":"","server_id":1}',
    ];
    $f2 = [
        'op' => 'delete', 'db' => 'demo', 'tbl' => 'orders', 'pk' => 'demo.orders:2',
        'pos' => 'mysql-bin.000001:200', 'ts' => '1795013416',
        'data' => '{"op":"delete","database":"demo","table":"orders","row":{"id":2},'
            . '"before":null,"table_key":["id"],"gtid":"","server_id":1}',
    ];
    return [Message::fromEntry('1-0', $f1), Message::fromEntry('2-0', $f2)];
}

function inner(CustomSink $sink): object
{
    $r = new ReflectionProperty($sink, 'handler');
    $r->setAccessible(true);
    return $r->getValue($sink);
}

// HandlerInterface：收到 Change
$sink = new CustomSink(CollectChangeSink::class, ['tag-a']);
$sink->write(sampleMessages());
check(
    'HandlerInterface 收到 Change 列表',
    inner($sink)->got === ['insert:orders:1', 'delete:orders:2'],
    json_encode(inner($sink)->got)
);
check('构造函数参数被传递', inner($sink)->tag === 'tag-a');
check('name 包含类名', str_contains($sink->name(), 'CollectChangeSink'));

// MessageHandlerInterface：收到 Message
$sink2 = new CustomSink(CollectMessageSink::class);
$sink2->write(sampleMessages());
check(
    'MessageHandlerInterface 收到 Message 列表',
    inner($sink2)->ids === ['1-0', '2-0'],
    json_encode(inner($sink2)->ids)
);

// 空批次不调用
$sink3 = new CustomSink(CollectChangeSink::class);
$sink3->write([]);
check('空批次不调用 handler', inner($sink3)->got === []);

// 接口不符
try {
    new CustomSink(NotASink::class);
    check('拒绝未实现接口的类', false);
} catch (RuntimeException $e) {
    check('拒绝未实现接口的类', str_contains($e->getMessage(), 'MessageHandlerInterface'));
}

// 类不存在
try {
    new CustomSink('App\NoSuchSinkClass');
    check('拒绝不存在的类', false);
} catch (RuntimeException $e) {
    check('拒绝不存在的类', str_contains($e->getMessage(), '不存在'));
}

// Message::toChange 还原
$change = sampleMessages()[0]->toChange();
check('toChange 还原 op/table/row', $change->op === 'insert'
    && $change->table === 'orders'
    && $change->row === ['id' => 1, 'status' => 'new']);
check('toChange 拆分 pos', $change->binlogFile === 'mysql-bin.000001'
    && $change->binlogPos === '100');
check('toChange 还原 tableKey', $change->tableKey === ['id']);
check('Message::toChanges 批量转换', count(Message::toChanges(sampleMessages())) === 2);

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
