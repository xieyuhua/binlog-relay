<?php
/**
 * EventBuilder 单元测试（不依赖 MySQL）
 *
 * 用真实 DTO 对象构造多行 insert/update/delete，验证：
 * - 一个 RowsDTO 内的多行被正确展开
 * - update 的 before/after 不串位
 * - 主键列名识别
 * - 二进制/非 UTF-8 值降级为 base64 包装
 * - GTID 未初始化时不抛错（v9 的坑）
 *
 *   php tests/EventBuilderTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Event\Change;
use BinlogRelay\Event\EventBuilder;
use BinlogRelay\Replication\BinLog\BinLogCurrent;
use BinlogRelay\Replication\Definitions\ConstEventType;
use BinlogRelay\Replication\Definitions\ConstFieldType;
use BinlogRelay\Replication\Event\DTO\DeleteRowsDTO;
use BinlogRelay\Replication\Event\DTO\UpdateRowsDTO;
use BinlogRelay\Replication\Event\DTO\WriteRowsDTO;
use BinlogRelay\Replication\Event\EventInfo;
use BinlogRelay\Replication\Event\RowEvent\ColumnDTO;
use BinlogRelay\Replication\Event\RowEvent\ColumnDTOCollection;
use BinlogRelay\Replication\Event\RowEvent\TableMap;
use BinlogRelay\Replication\Repository\FieldDTO;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

/** 构造带主键 id、以及一个 BLOB 列的 TableMap */
function makeTableMap(): TableMap
{
    $columns = new ColumnDTOCollection();
    $columns->add(new ColumnDTO(
        new FieldDTO('id', null, null, '', 'int unsigned', 'PRI'),
        BinlogRelay\Replication\Definitions\ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0
    ));
    $columns->add(new ColumnDTO(
        new FieldDTO('name', null, null, '', 'varchar(64)', ''),
        BinlogRelay\Replication\Definitions\ConstFieldType::VARCHAR, 0, 0, 0, 0, 0, 0, 0, 0
    ));
    $columns->add(new ColumnDTO(
        new FieldDTO('payload', null, null, '', 'blob', ''),
        BinlogRelay\Replication\Definitions\ConstFieldType::BLOB, 0, 0, 0, 0, 0, 0, 0, 0
    ));
    return new TableMap('demo', 'orders', '42', 3, $columns);
}

function makeInfo(int $type, string $pos): EventInfo
{
    $current = new BinLogCurrent();
    $current->setBinFileName('mysql-bin.000007');
    // gtid 故意不设置：模拟 v9 未启用 GTID 时的未初始化属性
    return new EventInfo(1795013415, $type, 1, 64, $pos, 0, false, $current);
}

// --- insert 多行 ---
$dto = new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2->value, '1000'), makeTableMap(), 2, [
    ['id' => 1, 'name' => '甲'],
    ['id' => 2, 'name' => '乙'],
]);
$changes = EventBuilder::fromRowsDTO($dto);
check('insert 两行展开为两条 Change', count($changes) === 2);
check('insert op 正确', $changes[0]->op === Change::INSERT);
check('insert 取到主键列', $changes[0]->tableKey === ['id'], json_encode($changes[0]->tableKey));
check('insert row 原样保留', $changes[1]->row === ['id' => 2, 'name' => '乙']);
check('insert before 为 null', $changes[0]->before === null);
check('binlog 位置透传', $changes[0]->binlogPos === '1000' && $changes[0]->binlogFile === 'mysql-bin.000007');
check('GTID 未初始化时返回空串', $changes[0]->gtid === '');
check('rowIdentity 可用作幂等键', $changes[0]->rowIdentity() === 'demo.orders:1', $changes[0]->rowIdentity());
check('有主键时 hasKey 为 true', $changes[0]->hasKey());

// --- 联合主键 ---
$composite = new TableMap('demo', 'order_items', '43', 3, (function () {
    $c = new ColumnDTOCollection();
    $c->add(new ColumnDTO(new FieldDTO('order_id', null, null, '', 'int', 'PRI'), ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0));
    $c->add(new ColumnDTO(new FieldDTO('item_id', null, null, '', 'int', 'PRI'), ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0));
    $c->add(new ColumnDTO(new FieldDTO('qty', null, null, '', 'int', ''), ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0));
    return new ColumnDTOCollection($c->toArray());
})());
$dto = new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2->value, '2500'), $composite, 1, [
    ['order_id' => 5, 'item_id' => 9, 'qty' => 2],
]);
$changes = EventBuilder::fromRowsDTO($dto);
check('联合主键两列都识别', $changes[0]->tableKey === ['order_id', 'item_id'], json_encode($changes[0]->tableKey));
check('联合主键 pk 用逗号连接', $changes[0]->rowIdentity() === 'demo.order_items:5,9', $changes[0]->rowIdentity());

// --- 仅唯一键 ---
$uniq = new TableMap('demo', 'users', '44', 2, (function () {
    $c = new ColumnDTOCollection();
    $c->add(new ColumnDTO(new FieldDTO('id', null, null, '', 'int', ''), ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0));
    $c->add(new ColumnDTO(new FieldDTO('code', null, null, '', 'varchar(20)', 'UNI'), ConstFieldType::VARCHAR, 0, 0, 0, 0, 0, 0, 0, 0));
    return new ColumnDTOCollection($c->toArray());
})());
$dto = new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2->value, '2600'), $uniq, 1, [
    ['id' => 1, 'code' => 'C1'],
]);
$changes = EventBuilder::fromRowsDTO($dto);
check('无主键时回退到唯一键', $changes[0]->tableKey === ['code'], json_encode($changes[0]->tableKey));
check('唯一键可作幂等键', $changes[0]->rowIdentity() === 'demo.users:C1', $changes[0]->rowIdentity());

// --- 无主键无唯一键 ---
$nokey = new TableMap('demo', 'logs', '45', 2, (function () {
    $c = new ColumnDTOCollection();
    $c->add(new ColumnDTO(new FieldDTO('x', null, null, '', 'int', ''), ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0));
    $c->add(new ColumnDTO(new FieldDTO('y', null, null, '', 'int', ''), ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0));
    return new ColumnDTOCollection($c->toArray());
})());
$warned = null;
EventBuilder::onMissingKey(function (string $msg) use (&$warned) { $warned = $msg; });
$dtoA = new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2->value, '2700'), $nokey, 2, [
    ['x' => 1, 'y' => 2],
    ['x' => 3, 'y' => 4],
]);
$changes = EventBuilder::fromRowsDTO($dtoA);
check('无键表 tableKey 为空', $changes[0]->tableKey === []);
check('无键表 hasKey 为 false', !$changes[0]->hasKey());
check('无键表 pk 退化为行哈希', str_starts_with($changes[0]->rowIdentity(), 'demo.logs:#'), $changes[0]->rowIdentity());
check('不同行生成不同 pk', $changes[0]->rowIdentity() !== $changes[1]->rowIdentity());
check('无键表触发一次告警', is_string($warned) && str_contains($warned, 'demo.logs'), (string)$warned);

$warnCount = 0;
EventBuilder::onMissingKey(function () use (&$warnCount) { $warnCount++; });
// 同一张表再出现，不应重复告警
EventBuilder::fromRowsDTO(new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2::class ? ConstEventType::WRITE_ROWS_EVENT_V2->value : 0, '2800'), $nokey, 1, [['x' => 9, 'y' => 9]]));
check('同一无键表只告警一次', $warnCount === 0, (string)$warnCount);

// delete 用 before 计算时与 insert 一致
$delChanges = EventBuilder::fromRowsDTO(new DeleteRowsDTO(
    makeInfo(ConstEventType::DELETE_ROWS_EVENT_V2->value, '2900'), $nokey, 1, [['x' => 1, 'y' => 2]]
));
check('无键表 insert 与 delete 的 pk 一致', $delChanges[0]->rowIdentity() === 'demo.logs:#' . substr(sha1(json_encode(['x'=>1,'y'=>2], JSON_UNESCAPED_UNICODE)), 0, 16));

// --- update before/after ---
$dto = new UpdateRowsDTO(makeInfo(ConstEventType::UPDATE_ROWS_EVENT_V2->value, '2000'), makeTableMap(), 1, [
    ['before' => ['id' => 1, 'name' => '甲'], 'after' => ['id' => 1, 'name' => '甲改']],
]);
$changes = EventBuilder::fromRowsDTO($dto);
check('update 展开一条', count($changes) === 1);
check('update row 是 after', $changes[0]->row['name'] === '甲改');
check('update before 不串位', $changes[0]->before['name'] === '甲');

// --- delete ---
$dto = new DeleteRowsDTO(makeInfo(ConstEventType::DELETE_ROWS_EVENT_V2->value, '3000'), makeTableMap(), 1, [
    ['id' => 9, 'name' => '丙'],
]);
$changes = EventBuilder::fromRowsDTO($dto);
check('delete op 正确', $changes[0]->op === Change::DELETE);
check('delete 无 before', $changes[0]->before === null);

// --- 非 UTF-8 值降级 ---
$dto = new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2->value, '4000'), makeTableMap(), 1, [
    ['id' => 3, 'payload' => "\xFF\xFE\x00binary"],
]);
$changes = EventBuilder::fromRowsDTO($dto);
$payload = $changes[0]->row['payload'];
check('二进制列包成 __binary__', is_array($payload) && isset($payload['__binary__']), json_encode($payload));
check('二进制可 JSON 序列化', json_encode($changes[0]->toArray()) !== false);

// --- 时间列 ---
$dto = new WriteRowsDTO(makeInfo(ConstEventType::WRITE_ROWS_EVENT_V2->value, '5000'), makeTableMap(), 1, [
    ['id' => 4, 'created' => new DateTime('2026-11-18 22:00:00')],
]);
$changes = EventBuilder::fromRowsDTO($dto);
check('DateTime 转字符串', $changes[0]->row['created'] === '2026-11-18 22:00:00');

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
