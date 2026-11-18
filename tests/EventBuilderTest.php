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

require __DIR__ . '/../vendor/autoload.php';

use BinlogRelay\Event\Change;
use BinlogRelay\Event\EventBuilder;
use MySQLReplication\BinLog\BinLogCurrent;
use MySQLReplication\Definitions\ConstEventType;
use MySQLReplication\Event\DTO\DeleteRowsDTO;
use MySQLReplication\Event\DTO\UpdateRowsDTO;
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\Event\EventInfo;
use MySQLReplication\Event\RowEvent\ColumnDTO;
use MySQLReplication\Event\RowEvent\ColumnDTOCollection;
use MySQLReplication\Event\RowEvent\TableMap;
use MySQLReplication\Repository\FieldDTO;

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
        MySQLReplication\Definitions\ConstFieldType::LONG, 0, 0, 0, 0, 0, 0, 0, 0
    ));
    $columns->add(new ColumnDTO(
        new FieldDTO('name', null, null, '', 'varchar(64)', ''),
        MySQLReplication\Definitions\ConstFieldType::VARCHAR, 0, 0, 0, 0, 0, 0, 0, 0
    ));
    $columns->add(new ColumnDTO(
        new FieldDTO('payload', null, null, '', 'blob', ''),
        MySQLReplication\Definitions\ConstFieldType::BLOB, 0, 0, 0, 0, 0, 0, 0, 0
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
