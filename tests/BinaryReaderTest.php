<?php
/**
 * BinaryDataReader 语义测试
 *
 * 重点覆盖 unread（RowEvent 的 decimal 有符号处理会用到），
 * 以及偏移量重构后的边界行为。
 *
 *   php tests/BinaryReaderTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Replication\BinaryDataReader\BinaryDataReader;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

// --- 顺序读取 ---
$r = new BinaryDataReader(pack('V', 1) . pack('V', 2) . pack('V', 3));
check('顺序读 3 个 int32', [$r->readInt32(), $r->readInt32(), $r->readInt32()] === [1, 2, 3]);

// --- 剩余长度 ---
$r = new BinaryDataReader(str_repeat('A', 10));
$r->read(3);
check('剩余长度 7', $r->getBinaryDataLength() === 7);
check('剩余数据长度 7', strlen($r->getBinaryData()) === 7);
check('剩余内容正确', $r->getBinaryData() === str_repeat('A', 7));

// --- unread：decimal 符号位变换（RowEvent 真实用法）---
$r = new BinaryDataReader(pack('C', 0x85) . 'XYZ');
$v = $r->readUInt8();
check('读到 0x85', $v === 0x85);
$r->unread(pack('C', $v ^ 0x80));
check('unread 后读到 0x05', $r->readUInt8() === 0x05);
$r->unread(pack('C', 0x05));
$rest = $r->read(3);
check('连续 unread 不丢字节', bin2hex($rest) === bin2hex(pack('C', 5) . 'XY'), bin2hex($rest));
check('连续 unread 后剩余 1', $r->getBinaryDataLength() === 1);
check('其余数据仍是 Z', $r->read(1) === 'Z');

// --- unread 替换已读部分（与原实现语义一致）---
$r = new BinaryDataReader('ABCDEF');
$r->read(2);
$r->unread('XY');
check('unread 替换已读部分', $r->read(4) === 'XYCD');

// --- advance ---
$r = new BinaryDataReader('ABCDEF');
$r->advance(2);
check('advance(2)', $r->read(4) === 'CDEF');

// --- readBytes 统计 ---
$r = new BinaryDataReader(str_repeat('Z', 20));
$r->read(5);
$r->advance(3);
check('getReadBytes 累计 8', $r->getReadBytes() === 8);

// --- 边界 ---
$r = new BinaryDataReader('AB');
check('读超长返回可用部分', $r->read(10) === 'AB');
check('读超长后剩余 0', $r->getBinaryDataLength() === 0);

// --- 各类数值类型 ---
$r = new BinaryDataReader(pack('C', 200) . pack('v', 60000) . pack('V', 4000000000));
check('readUInt8', $r->readUInt8() === 200);
check('readUInt16', $r->readUInt16() === 60000);
check('readUInt32', $r->readUInt32() === 4000000000);

// --- 有符号 ---
$r = new BinaryDataReader(pack('c', -5) . pack('s', -1000) . pack('l', -100000));
check('readInt8', $r->readInt8() === -5);
check('readInt16', $r->readInt16() === -1000);
check('readInt32', $r->readInt32() === -100000);

// --- 长度前缀字符串 ---
$r = new BinaryDataReader(pack('C', 3) . 'abc');
check('readLengthString(1)', $r->readLengthString(1) === 'abc');

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
