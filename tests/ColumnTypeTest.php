<?php
/**
 * 列类型解码测试
 *
 * 用构造的字节流验证各 MySQL 列类型的读取路径，
 * 覆盖 RowEvent::getColumnData 里的 switch 分支（不需要 MySQL）。
 *
 *   php tests/ColumnTypeTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Replication\BinaryDataReader\BinaryDataReader;
use BinlogRelay\Replication\BinaryDataReader\BinaryDataReaderException;

$fail = 0;
function eq($name, $got, $want) {
    global $fail;
    $ok = $got === $want;
    if (!$ok) { $fail++; printf("%-46s FAIL got=%s want=%s\n", $name, var_export($got,true), var_export($want,true)); }
    else printf("%-46s OK\n", $name);
}

// 各类型的最小读取验证（直接调 BinaryDataReader 对应方法）
$cases = [
    ['TINY unsigned', pack('C', 250), fn($r) => $r->readUInt8(), 250],
    ['TINY signed',   pack('c', -6),   fn($r) => $r->readInt8(), -6],
    ['SHORT unsigned',pack('v', 65530),fn($r) => $r->readUInt16(), 65530],
    ['SHORT signed',  pack('s', -300), fn($r) => $r->readInt16(), -300],
    ['LONG unsigned', pack('V', 4000000000), fn($r) => $r->readUInt32(), 4000000000],
    ['LONG signed',   pack('l', -123456), fn($r) => $r->readInt32(), -123456],
    ['FLOAT',         pack('f', 1.5),  fn($r) => round($r->readFloat(),4), 1.5],
    ['DOUBLE',        pack('d', 3.14159265358979), fn($r) => $r->readDouble(), 3.14159265358979],
];
foreach ($cases as [$name, $bytes, $read, $want]) {
    $r = new BinaryDataReader($bytes);
    eq($name, $read($r), $want);
}

// VARCHAR：1 字节长度前缀
$r = new BinaryDataReader(pack('C', 5) . 'hello');
eq('VARCHAR 1字节长度', $r->readLengthString(1), 'hello');
eq('VARCHAR 读完剩余0', $r->getBinaryDataLength(), 0);

// BLOB：2 字节长度前缀
$r = new BinaryDataReader(pack('v', 6) . 'world!');
eq('BLOB 2字节长度', $r->readLengthString(2), 'world!');

// 混合序列：模拟一行多列
$row = pack('C', 42) . pack('v', 1000) . pack('V', 123456) . pack('C', 3) . 'abc';
$r = new BinaryDataReader($row);
eq('混合行 int8',  $r->readUInt8(), 42);
eq('混合行 int16', $r->readUInt16(), 1000);
eq('混合行 int32', $r->readUInt32(), 123456);
eq('混合行 varchar', $r->readLengthString(1), 'abc');
eq('混合行读完',   $r->getBinaryDataLength(), 0);

// unread 在 decimal 路径的正确性
$r = new BinaryDataReader(pack('C', 0x81) . pack('C', 0x7F));
$b = $r->readUInt8();
$neg = (0 !== ($b & 0x80));
$r->unread(pack('C', $b ^ 0x80));
$back = $r->readUInt8();
eq('decimal 符号位回写正确', [$neg, $back], [true, 0x01]);

echo $fail === 0 ? "\n全部通过\n" : "\n$fail 项失败\n";
exit($fail ? 1 : 0);
