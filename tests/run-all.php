<?php
/**
 * 一次跑完全部测试（CI / 提交前自检）
 *
 *   php tests/run-all.php
 */

declare(strict_types=1);

$tests = [
    '点位 / 锁 / 过滤器' => __DIR__ . '/selftest.php',
    'EventBuilder 行展开' => __DIR__ . '/EventBuilderTest.php',
    'Handler 链路' => __DIR__ . '/HandlerTest.php',
    '队列消息与消费端' => __DIR__ . '/ConsumerTest.php',
    '列裁剪' => __DIR__ . '/ProjectorTest.php',
    '点位恢复策略' => __DIR__ . '/CheckpointResumeTest.php',
    '权限解析' => __DIR__ . '/GrantParseTest.php',
    'Redis API 契约' => __DIR__ . '/RedisApiTest.php',
    '自定义 Sink' => __DIR__ . '/CustomSinkTest.php',
    '脚本处理器' => __DIR__ . '/ScriptHandlerTest.php',
    '二进制读取器' => __DIR__ . '/BinaryReaderTest.php',
    '列类型解码' => __DIR__ . '/ColumnTypeTest.php',
    'GTID 集合' => __DIR__ . '/GtidSetTest.php',
    'DDL 识别' => __DIR__ . '/DdlChangeTest.php',
    '全量同步分页' => __DIR__ . '/FullSyncTest.php',
    '告警通知' => __DIR__ . '/AlertTest.php',

];

$failed = 0;
foreach ($tests as $name => $file) {
    echo str_repeat('=', 56) . "\n{$name}\n" . str_repeat('=', 56) . "\n";
    passthru(PHP_BINARY . ' ' . escapeshellarg($file), $code);
    if ($code !== 0) {
        $failed++;
    }
    echo "\n";
}

echo str_repeat('=', 56) . "\n";
echo $failed === 0 ? "全部测试通过\n" : "{$failed} 个测试文件失败\n";
exit($failed === 0 ? 0 : 1);
