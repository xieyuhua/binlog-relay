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
