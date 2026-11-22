<?php
/**
 * 告警管理器测试：去重、限流、通道故障隔离
 *
 * 不发起真实网络请求（Notifier 用假的实现）。
 *
 *   php tests/AlertTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Alert\NotificationManager;
use BinlogRelay\Alert\NotifierInterface;
use BinlogRelay\Support\Logger;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

/** 记录调用次数的假通道 */
final class CountingNotifier implements NotifierInterface
{
    public int $count = 0;

    public function __construct(private readonly bool $fail = false)
    {
    }

    public function send(string $title, string $message, array $fields = []): void
    {
        $this->count++;
        if ($this->fail) {
            throw new RuntimeException('模拟通道故障');
        }
    }

    public function name(): string
    {
        return 'counting';
    }
}

$logger = new Logger('error');

// 1. 未配置通道时是空操作
$m = NotificationManager::fromConfig([], $logger);
check('未配置通道时 isEmpty', $m->isEmpty());
$m->alert('k', '标题', '消息');   // 不应抛异常
check('空管理器 alert 不抛异常', true);

// 2. 同 key 去重
$n = new CountingNotifier();
$m = new NotificationManager($logger, 300, 100);
$m->add($n);
$m->alert('same', '标题', '消息');
$m->alert('same', '标题', '消息');
$m->alert('same', '标题', '消息');
check('同 key 冷却期内只发一次', $n->count === 1, "实际 {$n->count}");

// 3. 不同 key 各自发送
$m->alert('other', '标题', '消息');
check('不同 key 分别发送', $n->count === 2, "实际 {$n->count}");

// 4. 限流：每分钟最多 N 条
$n2 = new CountingNotifier();
$m2 = new NotificationManager($logger, 0, 3);   // cooldown=0 让去重失效，只测限流
$m2->add($n2);
for ($i = 0; $i < 10; $i++) {
    $m2->alert('key' . $i, '标题', '消息');
}
check('超过每分钟上限被丢弃', $n2->count === 3, "实际 {$n2->count}");

// 5. 通道故障隔离：一个挂了不影响另一个
$bad = new CountingNotifier(true);
$good = new CountingNotifier();
$m3 = new NotificationManager($logger, 300, 100);
$m3->add($bad);
$m3->add($good);
$m3->alert('k', '标题', '消息');
check('故障通道不影响其它通道', $bad->count === 1 && $good->count === 1);

// 6. 配置解析
$m4 = NotificationManager::fromConfig(['wecom' => ['key' => 'abc']], $logger);
check('配置企微后非空', !$m4->isEmpty());
$m5 = NotificationManager::fromConfig(['dingtalk' => ['token' => 'xyz']], $logger);
check('配置钉钉后非空', !$m5->isEmpty());

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
