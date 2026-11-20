<?php
/**
 * SHOW GRANTS 权限解析测试
 *
 * 纯逻辑测试，不需要 MySQL 连接。
 *
 *   php tests/GrantParseTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../src/bootstrap.php';

use BinlogRelay\Console\CheckCommand;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

/** 通过反射调用私有解析方法 */
function parse(array $grants): array
{
    $cmd = new CheckCommand();
    $pdo = new class($grants) extends PDO {
        public function __construct(private array $grants) {}
        public function query(string $sql, ?int $mode = null, mixed ...$args): PDOStatement|false
        {
            return new class($this->grants) extends PDOStatement {
                public function __construct(private array $rows) {}
                public function fetchAll(int $mode = PDO::FETCH_DEFAULT, mixed ...$args): array
                {
                    return $this->rows;
                }
            };
        }
    };
    $m = new ReflectionMethod(CheckCommand::class, 'parseReplicationGrants');
    $m->setAccessible(true);
    return $m->invoke($cmd, $pdo);
}

$cases = [
    '标准复制账号' => [
        ["GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%'"], [true, true],
    ],
    '仅 REPLICATION CLIENT' => [
        ["GRANT REPLICATION CLIENT ON *.* TO 'repl'@'%'"], [true, false],
    ],
    '仅 REPLICATION SLAVE' => [
        ["GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'"], [false, true],
    ],
    'ALL PRIVILEGES' => [
        ["GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"], [true, true],
    ],
    'MySQL 8 REPLICA 语法' => [
        ["GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO 'repl'@'%'"], [true, true],
    ],
    '普通业务账号' => [
        ["GRANT SELECT, INSERT ON `shop`.* TO 'app'@'%'"], [false, false],
    ],
    '表名含 replication 不误判' => [
        ["GRANT SELECT ON `replication_log`.* TO 'app'@'%'"], [false, false],
    ],
    'REVOKE 扣除 SLAVE' => [
        [
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'r'@'%'",
            "REVOKE REPLICATION SLAVE ON *.* FROM 'r'@'%'",
        ],
        [true, false],
    ],
    '多条 GRANT 合并' => [
        [
            "GRANT SELECT ON `shop`.* TO 'r'@'%'",
            "GRANT REPLICATION CLIENT ON *.* TO 'r'@'%'",
            "GRANT REPLICATION SLAVE ON *.* TO 'r'@'%'",
        ],
        [true, true],
    ],
];

foreach ($cases as $name => [$grants, $expected]) {
    [$client, $slave] = parse($grants);
    check(
        $name,
        $client === $expected[0] && $slave === $expected[1],
        sprintf('CLIENT=%s SLAVE=%s，期望 CLIENT=%s SLAVE=%s',
            $client ? 'YES' : 'no', $slave ? 'YES' : 'no',
            $expected[0] ? 'YES' : 'no', $expected[1] ? 'YES' : 'no')
    );
}

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
