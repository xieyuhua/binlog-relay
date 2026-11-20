<?php

declare(strict_types=1);

namespace BinlogRelay\Console;

use BinlogRelay\Checkpoint\StoreInterface;
use PDO;
use Throwable;

/**
 * 部署自检：MySQL 连接 / binlog 配置 / 复制权限 / 点位存储
 *
 * relay 与 binlog2redis 共用。Redis 输出相关的检查由调用方补充。
 */
final class CheckCommand
{
    private bool $ok = true;

    public function __construct()
    {
    }

    /**
     * @param array<string,mixed> $config 需包含 mysql 段
     */
    public static function mysql(array $config): self
    {
        $self = new self();
        $mysql = $config['mysql'];

        try {
            $pdo = new PDO(
                sprintf('mysql:host=%s;port=%d;charset=utf8mb4', $mysql['host'], $mysql['port']),
                $mysql['user'],
                $mysql['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_TIMEOUT => 5]
            );
            echo "[OK] MySQL 连接成功\n";

            $vars = [];
            foreach ($pdo->query(
                "SHOW GLOBAL VARIABLES WHERE Variable_name IN
                 ('binlog_format','binlog_row_image','log_bin','binlog_row_metadata')"
            ) as $row) {
                $vars[$row['Variable_name']] = $row['Value'];
            }

            foreach (['log_bin' => 'ON', 'binlog_format' => 'ROW', 'binlog_row_image' => 'FULL'] as $k => $want) {
                $got = strtoupper((string)($vars[$k] ?? ''));
                echo ($got === $want ? '[OK] ' : '[FAIL] ') . "{$k}={$got}" . ($got === $want ? '' : "，应为 {$want}") . "\n";
                $self->ok = $self->ok && $got === $want;
            }

            if (isset($vars['binlog_row_metadata'])) {
                $meta = strtoupper((string)$vars['binlog_row_metadata']);
                echo ($meta === 'FULL' ? '[OK] ' : '[WARN] ') . "binlog_row_metadata={$meta}"
                    . ($meta === 'FULL' ? "\n" : "（建议 FULL，否则每张表首次出现要查 information_schema）\n");
            }

            $self->checkReplicationPrivilege($pdo);
        } catch (Throwable $e) {
            echo '[FAIL] MySQL：' . $e->getMessage() . "\n";
            $self->ok = false;
        }

        return $self;
    }

    /**
     * 检查复制权限
     *
     * 不用 SHOW GRANTS 文本匹配（会被 REVOKE 记录、GRANT OPTION 等误判），
     * 而是实际执行一次 SHOW MASTER STATUS —— 这正是 relay 启动时要做的事，
     * 缺 REPLICATION CLIENT 会直接报错，缺 REPLICATION SLAVE 则在建立
     * binlog dump 连接时才失败，所以这里再单独查一次权限表。
     *
     * MySQL 8.0+ 用 SHOW BINARY LOG STATUS / REPLICATION_APPLIER 等新名字，
     * 统一按前缀匹配 REPLICATION。
     */
    private function checkReplicationPrivilege(PDO $pdo): void
    {
        $slug = '';
        try {
            $row = $pdo->query('SHOW MASTER STATUS')->fetch(PDO::FETCH_ASSOC);
            if ($row !== false) {
                $slug = 'SHOW MASTER STATUS 可执行';
            }
        } catch (Throwable) {
            // MySQL 8.4 起改名为 SHOW BINARY LOG STATUS
            try {
                $pdo->query('SHOW BINARY LOG STATUS')->fetch(PDO::FETCH_ASSOC);
                $slug = 'SHOW BINARY LOG STATUS 可执行';
            } catch (Throwable) {
                $slug = '';
            }
        }

        // REPLICATION CLIENT 决定能否读取位点；REPLICATION SLAVE 决定能否开 dump
        [$hasClient, $hasSlave] = $this->parseReplicationGrants($pdo);

        echo ($hasClient ? '[OK] ' : '[FAIL] ') . 'REPLICATION CLIENT'
            . ($hasClient ? '' : '（缺少：无法读取 binlog 位点）') . "\n";
        echo ($hasSlave ? '[OK] ' : '[FAIL] ') . 'REPLICATION SLAVE'
            . ($hasSlave ? '' : '（缺少：无法建立 binlog 复制连接）') . "\n";

        $this->ok = $this->ok && $hasClient && $hasSlave;
    }

    /**
     * 从 SHOW GRANTS 解析复制的两类权限
     *
     * @return array{0:bool,1:bool} [REPLICATION CLIENT, REPLICATION SLAVE]
     */
    private function parseReplicationGrants(PDO $pdo): array
    {
        $client = false;
        $slave = false;

        foreach ($pdo->query('SHOW GRANTS')->fetchAll(PDO::FETCH_COLUMN) as $grant) {
            $isRevoke = (bool)preg_match('/^\s*REVOKE\b/i', $grant);
            if (!$isRevoke && !preg_match('/^\s*GRANT\b/i', $grant)) {
                continue;
            }
            // 取 GRANT/REVOKE 与 ON 之间的权限列表
            if (!preg_match('/^\s*(?:GRANT|REVOKE)\s+(.+?)\s+ON\s/i', $grant, $m)) {
                continue;
            }

            // 按逗号拆成单条权限再比对，避免 "REPLICATION CLIENT" 被当成含 "REPLICATION" 的兜底
            $privs = array_map(
                static fn(string $p): string => trim(preg_replace('/\s+/', ' ', strtoupper($p))),
                explode(',', $m[1])
            );

            $grantClient = $grantSlave = false;
            foreach ($privs as $privilege) {
                if ($privilege === 'ALL PRIVILEGES' || $privilege === 'ALL') {
                    $grantClient = $grantSlave = true;
                    break;
                }
                if ($privilege === 'REPLICATION CLIENT') {
                    $grantClient = true;
                }
                // MySQL 8.0.26+ 把 REPLICATION SLAVE 改名为 REPLICATION REPLICA
                if ($privilege === 'REPLICATION SLAVE' || $privilege === 'REPLICATION REPLICA') {
                    $grantSlave = true;
                }
            }

            // REVOKE 记录用于反向扣除（MySQL 8 的 SHOW GRANTS 会同时列出）
            if ($isRevoke) {
                $client = $client && !$grantClient;
                $slave = $slave && !$grantSlave;
            } else {
                $client = $client || $grantClient;
                $slave = $slave || $grantSlave;
            }
        }

        return [$client, $slave];
    }

    public function checkpoint(StoreInterface $store): self
    {
        try {
            $store->load();
            echo '[OK] 点位存储：' . $store->name() . "\n";
        } catch (Throwable $e) {
            echo '[FAIL] 点位存储：' . $e->getMessage() . "\n";
            $this->ok = false;
        }
        return $this;
    }

    public function fail(string $message): self
    {
        echo '[FAIL] ' . $message . "\n";
        $this->ok = false;
        return $this;
    }

    public function ok(string $message): self
    {
        echo '[OK] ' . $message . "\n";
        return $this;
    }

    public function warn(string $message): self
    {
        echo '[WARN] ' . $message . "\n";
        return $this;
    }

    public function result(): int
    {
        echo $this->ok ? "\n自检通过\n" : "\n自检未通过，请修复上面的 FAIL 项\n";
        return $this->ok ? 0 : 1;
    }
}
