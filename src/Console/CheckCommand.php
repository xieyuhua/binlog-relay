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

            $hasRepl = false;
            foreach ($pdo->query('SHOW GRANTS')->fetchAll(PDO::FETCH_COLUMN) as $grant) {
                if (stripos($grant, 'REPLICATION') !== false) {
                    $hasRepl = true;
                }
            }
            echo ($hasRepl ? '[OK] ' : '[FAIL] ') . "复制权限\n";
            $self->ok = $self->ok && $hasRepl;
        } catch (Throwable $e) {
            echo '[FAIL] MySQL：' . $e->getMessage() . "\n";
            $self->ok = false;
        }

        return $self;
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
