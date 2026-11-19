<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

use PDO;
use PDOException;
use RuntimeException;

/**
 * PDO 连接封装（替代 doctrine/dbal 的 Connection）
 *
 * 解析库只用到 5 个操作：
 *   - fetchAllAssociative(sql, params)
 *   - fetchAssociative(sql)
 *   - executeQuery(sql)
 *   - getDatabasePlatform()->getDummySelectSQL()
 *   - close()
 *
 * 这里用 PDO 实现同样语义，不引入 DBAL。
 */
final class PdoConnection
{
    private ?PDO $pdo;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly string $user,
        private readonly string $password,
        private readonly string $charset = 'utf8mb4',
    ) {
        $this->pdo = $this->connect();
    }

    private function connect(): PDO
    {
        $dsn = sprintf('mysql:host=%s;port=%d;charset=%s', $this->host, $this->port, $this->charset);
        try {
            return new PDO($dsn, $this->user, $this->password, [
                PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES   => false,
                PDO::ATTR_TIMEOUT            => 5,
            ]);
        } catch (PDOException $e) {
            throw new RuntimeException('MySQL 连接失败：' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /** @return list<array<string,mixed>> */
    public function fetchAllAssociative(string $sql, array $params = []): array
    {
        $stmt = $this->run($sql, $params);
        return $stmt->fetchAll() ?: [];
    }

    /** @return array<string,mixed>|false */
    public function fetchAssociative(string $sql, array $params = []): array|false
    {
        $stmt = $this->run($sql, $params);
        return $stmt->fetch();
    }

    /** 返回首行首列 */
    public function fetchOne(string $sql, array $params = []): mixed
    {
        $stmt = $this->run($sql, $params);
        return $stmt->fetchColumn();
    }

    public function executeQuery(string $sql, array $params = []): void
    {
        $this->run($sql, $params);
    }

    /** 等价于 DBAL 的 getDatabasePlatform()->getDummySelectSQL() */
    public function getDummySelectSQL(): string
    {
        return 'SELECT 1';
    }

    /** 连接探活；断开时重连 */
    public function ping(): bool
    {
        try {
            $this->pdo?->query('SELECT 1');
            return true;
        } catch (\Throwable) {
            $this->pdo = $this->connect();
            return true;
        }
    }

    public function close(): void
    {
        $this->pdo = null;
    }

    public function pdo(): ?PDO
    {
        return $this->pdo;
    }

    private function run(string $sql, array $params = []): \PDOStatement
    {
        if ($this->pdo === null) {
            $this->pdo = $this->connect();
        }
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt;
    }
}
