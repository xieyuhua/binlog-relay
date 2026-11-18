<?php

declare(strict_types=1);

namespace BinlogRelay\Event;

/**
 * 二级过滤器（库/表级别）
 *
 * php-mysql-replication 自身的表过滤在 TABLE_MAP 阶段生效；这里再兜一层，
 * 让 handler 只看业务关心的表，也方便同一实例复用不同规则。
 */
final class Filter
{
    /** @var list<string> */
    private array $databasesOnly;
    /** @var list<string> */
    private array $tablesOnly;
    /** @var list<string> */
    private array $databasesIgnore;
    /** @var list<string> */
    private array $tablesIgnore;

    public function __construct(array $databasesOnly = [], array $tablesOnly = [], array $databasesIgnore = [], array $tablesIgnore = [])
    {
        $this->databasesOnly = $databasesOnly;
        $this->tablesOnly = $tablesOnly;
        $this->databasesIgnore = $databasesIgnore;
        $this->tablesIgnore = $tablesIgnore;
    }

    public static function fromConfig(array $cfg): self
    {
        return new self(
            $cfg['databases'] ?? [],
            $cfg['tables'] ?? [],
            $cfg['ignore_databases'] ?? [],
            $cfg['ignore_tables'] ?? []
        );
    }

    public function allows(string $database, string $table): bool
    {
        if ($this->databasesIgnore !== [] && in_array($database, $this->databasesIgnore, true)) {
            return false;
        }
        if ($this->tablesIgnore !== [] && in_array($table, $this->tablesIgnore, true)) {
            return false;
        }
        if ($this->databasesOnly !== [] && !in_array($database, $this->databasesOnly, true)) {
            return false;
        }
        if ($this->tablesOnly !== [] && !in_array($table, $this->tablesOnly, true)) {
            return false;
        }
        return true;
    }
}
