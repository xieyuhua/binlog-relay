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

    /** 只处理「已配置」的表（出现在 tables / columns 配置里），其余整表丢弃 */
    private bool $onlyConfigured;

    /** @var array<string,true> 已配置表名，用于 only_configured 判定 */
    private array $configured;

    public function __construct(
        array $databasesOnly = [],
        array $tablesOnly = [],
        array $databasesIgnore = [],
        array $tablesIgnore = [],
        bool $onlyConfigured = false,
        array $configuredTables = []
    ) {
        $this->databasesOnly = $databasesOnly;
        $this->tablesOnly = $tablesOnly;
        $this->databasesIgnore = $databasesIgnore;
        $this->tablesIgnore = $tablesIgnore;
        $this->onlyConfigured = $onlyConfigured;
        $this->configured = [];
        foreach ($configuredTables as $t) {
            $this->configured[(string)$t] = true;
        }
    }

    public static function fromConfig(array $cfg): self
    {
        $tablesOnly = $cfg['tables'] ?? [];
        $columnTables = array_keys((array)($cfg['columns'] ?? []));

        // only_configured：把 tables 与 columns 里出现过的表名视为"已配置"
        $configured = array_values(array_filter(
            array_merge($tablesOnly, $columnTables),
            static fn($t) => $t !== '*' && $t !== ''
        ));

        return new self(
            $cfg['databases'] ?? [],
            $tablesOnly,
            $cfg['ignore_databases'] ?? [],
            $cfg['ignore_tables'] ?? [],
            (bool)($cfg['only_configured'] ?? false),
            $configured
        );
    }

    /** @var array<string,bool> 判定结果缓存，键为 "db\0table"；表数量有限，无需淘汰 */
    private array $cache = [];

    public function allows(string $database, string $table): bool
    {
        // 同一张表的连续事件非常多，缓存判定结果避免重复线性查找
        $key = $database . "\0" . $table;
        if (isset($this->cache[$key])) {
            return $this->cache[$key];
        }
        return $this->cache[$key] = $this->decide($database, $table);
    }

    private function decide(string $database, string $table): bool
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

        // only_configured 时，白名单取 tables ∪ columns 的表名并集，
        // 这样"只在 columns 里配了裁剪规则"的表不会被 tables 白名单误杀
        if ($this->onlyConfigured) {
            return isset($this->configured[$table]);
        }

        if ($this->tablesOnly !== [] && !in_array($table, $this->tablesOnly, true)) {
            return false;
        }
        return true;
    }

    /** 供 Relay 在 TABLE_MAP 阶段下推过滤：返回真正要订阅的表清单 */
    public function resolvedTablesOnly(): array
    {
        return $this->tablesOnly;
    }

    public function isOnlyConfigured(): bool
    {
        return $this->onlyConfigured;
    }

    /** @return list<string> */
    public function configuredTables(): array
    {
        return array_keys($this->configured);
    }
}
