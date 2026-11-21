<?php

declare(strict_types=1);

namespace BinlogRelay\Event;

/**
 * DDL 变更事件
 *
 * binlog 里的 QUERY 事件除了 DDL，还会包含 BEGIN/COMMIT、以及各种隐含语句。
 * 这里只挑出真正的结构变更语句，供下游同步表结构。
 *
 * 与 Change 的区别：
 * - Change 描述「行数据变化」，DdlChange 描述「表结构变化」
 * - DDL 在 binlog 中是语句级（statement），不是行级
 * - 下游必须能执行 SQL，或据此更新自己的 schema 元数据
 */
final readonly class DdlChange
{
    public const CREATE_TABLE = 'create_table';
    public const ALTER_TABLE = 'alter_table';
    public const DROP_TABLE = 'drop_table';
    public const RENAME_TABLE = 'rename_table';
    public const TRUNCATE_TABLE = 'truncate_table';
    public const CREATE_INDEX = 'create_index';
    public const DROP_INDEX = 'drop_index';
    public const CREATE_DATABASE = 'create_database';
    public const DROP_DATABASE = 'drop_database';
    public const OTHER = 'other';

    public function __construct(
        /** 归一化后的操作类型，见上方常量 */
        public string $op,
        /** 所属库（QUERY 事件里的 schema） */
        public string $database,
        /** 涉及的库表（能解析出来时非空） */
        public array $tables,
        /** 完整 SQL 语句 */
        public string $query,
        public string $binlogFile,
        public string $binlogPos,
        public int $timestamp,
        public int $serverId,
    ) {
    }

    public function toArray(): array
    {
        return [
            'type'        => 'ddl',
            'op'          => $this->op,
            'database'    => $this->database,
            'tables'      => $this->tables,
            'query'       => $this->query,
            'binlog_file' => $this->binlogFile,
            'binlog_pos'  => $this->binlogPos,
            'timestamp'   => $this->timestamp,
            'server_id'   => $this->serverId,
        ];
    }

    /** DDL 的定位键：位置 + 语句哈希（同一位置可能有多个语句） */
    public function identity(): string
    {
        return $this->binlogFile . ':' . $this->binlogPos . ':ddl:' . substr(sha1($this->query), 0, 8);
    }

    /**
     * 从 SQL 语句识别 DDL 类型
     *
     * 返回 null 表示不是 DDL（BEGIN/COMMIT/INSERT 等），调用方应跳过。
     */
    public static function classify(string $query): ?string
    {
        // 去掉前导注释与空白
        $sql = ltrim(preg_replace('#^/\*.*?\*/#s', '', $query) ?? $query);
        if ($sql === '') {
            return null;
        }

        $upper = strtoupper($sql);

        return match (true) {
            str_starts_with($upper, 'CREATE TABLE')      => self::CREATE_TABLE,
            str_starts_with($upper, 'ALTER TABLE')       => self::ALTER_TABLE,
            str_starts_with($upper, 'DROP TABLE')        => self::DROP_TABLE,
            str_starts_with($upper, 'RENAME TABLE')      => self::RENAME_TABLE,
            str_starts_with($upper, 'TRUNCATE')          => self::TRUNCATE_TABLE,
            str_starts_with($upper, 'CREATE INDEX'),
            str_starts_with($upper, 'CREATE UNIQUE INDEX') => self::CREATE_INDEX,
            str_starts_with($upper, 'DROP INDEX')        => self::DROP_INDEX,
            str_starts_with($upper, 'CREATE DATABASE'),
            str_starts_with($upper, 'CREATE SCHEMA')     => self::CREATE_DATABASE,
            str_starts_with($upper, 'DROP DATABASE'),
            str_starts_with($upper, 'DROP SCHEMA')       => self::DROP_DATABASE,
            // 视图、存储过程等也算结构变更，统一归到 other
            str_starts_with($upper, 'CREATE VIEW'),
            str_starts_with($upper, 'ALTER VIEW'),
            str_starts_with($upper, 'DROP VIEW'),
            str_starts_with($upper, 'CREATE PROCEDURE'),
            str_starts_with($upper, 'DROP PROCEDURE'),
            str_starts_with($upper, 'CREATE FUNCTION'),
            str_starts_with($upper, 'DROP FUNCTION'),
            str_starts_with($upper, 'CREATE TRIGGER'),
            str_starts_with($upper, 'DROP TRIGGER')      => self::OTHER,
            // DML 与事务控制语句不是 DDL
            default                                      => null,
        };
    }

    /**
     * 从 SQL 中提取涉及的表名
     *
     * 只做保守解析（覆盖常见写法），解析不出来时返回空数组——
     * 下游可退化为「整库 schema 刷新」。
     *
     * @return list<string>
     */
    public static function extractTables(string $query): array
    {
        $tables = [];

        // CREATE TABLE / DROP TABLE / TRUNCATE TABLE / ALTER TABLE
        if (preg_match_all(
            '/\b(?:CREATE|DROP|ALTER|TRUNCATE)\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?'
            . '((?:`[^`]+`|[A-Za-z0-9_$]+)(?:\s*\.\s*(?:`[^`]+`|[A-Za-z0-9_$]+))?)/i',
            $query,
            $m
        )) {
            foreach ($m[1] as $name) {
                $tables[] = self::normalizeName($name);
            }
        }

        // RENAME TABLE a TO b, c TO d
        if (preg_match_all(
            '/\bRENAME\s+TABLE\s+(.+?)(?:;|$)/is',
            $query,
            $m
        )) {
            foreach (preg_split('/,/', $m[1][0]) ?: [] as $pair) {
                if (preg_match('/^\s*(`[^`]+`|[A-Za-z0-9_$.]+)\s+TO\s+(`[^`]+`|[A-Za-z0-9_$.]+)/i', $pair, $p)) {
                    $tables[] = self::normalizeName($p[1]);
                    $tables[] = self::normalizeName($p[2]);
                }
            }
        }

        return array_values(array_unique(array_filter($tables)));
    }

    /** 去掉反引号与库名前缀，只保留表名 */
    private static function normalizeName(string $name): string
    {
        $name = str_replace('`', '', trim($name));
        $pos = strrpos($name, '.');
        return $pos === false ? $name : substr($name, $pos + 1);
    }
}
