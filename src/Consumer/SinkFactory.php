<?php

declare(strict_types=1);

namespace BinlogRelay\Consumer;

use BinlogRelay\Sink\ClickHouseSink;
use BinlogRelay\Sink\ElasticsearchSink;
use BinlogRelay\Sink\HttpSink;
use BinlogRelay\Sink\MysqlSink;
use BinlogRelay\Sink\SinkInterface;
use BinlogRelay\Sink\TcpSink;
use PDO;
use RuntimeException;

/**
 * 按配置构造 Sink
 */
final class SinkFactory
{
    public static function make(array $cfg): SinkInterface
    {
        $driver = (string)($cfg['driver'] ?? 'http');
        $tables = $cfg['tables'] ?? [];
        $ops = $cfg['ops'] ?? [];

        return match ($driver) {
            'http' => new HttpSink(
                (string)$cfg['http']['url'],
                (array)($cfg['http']['headers'] ?? []),
                (float)($cfg['http']['timeout'] ?? 5.0),
                (int)($cfg['http']['retries'] ?? 0),
                $tables,
                $ops
            ),

            'tcp' => new TcpSink(
                (string)$cfg['tcp']['host'],
                (int)$cfg['tcp']['port'],
                (float)($cfg['tcp']['timeout'] ?? 3.0),
                $tables,
                $ops
            ),

            'mysql' => new MysqlSink(
                self::mysql($cfg['mysql']),
                (string)($cfg['mysql']['table_prefix'] ?? ''),
                [],
                $tables,
                $ops
            ),

            'clickhouse' => new ClickHouseSink(
                (string)$cfg['clickhouse']['url'],
                (string)($cfg['clickhouse']['database'] ?? 'default'),
                (string)($cfg['clickhouse']['delete_mode'] ?? 'mark'),
                (string)($cfg['clickhouse']['delete_column'] ?? '_is_deleted'),
                (array)($cfg['clickhouse']['headers'] ?? []),
                (float)($cfg['clickhouse']['timeout'] ?? 10.0),
                $tables,
                $ops
            ),

            'elasticsearch' => new ElasticsearchSink(
                (string)$cfg['elasticsearch']['url'],
                (string)($cfg['elasticsearch']['index_prefix'] ?? ''),
                ($cfg['elasticsearch']['user'] ?? '') !== '' ? (string)$cfg['elasticsearch']['user'] : null,
                ($cfg['elasticsearch']['password'] ?? '') !== '' ? (string)$cfg['elasticsearch']['password'] : null,
                (array)($cfg['elasticsearch']['headers'] ?? []),
                (int)($cfg['elasticsearch']['batch_bytes'] ?? 5 * 1024 * 1024),
                (float)($cfg['elasticsearch']['timeout'] ?? 10.0),
                $tables,
                $ops
            ),

            default => throw new RuntimeException("未知的 sink driver：{$driver}"),
        };
    }

    public static function mysql(array $cfg): PDO
    {
        $dsn = sprintf(
            'mysql:host=%s;port=%d;dbname=%s;charset=%s',
            $cfg['host'],
            $cfg['port'],
            $cfg['database'],
            $cfg['charset'] ?? 'utf8mb4'
        );
        return new PDO($dsn, (string)$cfg['user'], (string)$cfg['password'], [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES   => false,
        ]);
    }
}
