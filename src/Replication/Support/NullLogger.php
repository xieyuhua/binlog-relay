<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

/**
 * 空日志（替代 psr/log 的 NullLogger）
 */
final class NullLogger implements LoggerInterface
{
    public function debug(string|\Stringable $message, array $context = []): void
    {
    }

    public function info(string|\Stringable $message, array $context = []): void
    {
    }

    public function warning(string|\Stringable $message, array $context = []): void
    {
    }

    public function error(string|\Stringable $message, array $context = []): void
    {
    }
}
