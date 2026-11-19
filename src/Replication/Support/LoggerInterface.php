<?php

declare(strict_types=1);

namespace BinlogRelay\Replication\Support;

/**
 * 内置日志接口（替代 psr/log）
 *
 * 解析库只用到 debug()，这里保留 PSR-3 的方法签名，
 * 方便直接接入项目里已有的 Logger。
 */
interface LoggerInterface
{
    public function debug(string|\Stringable $message, array $context = []): void;

    public function info(string|\Stringable $message, array $context = []): void;

    public function warning(string|\Stringable $message, array $context = []): void;

    public function error(string|\Stringable $message, array $context = []): void;
}
