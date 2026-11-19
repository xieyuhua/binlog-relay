<?php

declare(strict_types=1);

namespace BinlogRelay\Support;

use BinlogRelay\Replication\Support\LoggerInterface;

/**
 * 极简日志：写 stderr 或指定文件。
 * 实现内置的 LoggerInterface（PSR-3 风格签名），零第三方依赖。
 */
final class Logger implements LoggerInterface
{
    private const LEVELS = ['debug' => 0, 'info' => 1, 'warn' => 2, 'warning' => 2, 'error' => 3];

    /** @var resource */
    private $stream;
    private int $threshold;

    public function __construct(string $level = 'info', string $file = '')
    {
        $this->threshold = self::LEVELS[strtolower($level)] ?? 1;
        if ($file !== '') {
            $dir = dirname($file);
            if (!is_dir($dir)) {
                @mkdir($dir, 0775, true);
            }
            $this->stream = fopen($file, 'ab');
        } else {
            $this->stream = fopen('php://stderr', 'wb');
        }
    }

    public function debug(string|\Stringable $message, array $context = []): void
    {
        $this->log('debug', $message, $context);
    }

    public function info(string|\Stringable $message, array $context = []): void
    {
        $this->log('info', $message, $context);
    }

    public function warning(string|\Stringable $message, array $context = []): void
    {
        $this->log('warning', $message, $context);
    }

    public function error(string|\Stringable $message, array $context = []): void
    {
        $this->log('error', $message, $context);
    }

    public function log(string|\Stringable $level, string|\Stringable $message, array $context = []): void
    {
        $rank = self::LEVELS[strtolower((string)$level)] ?? 1;
        if ($rank < $this->threshold) {
            return;
        }

        $msg = (string)$message;
        // 用 {key} 占位符做简单插值，与 PSR-3 习惯一致
        foreach ($context as $k => $v) {
            if (is_scalar($v) || $v === null) {
                $msg = str_replace('{' . $k . '}', (string)$v, $msg);
            }
        }

        fwrite($this->stream, sprintf(
            "[%s] %s %s%s",
            date('Y-m-d H:i:s'),
            strtoupper((string)$level),
            $msg,
            PHP_EOL
        ));
    }
}
