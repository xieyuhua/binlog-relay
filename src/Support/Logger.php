<?php

declare(strict_types=1);

namespace BinlogRelay\Support;

use Psr\Log\AbstractLogger;

/**
 * 极简 PSR-3 日志：写 stderr 或指定文件。
 * 不引入 monolog，保持二进制体积小。
 */
final class Logger extends AbstractLogger
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

    public function log($level, \Stringable|string $message, array $context = []): void
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
