<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;

/**
 * 默认处理器：每条变更输出一行 JSON（JSON Lines），方便直接管道给其它程序。
 *
 *   relay start | jq -c 'select(.table=="orders")'
 */
final class StdoutHandler implements HandlerInterface
{
    public function __construct(private readonly bool $pretty = false)
    {
    }

    public function handle(array $changes): void
    {
        $flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE;
        if ($this->pretty) {
            $flags |= JSON_PRETTY_PRINT;
        }
        $out = '';
        foreach ($changes as $change) {
            $out .= json_encode($change->toArray(), $flags) . PHP_EOL;
        }
        fwrite(STDOUT, $out);
    }
}
