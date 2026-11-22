<?php

declare(strict_types=1);

namespace BinlogRelay;

/**
 * 版本信息
 *
 * 打包时可通过环境变量注入（如 CI 里设置 RELAY_VERSION=$(git describe)），
 * 没有则用文件里的默认值。
 */
final class Version
{
    public const VERSION = '1.2.0';

    /** 构建时间，打包脚本可覆盖 */
    public const BUILD_TIME = 'unknown';

    public static function string(): string
    {
        $v = getenv('RELAY_VERSION') ?: self::VERSION;
        $b = getenv('RELAY_BUILD_TIME') ?: self::BUILD_TIME;
        return $b === 'unknown' ? $v : "{$v} ({$b})";
    }

    /** 运行时关键信息，排查环境问题时很有用 */
    public static function environment(): array
    {
        return [
            'PHP'      => PHP_VERSION,
            'SAPI'     => PHP_SAPI,
            'OS'       => PHP_OS_FAMILY,
            '扩展'      => implode(',', array_filter([
                extension_loaded('sockets') ? 'sockets' : null,
                extension_loaded('bcmath') ? 'bcmath' : null,
                extension_loaded('redis') ? 'redis' : null,
                extension_loaded('pdo_mysql') ? 'pdo_mysql' : null,
                extension_loaded('pcntl') ? 'pcntl' : null,
            ])),
            '打包模式'   => str_starts_with(__FILE__, 'phar://') ? 'phar' : 'source',
        ];
    }
}
