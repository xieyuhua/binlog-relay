<?php

/**
 * 零依赖自动加载器
 *
 * 不安装 Composer / 没有任何 vendor 时，只需 require 本文件即可加载整个项目。
 * 存在 vendor/autoload.php 时优先使用它（兼容 composer dump-autoload 优化后的场景）。
 */

declare(strict_types=1);

namespace BinlogRelay;

if (!defined('BINLOG_RELAY_ROOT')) {
    define('BINLOG_RELAY_ROOT', dirname(__DIR__));
}

if (!function_exists('BinlogRelay\\bootstrap')) {
    function bootstrap(): void
    {
        static $done = false;
        if ($done) {
            return;
        }
        $done = true;

        // 内置 PSR-4 始终注册（而非"有 vendor 就跳过"）：
        // composer 的 classmap 是生成时的快照，新增/重命名的文件不在里面，
        // composer autoloader 找不到时会落到这里兜底。重复注册无副作用。
        spl_autoload_register(static function (string $class): void {
            $prefix = 'BinlogRelay\\';
            if (!str_starts_with($class, $prefix)) {
                return;
            }
            $relative = substr($class, strlen($prefix));
            $file = BINLOG_RELAY_ROOT . '/src/' . str_replace('\\', '/', $relative) . '.php';
            if (is_file($file)) {
                require $file;
            }
        });

        // 兼容旧命名空间：内嵌库早期版本用 MySQLReplication\，现已统一为 BinlogRelay\Replication\
        spl_autoload_register(static function (string $class): void {
            $prefix = 'MySQLReplication\\';
            if (!str_starts_with($class, $prefix)) {
                return;
            }
            $relative = substr($class, strlen($prefix));
            $file = BINLOG_RELAY_ROOT . '/src/Replication/' . str_replace('\\', '/', $relative) . '.php';
            if (is_file($file)) {
                require $file;
            }
        });

        // 存在 composer autoload 时加载它（第三方包 + 优化后的 classmap）。
        // 它自身可能通过 autoload.files 引入 src/Support/env.php，
        // 所以必须用 in_array 判断而不是无条件 require。
        $autoload = BINLOG_RELAY_ROOT . '/vendor/autoload.php';
        if (is_file($autoload)) {
            require_once $autoload;
        }

        if (!function_exists('env')) {
            require_once BINLOG_RELAY_ROOT . '/src/Support/env.php';
        }
    }
}

bootstrap();
