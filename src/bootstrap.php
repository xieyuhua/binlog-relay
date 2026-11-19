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

        $autoload = BINLOG_RELAY_ROOT . '/vendor/autoload.php';
        if (is_file($autoload)) {
            require $autoload;
            return;
        }

        // 内置 PSR-4：BinlogRelay\Xxx → src/Xxx.php
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

        require_once BINLOG_RELAY_ROOT . '/src/Support/env.php';
    }
}

bootstrap();
