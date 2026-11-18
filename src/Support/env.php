<?php

declare(strict_types=1);

/**
 * 读取环境变量，支持 .env 文件（不依赖 vlucas/phpdotenv）
 */
function env(string $key, mixed $default = null): mixed
{
    static $vars = null;
    if ($vars === null) {
        $vars = load_env_files();
    }
    if (array_key_exists($key, $vars)) {
        return $vars[$key];
    }
    $v = getenv($key);
    return $v === false ? $default : $v;
}

/** 逗号分隔字符串 → 数组（自动去空格、去空项） */
function csv(?string $value): array
{
    if ($value === null || trim($value) === '') {
        return [];
    }
    return array_values(array_filter(array_map('trim', explode(',', $value)), fn($v) => $v !== ''));
}

/**
 * 依次加载 CWD 与项目根目录下的 .env，后者优先。
 * 只支持 KEY=VALUE 与 # 注释，够用且零依赖。
 */
function load_env_files(): array
{
    $vars = [];
    $candidates = [];
    if (defined('RELAY_ROOT')) {
        $candidates[] = RELAY_ROOT . '/.env';
    }
    $candidates[] = getcwd() . '/.env';

    foreach ($candidates as $file) {
        if (!is_file($file)) {
            continue;
        }
        foreach (file($file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $line = trim($line);
            if ($line === '' || $line[0] === '#') {
                continue;
            }
            $pos = strpos($line, '=');
            if ($pos === false) {
                continue;
            }
            $k = trim(substr($line, 0, $pos));
            $v = trim(substr($line, $pos + 1));
            if ($v !== '' && ($v[0] === '"' || $v[0] === "'") && str_ends_with($v, $v[0])) {
                $v = substr($v, 1, -1);
            }
            $vars[$k] = $v;
        }
    }
    return $vars;
}
