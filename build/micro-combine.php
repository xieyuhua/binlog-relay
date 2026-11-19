<?php
/**
 * 纯 PHP 版 micro:combine —— 不依赖 spc
 *
 * 产物格式（与 spc 的 MicroCombineCommand 完全一致）：
 *
 *   [ micro.sfx 内容 ]
 *   [ 可选 ini 段：魔数 \xfd\xf6\x69\xe6 + uint32 大端长度 + ini 文本 ]
 *   [ phar 或 php 脚本内容 ]
 *
 * micro.sfx 由 phpmicro 在末尾扫描这段结构并执行。
 *
 * 用法：
 *   php build/micro-combine.php <app.phar> <输出文件> [--micro=path] [--ini="k=v"]...
 *
 * 示例：
 *   php build/micro-combine.php dist/relay.phar dist/relay \
 *       --micro=build/micro.sfx \
 *       --ini="memory_limit=512M" \
 *       --ini="display_errors=stderr"
 */

declare(strict_types=1);

/** ini 段魔数，phpmicro 据此定位注入的配置 */
const MICRO_INI_MAGIC = "\xfd\xf6\x69\xe6";

function fail(string $msg): never
{
    fwrite(STDERR, "错误：{$msg}\n");
    exit(1);
}

function usage(): never
{
    fwrite(STDERR, <<<TXT
用法：
  php build/micro-combine.php <app.phar> <输出文件> [--micro=path] [--ini="k=v"]...

参数：
  --micro=path   指定 micro.sfx 路径，默认 build/micro.sfx
  --ini="k=v"    注入运行时 ini，可重复

TXT);
    exit(1);
}

/* ---------- 解析参数 ---------- */
$positional = [];
$microPath = __DIR__ . '/micro.sfx';
$iniPairs = [];

foreach (array_slice($argv, 1) as $arg) {
    if (str_starts_with($arg, '--micro=')) {
        $microPath = substr($arg, 8);
    } elseif (str_starts_with($arg, '--ini=')) {
        $iniPairs[] = substr($arg, 6);
    } elseif (str_starts_with($arg, '--help') || $arg === '-h') {
        usage();
    } elseif (str_starts_with($arg, '--')) {
        fail("未知参数：{$arg}");
    } else {
        $positional[] = $arg;
    }
}

[$input, $output] = $positional + [null, null];
if ($input === null || $output === null) {
    usage();
}

/* ---------- 校验输入 ---------- */
if (!is_file($microPath)) {
    fail(
        "找不到 micro.sfx：{$microPath}\n" .
        "请从 https://github.com/crazywhalecc/static-php-cli/releases 下载对应的 micro.sfx，\n" .
        "或放到 build/micro.sfx，或用 --micro= 指定路径。"
    );
}
if (!is_file($input)) {
    fail("找不到待打包文件：{$input}");
}

$micro = file_get_contents($microPath);
if ($micro === false || $micro === '') {
    fail("读取 micro.sfx 失败：{$microPath}");
}
if (strlen($micro) < 1024) {
    fail("micro.sfx 体积异常（" . strlen($micro) . " 字节），可能不是有效的运行时");
}

$payload = file_get_contents($input);
if ($payload === false || $payload === '') {
    fail("读取待打包文件失败：{$input}");
}

/* ---------- 构造 ini 段 ---------- */
$iniPart = '';
if ($iniPairs !== []) {
    $lines = [];
    foreach ($iniPairs as $pair) {
        if (!str_contains($pair, '=')) {
            fail("--ini 需要 key=value 形式：{$pair}");
        }
        [$key, $value] = explode('=', $pair, 2);
        $key = trim($key);
        if ($key === '') {
            fail("--ini 的 key 不能为空");
        }
        // 与 spc 一致：数字原样，其余加双引号
        $lines[] = $key . '=' . (is_numeric($value) ? $value : '"' . $value . '"');
    }
    $iniStr = implode("\n", $lines);
    $iniPart = MICRO_INI_MAGIC . pack('N', strlen($iniStr)) . $iniStr;
}

/* ---------- 合成 ---------- */
// 后缀取决于运行时（micro.sfx）的目标平台，而不是打包机：
// 交叉打包（Windows 上打 Linux 包）时不能加 .exe
$sfxKind = detectSfxKind($micro);
$isWindowsTarget = $sfxKind === 'PE';
if ($isWindowsTarget && !str_ends_with(strtolower($output), '.exe')) {
    $output .= '.exe';
}

$dir = dirname($output);
if (!is_dir($dir) && !mkdir($dir, 0775, true) && !is_dir($dir)) {
    fail("输出目录创建失败：{$dir}");
}

$bytes = file_put_contents($output, $micro . $iniPart . $payload);
if ($bytes === false) {
    fail("写入失败：{$output}");
}

if (PHP_OS_FAMILY !== 'Windows') {
    chmod($output, 0755);
}

printf(
    "合成完成：%s\n  micro.sfx : %s (%s, %s)\n  payload   : %s (%s)\n  ini       : %d 项\n  总计      : %s\n",
    $output,
    $microPath,
    humanSize(strlen($micro)),
    $sfxKind,
    $input,
    humanSize(strlen($payload)),
    count($iniPairs),
    humanSize($bytes)
);

// 交叉打包提醒：产物跟随 micro.sfx 的平台
if ($sfxKind === 'ELF' && PHP_OS_FAMILY === 'Windows') {
    echo "  注意      : 这是 Linux x86-64 产物，需拷到 Linux 上运行\n";
} elseif ($sfxKind === 'Mach-O' && PHP_OS_FAMILY !== 'Darwin') {
    echo "  注意      : 这是 macOS 产物，需拷到 macOS 上运行\n";
}

/** 按魔数识别运行时目标平台 */
function detectSfxKind(string $bytes): string
{
    $magic = substr($bytes, 0, 4);
    if ($magic === "\x7fELF") {
        return 'ELF';
    }
    if ($magic === "\xcf\xfa\xed\xfe" || $magic === "\xce\xfa\xed\xfe") {
        return 'Mach-O';
    }
    if (str_starts_with($magic, 'MZ')) {
        return 'PE';
    }
    return 'unknown';
}

function humanSize(int $bytes): string
{
    if ($bytes >= 1048576) {
        return round($bytes / 1048576, 2) . ' MB';
    }
    if ($bytes >= 1024) {
        return round($bytes / 1024, 1) . ' KB';
    }
    return $bytes . ' B';
}
