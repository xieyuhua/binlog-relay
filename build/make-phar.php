<?php
/**
 * 把 stage 目录打成 phar
 *
 *   php -d phar.readonly=0 build/make-phar.php <stageDir> <outputPhar> [entryFile]
 *
 * entryFile 默认 relay.php，消费端打包时传 consumer.php。
 * 生成的 phar 可直接 `php dist/relay.phar start` 运行，
 * 也可作为 micro.sfx 的输入合成单文件二进制。
 */

declare(strict_types=1);

[, $stage, $output, $entry] = $argv + [null, null, null, 'relay.php'];
if ($stage === null || $output === null) {
    fwrite(STDERR, "用法：php make-phar.php <stageDir> <outputPhar> [entryFile]\n");
    exit(1);
}
$stage = rtrim(str_replace('\\', '/', realpath($stage)), '/');
$output = str_replace('\\', '/', $output);
$entry = ltrim((string)$entry, '/');

if (!is_file($stage . '/' . $entry)) {
    fwrite(STDERR, "入口文件不存在：{$stage}/{$entry}\n");
    exit(1);
}

@unlink($output);

// 启动文件不压缩：phar 的 stub 需要尽快定位入口，且 gzip 对纯文本收益有限
$phar = new Phar($output, 0, 'relay.phar');
$phar->startBuffering();

$iterator = new RecursiveIteratorIterator(
    new RecursiveDirectoryIterator($stage, FilesystemIterator::SKIP_DOTS)
);
$count = 0;
foreach ($iterator as $file) {
    /** @var SplFileInfo $file */
    if (!$file->isFile()) {
        continue;
    }
    $local = str_replace('\\', '/', substr($file->getPathname(), strlen($stage) + 1));
    $phar->addFile($file->getPathname(), $local);
    $count++;
}

$stub = <<<STUB
#!/usr/bin/env php
<?php
Phar::mapPhar('relay.phar');
require 'phar://relay.phar/{$entry}';
__HALT_COMPILER();
STUB;

$phar->setStub($stub);
$phar->stopBuffering();
$phar->compressFiles(Phar::GZ);

echo "phar 生成完成：{$output}（{$count} 个文件，入口 {$entry}）\n";
