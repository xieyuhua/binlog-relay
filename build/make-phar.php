<?php
/**
 * 把 stage 目录打成 phar，stub 直接转发到 relay.php
 *
 *   php -d phar.readonly=0 build/make-phar.php <stageDir> <outputPhar>
 */

declare(strict_types=1);

[, $stage, $output] = $argv + [null, null, null];
if ($stage === null || $output === null) {
    fwrite(STDERR, "用法：php make-phar.php <stageDir> <outputPhar>\n");
    exit(1);
}
$stage = rtrim(str_replace('\\', '/', realpath($stage)), '/');
$output = str_replace('\\', '/', $output);

@unlink($output);

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

$stub = <<<'STUB'
#!/usr/bin/env php
<?php
Phar::mapPhar('relay.phar');
require 'phar://relay.phar/relay.php';
__HALT_COMPILER();
STUB;

$phar->setStub($stub);
$phar->stopBuffering();
$phar->compressFiles(Phar::GZ);

echo "phar 生成完成：{$output}（{$count} 个文件）\n";
