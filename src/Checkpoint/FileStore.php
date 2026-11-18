<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

use RuntimeException;

/**
 * 文件点位存储：原子写（写临时文件 + rename），锁用 flock。
 * 适合单机 / 小规模部署，零外部依赖。
 */
final class FileStore implements StoreInterface
{
    private string $path;
    private string $lockPath;

    public function __construct(string $path)
    {
        $this->path = $path;
        $this->lockPath = $path . '.lock';
        $dir = dirname($path);
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new RuntimeException("点位目录创建失败：{$dir}");
        }
    }

    public function name(): string
    {
        return 'file:' . $this->path;
    }

    public function load(): ?Checkpoint
    {
        if (!is_file($this->path)) {
            return null;
        }
        $json = @file_get_contents($this->path);
        if ($json === false || $json === '') {
            return null;
        }
        $data = json_decode($json, true);
        return is_array($data) ? Checkpoint::fromArray($data) : null;
    }

    public function save(Checkpoint $checkpoint): void
    {
        $tmp = $this->path . '.' . getmypid() . '.tmp';
        $json = json_encode($checkpoint->toArray(), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($json === false) {
            throw new RuntimeException('点位序列化失败');
        }
        // 先落临时文件并 fsync，再 rename 保证读侧永远看到完整内容
        $fh = @fopen($tmp, 'wb');
        if ($fh === false) {
            throw new RuntimeException("点位文件写入失败：{$tmp}");
        }
        fwrite($fh, $json);
        fflush($fh);
        if (function_exists('fsync')) {
            @fsync($fh);
        }
        fclose($fh);

        if (!@rename($tmp, $this->path)) {
            @unlink($tmp);
            throw new RuntimeException("点位文件替换失败：{$this->path}");
        }
    }

    public function acquireLock(string $owner, int $ttl): bool
    {
        $fh = @fopen($this->lockPath, 'c+');
        if ($fh === false) {
            return false;
        }
        if (!flock($fh, LOCK_EX | LOCK_NB)) {
            fclose($fh);
            return false;
        }
        // 持有句柄直到进程退出；内容只用于诊断
        ftruncate($fh, 0);
        fwrite($fh, json_encode([
            'owner' => $owner,
            'pid' => getmypid(),
            'host' => gethostname(),
            'time' => date('c'),
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        fflush($fh);
        $GLOBALS['__relay_lock_handle'] = $fh;
        return true;
    }

    public function refreshLock(string $owner, int $ttl): void
    {
        // flock 由内核随进程存活自动释放，无需续期
    }

    public function releaseLock(string $owner): void
    {
        if (isset($GLOBALS['__relay_lock_handle'])) {
            @flock($GLOBALS['__relay_lock_handle'], LOCK_UN);
            @fclose($GLOBALS['__relay_lock_handle']);
            unset($GLOBALS['__relay_lock_handle']);
        }
    }
}
