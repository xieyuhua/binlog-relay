<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;
use RuntimeException;

/**
 * 把变更交给外部脚本处理（常驻子进程，NDJSON over stdin/stdout）
 *
 * 适用场景：业务逻辑已有一份 PHP 脚本，不想再包成类。
 *
 * 协议（与脚本约定）：
 *   父进程 → 子进程 stdin：每行一个 JSON 数组，形如
 *       [{"op":"insert","database":"demo","table":"orders","row":{...},...}, ...]
 *       （一批变更一行，行尾 \n）
 *   子进程 → 父进程 stdout：每收到一行必须回一行
 *       成功：{"ok":true}
 *       失败：{"ok":false,"error":"原因"}
 *   子进程 stderr 的内容会原样转发到框架日志（级别 error）。
 *
 * 为什么用常驻进程而不是每批 fork 一次：
 *   fork 成本约 20–50ms，而一次 binlog 批次可能几十条，逐批 fork 会成为瓶颈。
 *   常驻进程还能在脚本里复用数据库连接。
 *
 * 脚本必须能长时间运行（读一行、处理、写一行、循环）。示例见 examples/script-handler.php。
 */
final class ScriptHandler implements HandlerInterface
{
    /** @var resource|null 子进程 stdin */
    private $stdin = null;
    /** @var resource|null 子进程 stdout */
    private $stdout = null;
    /** @var resource|null 子进程 stderr */
    private $stderr = null;
    /** @var resource|null proc_open 句柄 */
    private $process = null;

    private int $restarts = 0;

    /**
     * @param list<string> $args      传给脚本的额外参数
     * @param array<string,string> $env 额外环境变量
     */
    public function __construct(
        private readonly string $script,
        private readonly array $args = [],
        private readonly array $env = [],
        private readonly int $maxRestarts = 3,
        private readonly float $timeoutSeconds = 30.0,
    ) {
        if (!is_file($script)) {
            throw new RuntimeException("脚本不存在：{$script}");
        }
    }

    public function __destruct()
    {
        $this->stop();
    }

    public function handle(array $changes): void
    {
        if ($changes === []) {
            return;
        }

        $this->ensureStarted();

        $payload = json_encode(
            array_map(static fn(Change $c) => $c->toArray(), $changes),
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE
        );
        if ($payload === false) {
            throw new RuntimeException('变更序列化失败');
        }

        $this->writeLine($payload);
        $response = $this->readLine();

        if ($response === null) {
            $this->stop();
            throw new RuntimeException("处理脚本无响应（{$this->script}），已终止，下批自动重启");
        }

        $result = json_decode($response, true);
        if (!is_array($result) || ($result['ok'] ?? false) !== true) {
            $reason = is_array($result) ? (string)($result['error'] ?? '脚本返回 ok=false') : '脚本返回值不是 JSON';
            throw new RuntimeException("脚本处理失败：{$reason}");
        }
    }

    /** 启动常驻子进程 */
    private function ensureStarted(): void
    {
        if (is_resource($this->process) && !feof($this->stdout)) {
            return;
        }
        if ($this->restarts >= $this->maxRestarts) {
            throw new RuntimeException(sprintf(
                '脚本已重启 %d 次仍失败，请检查 %s',
                $this->restarts,
                $this->script
            ));
        }
        if ($this->process !== null) {
            $this->restarts++;
        }

        $cmd = array_merge([PHP_BINARY, $this->script], $this->args);
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $env = $this->env === [] ? null : array_merge($_ENV, $this->env);

        $process = proc_open($cmd, $descriptors, $pipes, null, $env);
        if (!is_resource($process)) {
            throw new RuntimeException("无法启动脚本：{$this->script}");
        }

        // Windows 的 proc_open 管道不支持非阻塞模式（stream_set_blocking 返回 false），
        // 所以读端改用 stream_set_timeout + fgets，写端用非阻塞+select 兜底。
        stream_set_blocking($pipes[0], true);
        stream_set_timeout($pipes[1], 1);
        stream_set_timeout($pipes[2], 1);

        [$this->stdin, $this->stdout, $this->stderr, $this->process] = [$pipes[0], $pipes[1], $pipes[2], $process];
    }

    private function writeLine(string $line): void
    {
        if (!is_resource($this->stdin)) {
            throw new RuntimeException('脚本 stdin 不可用');
        }
        $data = $line . "\n";
        $length = strlen($data);
        $offset = 0;

        // stdin 设为非阻塞 + stream_select 等待可写，避免脚本不读 stdin 时
        // 管道写满导致 fwrite 永久阻塞（超时设置在 readLine 里，那时已经晚了）
        stream_set_blocking($this->stdin, false);

        $deadline = microtime(true) + $this->timeoutSeconds;
        while ($offset < $length) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                $this->stop();
                throw new RuntimeException(sprintf(
                    '脚本未读取输入（%.0f 秒），可能已卡死：%s',
                    $this->timeoutSeconds,
                    $this->script
                ));
            }

            $write = [$this->stdin];
            $read = $except = null;
            $sec = (int)$remaining;
            $usec = (int)(($remaining - $sec) * 1e6);
            $ready = @stream_select($read, $write, $except, $sec, $usec);

            if ($ready === false) {
                $this->stop();
                throw new RuntimeException('stream_select 失败（stdin）');
            }
            if ($ready === 0) {
                continue;
            }

            $n = @fwrite($this->stdin, substr($data, $offset, 65536));
            if ($n === false) {
                $this->stop();
                throw new RuntimeException('写入脚本失败，脚本可能已退出');
            }
            if ($n === 0) {
                usleep(1000);
                continue;
            }
            $offset += $n;
        }
    }

    /** 读取一行响应，带超时；同时把 stderr 转发到日志 */
    private function readLine(): ?string
    {
        if (!is_resource($this->stdout)) {
            return null;
        }

        $deadline = microtime(true) + $this->timeoutSeconds;
        $buffer = '';

        while (true) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                throw new RuntimeException(sprintf('脚本响应超时（%.0f 秒）', $this->timeoutSeconds));
            }

            // 用 stream_select 只做「是否有数据」的判断，超时设短（50ms），
            // 这样即便 Windows 上 EOF 误报可读，也不会长时间阻塞。
            $read = [$this->stdout];
            if (is_resource($this->stderr)) {
                $read[] = $this->stderr;
            }
            $write = $except = null;
            $ready = @stream_select($read, $write, $except, 0, 50_000);

            if ($ready === false) {
                throw new RuntimeException('stream_select 失败');
            }
            if ($ready === 0) {
                continue;
            }

            // Windows 管道在 EOF 时 select 会误报可读，此时 fgets 会阻塞十几秒。
            // 所以在任何读操作之前先检查 eof / 子进程状态。
            if (feof($this->stdout)) {
                return $buffer === '' ? null : trim($buffer);
            }
            if (!$this->isProcessAlive()) {
                return null;
            }

            if (!in_array($this->stdout, $read, true)) {
                $this->relayStderrMessage();
                continue;
            }

            // 管道不支持非阻塞，fgets 可能阻塞；用子进程存活状态兜底。
            $chunk = @fgets($this->stdout);
            if ($chunk === false) {
                return $buffer === '' ? null : trim($buffer);
            }

            $buffer .= $chunk;
            if (str_contains($buffer, "\n")) {
                return trim(strtok($buffer, "\n") ?: $buffer);
            }
        }
    }

    /** 子进程是否仍在运行 */
    private function isProcessAlive(): bool
    {
        if (!is_resource($this->process)) {
            return false;
        }
        $status = @proc_get_status($this->process);
        return is_array($status) && ($status['running'] ?? false);
    }

    /** 转发子进程 stderr 的内容（非阻塞读取，失败不影响主流程） */
    private function relayStderrMessage(): void
    {
        if (!is_resource($this->stderr)) {
            return;
        }
        $err = @fread($this->stderr, 8192);
        if ($err !== false && $err !== '') {
            fwrite(STDERR, rtrim($err) . PHP_EOL);
        }
    }

    private function stop(): void
    {
        // 顺序很重要：先 SIGTERM，再关管道，最后 proc_close。
        // 直接 proc_close 会阻塞到子进程退出，遇到死循环脚本会永久卡住。
        if (is_resource($this->process)) {
            @proc_terminate($this->process, 15);
        }

        foreach ([$this->stdin, $this->stdout, $this->stderr] as $pipe) {
            if (is_resource($pipe)) {
                @fclose($pipe);
            }
        }

        if (is_resource($this->process)) {
            // proc_close 在某些平台上仍会等待，给它一点时间后强制结束
            $start = microtime(true);
            while (($status = @proc_get_status($this->process)) && $status['running']) {
                if (microtime(true) - $start > 2.0) {
                    @proc_terminate($this->process, 9);   // SIGKILL
                    break;
                }
                usleep(20000);
            }
            @proc_close($this->process);
        }

        $this->stdin = $this->stdout = $this->stderr = $this->process = null;
    }
}
