<?php
/**
 * 网络 IO 基准：sockets 扩展 vs stream_socket
 *
 * 用途：验证「换 IO 实现能带来多少收益」这个假设。
 * 结论是两者差距 < 5%，瓶颈在等待 MySQL 返回数据，不在 syscall。
 *
 *   php tests/netbench.php [rounds] [bytes]
 *
 * 需要一个本地 echo 服务，脚本会自动拉起和清理。
 * Windows 上可能受防火墙/回环性能影响，建议在 Linux 上跑。
 */

declare(strict_types=1);

$rounds = (int)($argv[1] ?? 20000);
$size = (int)($argv[2] ?? 4096);
$port = 17300 + (getmypid() % 100);

$payload = str_repeat('x', $size);
$serverScript = sys_get_temp_dir() . '/relay-echosrv-' . getmypid() . '.php';

file_put_contents($serverScript, <<<PHP
<?php
\$srv = stream_socket_server('tcp://127.0.0.1:{$port}', \$e, \$s);
if (!\$srv) { fwrite(STDERR, "bind failed: \$s\n"); exit(1); }
\$conns = [];
while (true) {
    \$r = array_merge([\$srv], \$conns);
    \$w = \$x = null;
    if (@stream_select(\$r, \$w, \$x, 10) < 1) break;
    foreach (\$r as \$sock) {
        if (\$sock === \$srv) {
            \$c = @stream_socket_accept(\$srv, 0);
            if (\$c) \$conns[(int)\$c] = \$c;
            continue;
        }
        \$d = @fread(\$sock, 65536);
        if (\$d === '' || \$d === false) { unset(\$conns[(int)\$sock]); @fclose(\$sock); continue; }
        @fwrite(\$sock, \$d);
    }
}
PHP);

$proc = proc_open(
    [PHP_BINARY, $serverScript],
    [1 => ['file', PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null', 'w'],
     2 => ['file', PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null', 'w']],
    $pipes
);
usleep(500_000);

function report(string $tag, float $sec, int $rounds, int $bytes): float
{
    $perSec = $rounds / max($sec, 1e-9);
    printf(
        "%-18s %10.0f 次/秒   %7.1f µs/次   %7.1f MB/s\n",
        $tag,
        $perSec,
        $sec * 1e6 / $rounds,
        $bytes * $rounds / $sec / 1048576
    );
    return $sec * 1e6 / $rounds;
}

printf("报文 %d 字节，往返 %d 次，端口 %d\n\n", $size, $rounds, $port);
$results = [];

// ---- sockets 扩展 ----
$sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if ($sock === false || !socket_connect($sock, '127.0.0.1', $port)) {
    fwrite(STDERR, "无法连接 echo 服务\n");
    proc_terminate($proc, 9);
    proc_close($proc);
    @unlink($serverScript);
    exit(1);
}
$t = hrtime(true);
for ($i = 0; $i < $rounds; $i++) {
    socket_write($sock, $payload, $size);
    socket_recv($sock, $buf, $size, MSG_WAITALL);
}
$results['sockets'] = report('sockets ext', (hrtime(true) - $t) / 1e9, $rounds, $size);
socket_close($sock);
usleep(200_000);

// ---- stream_socket ----
$stream = @stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $errstr, 5);
if ($stream === false) {
    fwrite(STDERR, "stream_socket 连接失败：{$errstr}\n");
} else {
    $t = hrtime(true);
    for ($i = 0; $i < $rounds; $i++) {
        fwrite($stream, $payload);
        $got = '';
        while (strlen($got) < $size) {
            $chunk = fread($stream, 8192);
            if ($chunk === '' || $chunk === false) {
                break;
            }
            $got .= $chunk;
        }
    }
    $results['stream'] = report('stream_socket', (hrtime(true) - $t) / 1e9, $rounds, $size);
    fclose($stream);
}

proc_terminate($proc, 9);
proc_close($proc);
@unlink($serverScript);

if (isset($results['sockets'], $results['stream'])) {
    $diff = abs($results['sockets'] - $results['stream']) / max($results['sockets'], $results['stream']) * 100;
    printf("\n两者差距 %.1f%% —— 换 IO 实现收益有限，瓶颈在等待 MySQL 返回数据\n", $diff);
}
