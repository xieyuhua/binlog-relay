<?php
/**
 * 消费端测试（无需真实 Redis / 下游服务）
 *
 * 覆盖：
 * - Message 编解码与旧格式降级
 * - Consumer：成功 ACK、失败不 ACK、指数退避后重试、超限进死信
 * - Sink 的表/操作过滤
 * - HttpSink 真实发请求（本地一次性 HTTP 服务接收并校验 body）
 *
 *   php tests/ConsumerTest.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use BinlogRelay\Consumer\Consumer;
use BinlogRelay\Consumer\StreamInterface;
use BinlogRelay\Event\Change;
use BinlogRelay\Queue\Message;
use BinlogRelay\Sink\SinkInterface;
use BinlogRelay\Support\Logger;

$failed = 0;
function check(string $name, bool $ok, string $detail = ''): void
{
    global $failed;
    echo ($ok ? '[OK]   ' : '[FAIL] ') . $name . ($ok || $detail === '' ? '' : " —— {$detail}") . "\n";
    if (!$ok) {
        $failed++;
    }
}

/* ===== 1. Message 编解码 ===== */
$change = new Change(
    Change::UPDATE, 'shop', 'orders',
    ['id' => 7, 'status' => 'paid'],
    ['id' => 7, 'status' => 'new'],
    ['id'], 'mysql-bin.000012', '4321', 'gtid-1', 1795013415, 3
);
$fields = Message::fromChange($change);
check('生产端字段齐全', array_keys($fields) === ['op', 'db', 'tbl', 'pk', 'pos', 'ts', 'data']);
check('pk 为幂等键', $fields['pk'] === 'shop.orders:7', $fields['pk']);

$msg = Message::fromEntry('1795-0', $fields);
check('消费端还原 op', $msg->op === 'update');
check('消费端还原 row', $msg->row() === ['id' => 7, 'status' => 'paid']);
check('消费端还原 before', $msg->before() === ['id' => 7, 'status' => 'new']);
check('消费端还原 tableKey', $msg->tableKey() === ['id']);
check('消费端还原 pos', $msg->pos === 'mysql-bin.000012:4321');

$broken = Message::fromEntry('1795-1', [
    'op' => 'insert', 'db' => 'shop', 'tbl' => 'orders', 'pk' => 'shop.orders:8',
    'pos' => 'bin:1', 'ts' => '1795000000', 'data' => '{not json',
]);
check('data 损坏时保留表信息', $broken->table === 'orders' && $broken->op === 'insert');
check('data 损坏时 row 为空数组', $broken->row() === []);

$legacy = Message::fromEntry('1795-2', ['op' => 'delete', 'db' => 'a', 'tbl' => 'b', 'pk' => 'x']);
check('老消息无 data 也能解析', $legacy->op === 'delete' && $legacy->table === 'b');

/* ===== 2. 假 Stream / 假 Sink ===== */
final class FakeStream implements StreamInterface
{
    /** @var list<array{0:string,1:array}> */
    public array $pending = [];
    public array $acked = [];
    public array $added = [];
    private bool $groupCreated = false;

    public function __construct(array $entries = [])
    {
        foreach ($entries as $id => $fields) {
            $this->pending[] = [(string)$id, $fields];
        }
    }

    public function ensureGroup(string $stream, string $group): void
    {
        $this->groupCreated = true;
    }

    public function groupCreated(): bool
    {
        return $this->groupCreated;
    }

    public function readGroup(string $stream, string $group, string $consumer, int $count, int $blockMs): ?array
    {
        if ($this->pending === []) {
            return null;
        }
        $out = [];
        foreach (array_splice($this->pending, 0, $count) as [$id, $fields]) {
            $out[$id] = $fields;
        }
        return $out;
    }

    public function ack(string $stream, string $group, array $ids): void
    {
        array_push($this->acked, ...$ids);
    }

    public function autoClaim(string $stream, string $group, string $consumer, int $minIdleMs, int $count): array
    {
        return [];
    }

    public function add(string $stream, array $fields): string
    {
        $this->added[] = ['stream' => $stream, 'fields' => $fields];
        return 'x' . count($this->added);
    }

    public function lag(string $stream, string $group): array
    {
        return ['stream_len' => 0, 'pending' => 0, 'lag' => 0];
    }
}

final class FlakySink implements SinkInterface
{
    public array $written = [];
    public int $calls = 0;

    public function __construct(private readonly int $failTimes = 0)
    {
    }

    public function boot(): void
    {
    }

    public function close(): void
    {
    }

    public function name(): string
    {
        return 'flaky';
    }

    public function write(array $messages): void
    {
        $this->calls++;
        if ($this->calls <= $this->failTimes) {
            throw new RuntimeException('模拟下游失败');
        }
        foreach ($messages as $m) {
            $this->written[] = $m->id;
        }
    }
}

function sampleEntries(): array
{
    return [
        '1-0' => ['op' => 'insert', 'db' => 'a', 'tbl' => 't', 'pk' => 'a.t:1', 'pos' => 'b:1', 'ts' => '1', 'data' => '{"op":"insert","row":{"id":1}}'],
        '2-0' => ['op' => 'insert', 'db' => 'a', 'tbl' => 't', 'pk' => 'a.t:2', 'pos' => 'b:2', 'ts' => '2', 'data' => '{"op":"insert","row":{"id":2}}'],
    ];
}

function runOnce(StreamInterface $stream, SinkInterface $sink, int $maxRetries = 3, string $dlq = 'dlq', float $backoff = 0.01): void
{
    $consumer = new Consumer(
        $stream, $sink, new Logger('error'),
        'stream', 'group', 'worker',
        10, 0, $maxRetries, $dlq, 0, $backoff
    );
    // 消费一轮后退出：deadline 设到过去之后，readGroup 首次返回后 process 完成即停
    $consumer->runUntil(microtime(true) + 0.05);
}

// 2a. 成功 → 全部 ACK
$stream = new FakeStream(sampleEntries());
$sink = new FlakySink();
runOnce($stream, $sink);
check('成功时 sink 收到全部消息', $sink->written === ['1-0', '2-0'], json_encode($sink->written));
check('成功时全部 ACK', $stream->acked === ['1-0', '2-0'], json_encode($stream->acked));
check('自动创建 consumer group', $stream->groupCreated());

// 2b. 首次失败 → 重试成功并 ACK
$stream = new FakeStream(sampleEntries());
$sink = new FlakySink(1);
runOnce($stream, $sink);
check('失败后重试成功并 ACK', $stream->acked === ['1-0', '2-0'], json_encode($stream->acked));
check('sink 被调用两次', $sink->calls === 2, (string)$sink->calls);

// 2c. 一直失败 → 进死信并 ACK 原消息
$stream = new FakeStream(sampleEntries());
$sink = new FlakySink(99);
runOnce($stream, $sink, 2);
check('超限后写入死信 2 条', count($stream->added) === 2, (string)count($stream->added));
check('死信带 error/source', isset($stream->added[0]['fields']['error'], $stream->added[0]['fields']['source']));
check('死信 op/db/tbl/pk 完整',
    $stream->added[0]['fields']['op'] === 'insert'
    && $stream->added[0]['fields']['tbl'] === 't'
    && $stream->added[0]['fields']['pk'] === 'a.t:1'
    && $stream->added[1]['fields']['pk'] === 'a.t:2', json_encode($stream->added[0]['fields']));
check('进死信后原消息被 ACK', $stream->acked === ['1-0', '2-0'], json_encode($stream->acked));

// 2d. 未配置死信 → 不 ACK、消息留在 pending
$stream = new FakeStream(sampleEntries());
$sink = new FlakySink(99);
runOnce($stream, $sink, 2, '');
check('无死信时消息保持 pending', $stream->acked === [] && $stream->added === []);

/* ===== 3. Sink 过滤 ===== */
$http = new BinlogRelay\Sink\HttpSink('http://127.0.0.1:1/', [], 1.0, 0, ['orders'], []);
$ref = new ReflectionMethod($http, 'filter');
$ref->setAccessible(true);
$m1 = Message::fromEntry('1', ['op' => 'insert', 'db' => 'a', 'tbl' => 'orders', 'data' => '{}']);
$m2 = Message::fromEntry('2', ['op' => 'insert', 'db' => 'a', 'tbl' => 'users', 'data' => '{}']);
check('表过滤生效', count($ref->invoke($http, [$m1, $m2])) === 1);

$http2 = new BinlogRelay\Sink\HttpSink('http://127.0.0.1:1/', [], 1.0, 0, [], ['delete']);
$ref2 = new ReflectionMethod($http2, 'filter');
$ref2->setAccessible(true);
$m3 = Message::fromEntry('3', ['op' => 'delete', 'db' => 'a', 'tbl' => 'orders', 'data' => '{}']);
check('操作类型过滤生效', count($ref2->invoke($http2, [$m1, $m3])) === 1);

/* ===== 4. HttpSink 真实请求（本地一次性服务） ===== */
$port = 18081;
$bodyFile = sys_get_temp_dir() . '/relay_http_body.json';
@unlink($bodyFile);
$script = sys_get_temp_dir() . '/relay_srv_' . getmypid() . '.php';
file_put_contents($script, '<?php
$srv = stream_socket_server("tcp://127.0.0.1:' . $port . '", $e, $s);
if (!$srv) { exit(1); }
$conn = stream_socket_accept($srv, 15);
if (!$conn) { exit(1); }
$req = "";
while (!feof($conn)) {
    $chunk = fread($conn, 65536);
    if ($chunk === "" || $chunk === false) break;
    $req .= $chunk;
    if (strpos($req, "\r\n\r\n") !== false) {
        $len = 0;
        if (preg_match("/Content-Length:\s*(\d+)/i", $req, $m)) $len = (int)$m[1];
        $body = substr($req, strpos($req, "\r\n\r\n") + 4);
        if (strlen($body) >= $len) break;
    }
}
file_put_contents(' . var_export($bodyFile, true) . ', $req);
fwrite($conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
fclose($conn);
');
$proc = proc_open(
    [PHP_BINARY, $script],
    [1 => ['file', PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null', 'w'], 2 => ['file', PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null', 'w']],
    $pipes
);
usleep(400_000);

$ok = false;
try {
    $messages = [];
    foreach (sampleEntries() as $id => $f) {
        $messages[] = Message::fromEntry($id, $f);
    }
    $sink = new BinlogRelay\Sink\HttpSink("http://127.0.0.1:{$port}/ingest");
    $sink->write($messages);
    $raw = @file_get_contents($bodyFile);
    $body = $raw === false ? '' : substr($raw, strpos($raw, "\r\n\r\n") + 4);
    $json = json_decode($body, true);
    check('HttpSink 发送 events 数组', is_array($json) && isset($json['events']) && count($json['events']) === 2);
    check('HttpSink 单条含 op/db/tbl/pk/data',
        isset($json['events'][0]['op'], $json['events'][0]['db'], $json['events'][0]['tbl'], $json['events'][0]['pk'], $json['events'][0]['data']));
    $ok = true;
} catch (Throwable $e) {
    check('HttpSink 请求', false, $e->getMessage());
} finally {
    if (is_resource($proc)) {
        proc_terminate($proc);
        proc_close($proc);
    }
    @unlink($script);
    @unlink($bodyFile);
}

echo $failed === 0 ? "\n全部通过\n" : "\n{$failed} 项失败\n";
exit($failed === 0 ? 0 : 1);
