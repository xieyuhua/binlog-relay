<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;
use RuntimeException;

/**
 * TCP 推送：长连接 + 长度前缀协议（可跨语言对接）
 *
 * 帧格式（大端）：
 *   [4 字节 uint32 长度 N] + [N 字节 UTF-8 JSON]
 *   JSON = { "events": [ ... ] }
 *
 * 服务端约定：每帧回一个 1 字节 ACK（0x06 成功 / 0x15 失败）。
 * 不实现帧内重试，失败交给消费端重试；断线后下次 write() 自动重连。
 */
final class TcpSink extends AbstractSink
{
    /** @var resource|null */
    private $socket = null;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly float $timeout = 3.0,
        array $tables = [],
        array $ops = [],
    ) {
        parent::__construct($tables, $ops);
    }

    public function name(): string
    {
        return 'tcp://' . $this->host . ':' . $this->port;
    }

    public function boot(): void
    {
        $this->connect();
    }

    public function close(): void
    {
        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }
        $this->socket = null;
    }

    public function write(array $messages): void
    {
        $messages = $this->filter($messages);
        if ($messages === []) {
            return;
        }

        $json = json_encode(
            ['events' => array_map(fn(Message $m) => $m->toArray(), $messages)],
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE
        );
        if ($json === false) {
            throw new RuntimeException('TCP payload 序列化失败');
        }

        $frame = pack('N', strlen($json)) . $json;

        // 断线重连一次：长连接被对端关闭是常态
        try {
            $this->sendAll($frame);
            $ack = fread($this->socket, 1);
        } catch (\Throwable) {
            $this->close();
            $this->connect();
            $this->sendAll($frame);
            $ack = fread($this->socket, 1);
        }

        if ($ack !== "\x06") {
            throw new RuntimeException('TCP 服务端未确认（ack=' . bin2hex((string)$ack) . '）');
        }
    }

    private function connect(): void
    {
        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            sprintf('tcp://%s:%d', $this->host, $this->port),
            $errno,
            $errstr,
            $this->timeout
        );
        if ($socket === false) {
            throw new RuntimeException(sprintf(
                'TCP 连接失败 %s:%d（%s）',
                $this->host,
                $this->port,
                $errstr ?: (string)$errno
            ));
        }
        stream_set_timeout($socket, (int)ceil($this->timeout));
        $this->socket = $socket;
    }

    private function sendAll(string $data): void
    {
        if (!is_resource($this->socket)) {
            throw new RuntimeException('TCP 连接未建立');
        }
        $total = strlen($data);
        $sent = 0;
        while ($sent < $total) {
            $n = @fwrite($this->socket, substr($data, $sent, 65536));
            if ($n === false || $n === 0) {
                throw new RuntimeException('TCP 写入失败');
            }
            $sent += $n;
        }
    }
}
