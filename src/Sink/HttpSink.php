<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;
use RuntimeException;

/**
 * HTTP 推送：整批 POST 一个 JSON 数组到业务接口
 *
 * 请求体：
 *   { "events": [ {id,op,db,tbl,pk,pos,ts,data}, ... ] }
 *
 * 幂等约定：接收方按 pk + pos 去重；返回 2xx 视为成功。
 * 重试由消费端统一负责（sink 抛异常即可）。
 */
final class HttpSink extends AbstractSink
{
    /** @param array<string,string> $headers */
    public function __construct(
        private readonly string $url,
        private readonly array $headers = [],
        private readonly float $timeout = 5.0,
        private readonly int $retries = 0,
        array $tables = [],
        array $ops = [],
    ) {
        parent::__construct($tables, $ops);
    }

    public function name(): string
    {
        return 'http:' . $this->url;
    }

    public function write(array $messages): void
    {
        $messages = $this->filter($messages);
        if ($messages === []) {
            return;
        }

        $payload = json_encode(
            ['events' => array_map(fn(Message $m) => $m->toArray(), $messages)],
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE
        );
        if ($payload === false) {
            throw new RuntimeException('HTTP payload 序列化失败');
        }

        $attempt = 0;
        while (true) {
            [$status, $body, $error] = $this->post($payload);
            if ($error === null && $status >= 200 && $status < 300) {
                return;
            }
            if ($attempt++ >= $this->retries) {
                throw new RuntimeException(sprintf(
                    'HTTP 推送失败（%s%s），响应：%s',
                    $status ?: '-',
                    $error ? ' ' . $error : '',
                    substr((string)$body, 0, 200)
                ));
            }
            // 只对 5xx / 网络错误重试，4xx 立刻失败（重试也没用）
            if ($status >= 400 && $status < 500) {
                throw new RuntimeException('HTTP 推送被拒绝（' . $status . '）：' . substr((string)$body, 0, 200));
            }
            usleep(200_000 * ($attempt + 1));
        }
    }

    /** @return array{0:int,1:string,2:?string} [状态码, 响应体, 错误信息] */
    private function post(string $payload): array
    {
        $ch = curl_init($this->url);
        $headers = array_merge(['Content-Type: application/json; charset=utf-8'], $this->headers);
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $payload,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_TIMEOUT        => (int)ceil($this->timeout),
            CURLOPT_CONNECTTIMEOUT => (int)ceil($this->timeout),
        ]);
        $body = curl_exec($ch);
        $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        $error = $body === false ? curl_error($ch) : null;
        curl_close($ch);

        return [$status, (string)$body, $error];
    }
}
