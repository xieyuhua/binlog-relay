<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;
use RuntimeException;

/**
 * Elasticsearch 落库：_bulk 批量写入
 *
 * 文档 ID 使用主键值（pk 里的主键部分），index/delete 天然幂等。
 * 索引名规则：index_prefix + 源表名（例如 shop_orders）。
 *
 * 认证：支持 Basic Auth（user/password）与 ApiKey（headers['Authorization']）。
 *
 * 部分失败处理：
 *   _bulk 返回 200 但 items 里可能有单条失败。默认要求全部成功（versions conflict 除外），
 *   否则抛异常让消费端重试——已有文档用 index 覆盖写，重放安全。
 */
final class ElasticsearchSink extends AbstractSink
{
    /** @param array<string,string> $headers */
    public function __construct(
        private readonly string $url,
        private readonly string $indexPrefix = '',
        private readonly ?string $user = null,
        private readonly ?string $password = null,
        private readonly array $headers = [],
        private readonly int $batchBytes = 5 * 1024 * 1024,
        private readonly float $timeout = 10.0,
        array $tables = [],
        array $ops = [],
    ) {
        parent::__construct($tables, $ops);
    }

    public function name(): string
    {
        return 'elasticsearch:' . $this->url;
    }

    public function write(array $messages): void
    {
        $messages = $this->filter($messages);
        if ($messages === []) {
            return;
        }

        // 按体积切分 _bulk 请求，避免单请求过大被拒
        $chunk = [];
        $bytes = 0;
        foreach ($messages as $m) {
            $size = strlen(json_encode($m->data) ?: '') + 200;
            if ($chunk !== [] && $bytes + $size > $this->batchBytes) {
                $this->bulk($chunk);
                $chunk = [];
                $bytes = 0;
            }
            $chunk[] = $m;
            $bytes += $size;
        }
        if ($chunk !== []) {
            $this->bulk($chunk);
        }
    }

    /** @param list<Message> $messages */
    private function bulk(array $messages): void
    {
        $lines = [];
        foreach ($messages as $m) {
            $index = $this->indexName($m->table);
            $id = $this->documentId($m);
            $lines[] = json_encode(
                ['index' => ['_index' => $index, '_id' => $id]],
                JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES
            );
            $lines[] = json_encode(
                ['_op' => $m->op, '_pos' => $m->pos, '_ts' => $m->timestamp] + $m->row(),
                JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE
            );
        }
        $body = implode("\n", $lines) . "\n";

        [$status, $resp] = $this->request('/_bulk', $body);
        if ($status < 200 || $status >= 300) {
            throw new RuntimeException("ES _bulk 失败（{$status}）：" . substr($resp, 0, 300));
        }

        $result = json_decode($resp, true);
        if (!is_array($result)) {
            throw new RuntimeException('ES _bulk 响应不是合法 JSON');
        }
        if (!empty($result['errors'])) {
            $failed = [];
            foreach ($result['items'] ?? [] as $item) {
                $op = $item['index'] ?? $item['create'] ?? null;
                if ($op === null) {
                    continue;
                }
                $err = $op['error'] ?? null;
                if ($err !== null) {
                    $failed[] = 'doc ' . ($op['_id'] ?? '?') . ': ' . ($err['type'] ?? '') . ' ' . ($err['reason'] ?? '');
                }
            }
            if ($failed !== []) {
                throw new RuntimeException('ES 部分写入失败：' . implode('; ', array_slice($failed, 0, 5)));
            }
        }
    }

    private function indexName(string $table): string
    {
        return $this->indexPrefix . $table;
    }

    /** 用主键值做 _id，保证重放不会产生重复文档 */
    private function documentId(Message $m): string
    {
        $keys = $m->tableKey();
        if ($keys === []) {
            // 没有主键时退化为 pk 字符串（仍稳定）
            return sha1($m->pk);
        }
        $parts = [];
        foreach ($keys as $k) {
            $parts[] = (string)($m->row()[$k] ?? '');
        }
        return implode(':', $parts);
    }

    /** @return array{0:int,1:string} */
    private function request(string $path, string $body): array
    {
        $ch = curl_init(rtrim($this->url, '/') . $path);
        $headers = array_merge(['Content-Type: application/x-ndjson'], $this->headers);
        if ($this->user !== null) {
            curl_setopt($ch, CURLOPT_USERPWD, $this->user . ':' . (string)$this->password);
        }
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $body,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_TIMEOUT        => (int)ceil($this->timeout),
            CURLOPT_CONNECTTIMEOUT => (int)ceil($this->timeout),
        ]);
        $resp = curl_exec($ch);
        $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        $err = $resp === false ? curl_error($ch) : null;
        curl_close($ch);

        if ($err !== null) {
            throw new RuntimeException('ES 请求失败：' . $err);
        }
        return [$status, (string)$resp];
    }
}
