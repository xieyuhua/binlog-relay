<?php

declare(strict_types=1);

namespace BinlogRelay\Sink;

use BinlogRelay\Queue\Message;
use RuntimeException;

/**
 * ClickHouse 落库：走 HTTP 接口 + JSONEachRow + 批量 INSERT
 *
 *   POST {url}/?query=INSERT INTO db.tbl FORMAT JSONEachRow
 *
 * 每张表一次请求，body 是多行 JSON（NDJSON），CK 原生支持高吞吐写入。
 *
 * 表引擎建议 ReplacingMergeTree(order_by=主键)：重复写入同一主键只保留最新版本，
 * 天然满足 binlog 重放的幂等诉求；查询时用 FINAL 或按版本列去重。
 *
 * 删除语义：ReplacingMergeTree 不处理 DELETE，需要删除用
 *   - 加 is_deleted 标记列（软删，推荐），或
 *   - 目标表使用 CollapsingMergeTree / 定期 OPTIMIZE
 * 配置里的 delete_mode 决定：
 *   mark   —— 写入 {"...": ..., "_is_deleted": 1}（需要目标表有该列）
 *   skip   —— 忽略删除事件
 */
final class ClickHouseSink extends AbstractSink
{
    /**
     * @param string $url      例如 http://127.0.0.1:8123
     * @param string $database 目标库
     * @param string $deleteMode mark|skip
     * @param array<string,string> $headers 额外请求头，如 X-ClickHouse-User/Key
     */
    public function __construct(
        private readonly string $url,
        private readonly string $database = 'default',
        private readonly string $deleteMode = 'mark',
        private readonly string $deleteColumn = '_is_deleted',
        private readonly array $headers = [],
        private readonly float $timeout = 10.0,
        array $tables = [],
        array $ops = [],
    ) {
        parent::__construct($tables, $ops);
    }

    public function name(): string
    {
        return 'clickhouse:' . $this->url . '/' . $this->database;
    }

    public function write(array $messages): void
    {
        $messages = $this->filter($messages);
        if ($messages === []) {
            return;
        }

        /** @var array<string,list<string>> $byTable table => NDJSON 行 */
        $byTable = [];
        foreach ($messages as $m) {
            if ($m->op === 'delete') {
                if ($this->deleteMode !== 'mark') {
                    continue;
                }
                $row = $m->row();
                $row[$this->deleteColumn] = 1;
            } else {
                $row = $m->row();
                if ($this->deleteMode === 'mark') {
                    $row[$this->deleteColumn] = 0;
                }
            }
            $json = json_encode($row, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
            if ($json === false) {
                throw new RuntimeException('ClickHouse 行序列化失败：' . $m->pk);
            }
            $byTable[$m->table][] = $json;
        }

        foreach ($byTable as $table => $lines) {
            $this->insert($table, implode("\n", $lines));
        }
    }

    /** @param list<string> $lines */
    private function insert(string $table, string $body): void
    {
        $table = $this->database . '.' . $table;
        $query = sprintf('INSERT INTO %s FORMAT JSONEachRow', $table);
        $url = rtrim($this->url, '/') . '/?' . http_build_query(['query' => $query]);

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $body,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => array_merge(['Content-Type: application/x-ndjson'], $this->headers),
            CURLOPT_TIMEOUT        => (int)ceil($this->timeout),
            CURLOPT_CONNECTTIMEOUT => (int)ceil($this->timeout),
        ]);
        $resp = curl_exec($ch);
        $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        $err = $resp === false ? curl_error($ch) : null;
        curl_close($ch);

        if ($err !== null || $status < 200 || $status >= 300) {
            throw new RuntimeException(sprintf(
                'ClickHouse 写入失败 %s（%s%s）：%s',
                $table,
                $status ?: '-',
                $err ? ' ' . $err : '',
                substr((string)$resp, 0, 300)
            ));
        }
    }
}
