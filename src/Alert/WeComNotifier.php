<?php

declare(strict_types=1);

namespace BinlogRelay\Alert;

use BinlogRelay\Support\Logger;

/**
 * 企业微信群机器人
 *
 * 接口：POST https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=<KEY>
 * 限制：
 *   - 每个机器人 20 条/分钟
 *   - markdown 内容最长 4096 字节
 *
 * @see https://developer.work.weixin.qq.com/document/path/91770
 */
final class WeComNotifier implements NotifierInterface
{
    private const MAX_BYTES = 4000;   // 留一点余量，官方限制 4096

    public function __construct(
        private readonly string $key,
        private readonly float $timeout = 5.0,
        private readonly ?Logger $logger = null,
    ) {
        if ($key === '') {
            throw new \InvalidArgumentException('企微机器人 key 不能为空');
        }
    }

    public function name(): string
    {
        return 'wecom';
    }

    /**
     * @param array<string,string> $fields
     */
    public function send(string $title, string $message, array $fields = []): void
    {
        $content = $this->render($title, $message, $fields);

        $this->post([
            'msgtype'  => 'markdown',
            'markdown' => ['content' => $content],
        ]);
    }

    /**
     * @param array<string,string> $fields
     */
    private function render(string $title, string $message, array $fields): string
    {
        // 用颜色标记严重程度：告警红色、恢复绿色
        $lines = ['## <font color="warning">' . $title . '</font>', ''];

        if ($message !== '') {
            $lines[] = $message;
            $lines[] = '';
        }

        foreach ($fields as $k => $v) {
            $lines[] = "> **{$k}**：{$v}";
        }

        $text = implode("\n", $lines);

        // 超出字节上限时截断，避免接口直接报错
        if (strlen($text) > self::MAX_BYTES) {
            $text = mb_strcut($text, 0, self::MAX_BYTES - 20, 'UTF-8') . "\n\n…（内容过长已截断）";
        }
        return $text;
    }

    private function post(array $payload): void
    {
        $url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=' . rawurlencode($this->key);

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => json_encode($payload, JSON_UNESCAPED_UNICODE),
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => ['Content-Type: application/json; charset=utf-8'],
            CURLOPT_TIMEOUT        => (int)ceil($this->timeout),
            CURLOPT_CONNECTTIMEOUT => (int)ceil($this->timeout),
        ]);
        $resp = curl_exec($ch);
        $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        $err = $resp === false ? curl_error($ch) : null;
        curl_close($ch);

        if ($err !== null) {
            $this->logger?->warning('企微告警发送失败', ['err' => $err]);
            return;
        }
        if ($status < 200 || $status >= 300) {
            $this->logger?->warning('企微告警返回异常', ['status' => $status, 'body' => substr((string)$resp, 0, 200)]);
            return;
        }

        // 企微即使 HTTP 200，也可能在 body 里返回 errcode != 0
        $result = json_decode((string)$resp, true);
        if (is_array($result) && ($result['errcode'] ?? 0) !== 0) {
            $this->logger?->warning('企微告警被拒绝', [
                'errcode' => $result['errcode'] ?? '',
                'errmsg' => $result['errmsg'] ?? '',
            ]);
        }
    }
}
