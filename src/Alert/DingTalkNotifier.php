<?php

declare(strict_types=1);

namespace BinlogRelay\Alert;

use BinlogRelay\Support\Logger;

/**
 * 钉钉群机器人
 *
 * 接口：POST https://oapi.dingtalk.com/robot/send?access_token=<TOKEN>
 *
 * 安全设置三选一（在钉钉后台配置）：
 *   1. 自定义关键词 —— 消息里必须包含关键词（本类会自动带上 keyword）
 *   2. 加签（推荐）—— 用 secret 计算 timestamp + sign
 *   3. IP 白名单 —— 无需额外参数
 *
 * 限制：每个机器人 20 条/分钟
 *
 * @see https://open.dingtalk.com/document/robots/custom-robot-access
 */
final class DingTalkNotifier implements NotifierInterface
{
    private const MAX_BYTES = 4000;

    public function __construct(
        private readonly string $token,
        /** 加签密钥；用关键词或 IP 白名单时可留空 */
        private readonly string $secret = '',
        /** 自定义关键词模式时必须出现在消息里 */
        private readonly string $keyword = '',
        private readonly float $timeout = 5.0,
        private readonly ?Logger $logger = null,
    ) {
        if ($token === '') {
            throw new \InvalidArgumentException('钉钉机器人 token 不能为空');
        }
    }

    public function name(): string
    {
        return 'dingtalk';
    }

    /**
     * @param array<string,string> $fields
     */
    public function send(string $title, string $message, array $fields = []): void
    {
        $content = $this->render($title, $message, $fields);

        $payload = [
            'msgtype'  => 'markdown',
            'markdown' => [
                'title' => $title,
                'text'  => $content,
            ],
        ];

        $this->post($payload);
    }

    /**
     * @param array<string,string> $fields
     */
    private function render(string $title, string $message, array $fields): string
    {
        // 关键词模式：消息必须包含关键词，否则钉钉会拒绝
        $prefix = $this->keyword !== '' ? $this->keyword . ' ' : '';

        $lines = ['### ' . $prefix . $title, ''];

        if ($message !== '') {
            $lines[] = $message;
            $lines[] = '';
        }

        foreach ($fields as $k => $v) {
            $lines[] = "- **{$k}**：{$v}";
        }

        $text = implode("\n", $lines);

        if (strlen($text) > self::MAX_BYTES) {
            $text = mb_strcut($text, 0, self::MAX_BYTES - 20, 'UTF-8') . "\n\n…（内容过长已截断）";
        }
        return $text;
    }

    private function post(array $payload): void
    {
        $url = 'https://oapi.dingtalk.com/robot/send?access_token=' . rawurlencode($this->token);

        // 加签：timestamp 必须是毫秒，sign = urlencode(base64(hmac_sha256(secret, timestamp + "\n" + secret)))
        if ($this->secret !== '') {
            $timestamp = (string)(int)(microtime(true) * 1000);
            $sign = rawurlencode(base64_encode(hash_hmac(
                'sha256',
                $timestamp . "\n" . $this->secret,
                $this->secret,
                true
            )));
            $url .= '&timestamp=' . $timestamp . '&sign=' . $sign;
        }

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
            $this->logger?->warning('钉钉告警发送失败', ['err' => $err]);
            return;
        }
        if ($status < 200 || $status >= 300) {
            $this->logger?->warning('钉钉告警返回异常', ['status' => $status, 'body' => substr((string)$resp, 0, 200)]);
            return;
        }

        $result = json_decode((string)$resp, true);
        if (is_array($result) && ($result['errcode'] ?? 0) !== 0) {
            $this->logger?->warning('钉钉告警被拒绝', [
                'errcode' => $result['errcode'] ?? '',
                'errmsg' => $result['errmsg'] ?? '',
            ]);
        }
    }
}
