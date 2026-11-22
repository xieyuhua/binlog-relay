<?php

declare(strict_types=1);

namespace BinlogRelay\Alert;

use BinlogRelay\Support\Logger;

/**
 * 告警管理器：多渠道分发 + 去重 + 限流
 *
 * 两个保护机制：
 * 1. 去重：同一 key 在 cooldown 内只发一次，避免故障时刷屏
 * 2. 限流：全局每分钟最多 N 条（企微/钉钉都是 20 条/分钟）
 *
 * 为什么需要：投递失败重试 5 次会触发 5 条告警，如果每次都发，
 * 一次故障就能把机器人限流打满，导致真正的严重告警发不出去。
 */
final class NotificationManager
{
    /** @var list<NotifierInterface> */
    private array $notifiers = [];

    /** @var array<string,int> key => 上次发送时间戳 */
    private array $lastSent = [];

    /** @var list<int> 最近发送的时间戳（用于限流） */
    private array $recentSends = [];

    public function __construct(
        private readonly Logger $logger,
        /** 同一 key 的去重窗口（秒） */
        private readonly int $cooldownSeconds = 300,
        /** 每分钟最多发送条数 */
        private readonly int $maxPerMinute = 15,
    ) {
    }

    public function add(NotifierInterface $notifier): void
    {
        $this->notifiers[] = $notifier;
    }

    public function isEmpty(): bool
    {
        return $this->notifiers === [];
    }

    /**
     * 发送告警
     *
     * @param string               $key    去重键（如 "delivery:redis"），相同 key 在冷却期内不重复发
     * @param array<string,string> $fields
     */
    public function alert(string $key, string $title, string $message, array $fields = []): void
    {
        if ($this->notifiers === []) {
            return;
        }

        // 1. 去重
        $now = time();
        if (isset($this->lastSent[$key]) && $now - $this->lastSent[$key] < $this->cooldownSeconds) {
            return;
        }

        // 2. 限流
        $this->recentSends = array_values(array_filter(
            $this->recentSends,
            static fn(int $t) => $now - $t < 60
        ));
        if (count($this->recentSends) >= $this->maxPerMinute) {
            $this->logger->warning('告警已达每分钟上限，本条被丢弃', [
                'key' => $key,
                'limit' => $this->maxPerMinute,
            ]);
            return;
        }

        $this->lastSent[$key] = $now;
        $this->recentSends[] = $now;

        foreach ($this->notifiers as $notifier) {
            try {
                $notifier->send($title, $message, $fields);
            } catch (\Throwable $e) {
                // 告警通道故障绝不能影响主流程
                $this->logger->warning('告警发送异常', [
                    'channel' => $notifier->name(),
                    'err' => $e->getMessage(),
                ]);
            }
        }

        $this->logger->info('已发送告警', ['key' => $key, 'title' => $title]);
    }

    /** 从配置构造 */
    public static function fromConfig(array $cfg, Logger $logger): self
    {
        $manager = new self(
            $logger,
            (int)($cfg['cooldown_seconds'] ?? 300),
            (int)($cfg['max_per_minute'] ?? 15),
        );

        if (!empty($cfg['wecom']['key'])) {
            $manager->add(new WeComNotifier(
                (string)$cfg['wecom']['key'],
                (float)($cfg['timeout'] ?? 5.0),
                $logger
            ));
        }

        if (!empty($cfg['dingtalk']['token'])) {
            $manager->add(new DingTalkNotifier(
                (string)$cfg['dingtalk']['token'],
                (string)($cfg['dingtalk']['secret'] ?? ''),
                (string)($cfg['dingtalk']['keyword'] ?? ''),
                (float)($cfg['timeout'] ?? 5.0),
                $logger
            ));
        }

        return $manager;
    }
}
