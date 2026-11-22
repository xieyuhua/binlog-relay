<?php

declare(strict_types=1);

namespace BinlogRelay\Alert;

/**
 * 告警通道接口
 *
 * 约定：
 * - send() 尽量不抛异常（告警失败不应影响主流程），内部吞掉错误只记日志
 * - 同一类告警会被 NotificationManager 去重/限流后才会调用
 */
interface NotifierInterface
{
    /**
     * @param array<string,string> $fields 键值对形式的细节（会格式化成列表）
     */
    public function send(string $title, string $message, array $fields = []): void;

    public function name(): string;
}
