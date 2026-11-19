<?php

declare(strict_types=1);

namespace BinlogRelay\Standalone;

use BinlogRelay\Relay\RowSubscriber;

/**
 * 单体服务的订阅者
 *
 * 逻辑与生产端完全一致（过滤 → 归一化 → 裁剪 → 攒批），
 * 差异只在注入的 handler：这里是单个 StreamWriter，生产端是 HandlerChain。
 *
 * 保留独立类名是为了让配置文件里的引用更直观，同时避免上游改名影响调用方。
 */
final class RelaySubscriber extends RowSubscriber
{
}
