<?php

declare(strict_types=1);

namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;
use BinlogRelay\Support\Logger;

/**
 * 处理器链：按配置顺序依次执行，任一失败则整批失败（点位不推进）
 */
final class HandlerChain implements HandlerInterface
{
    /** @var list<HandlerInterface> */
    private array $handlers = [];

    public function __construct(private readonly Logger $logger)
    {
    }

    /** @param list<class-string|HandlerInterface> $specs */
    public function build(array $specs): void
    {
        foreach ($specs as $spec) {
            if ($spec instanceof HandlerInterface) {
                $this->handlers[] = $spec;
                continue;
            }
            if (!class_exists($spec)) {
                throw new \RuntimeException("处理器不存在：{$spec}");
            }
            $handler = new $spec();
            if (!$handler instanceof HandlerInterface) {
                throw new \RuntimeException("处理器必须实现 HandlerInterface：{$spec}");
            }
            $this->handlers[] = $handler;
        }
    }

    public function handle(array $changes): void
    {
        if ($changes === []) {
            return;
        }
        foreach ($this->handlers as $handler) {
            $start = microtime(true);
            $handler->handle($changes);
            $cost = (microtime(true) - $start) * 1000;
            if ($cost > 1000) {
                $this->logger->warning(sprintf(
                    '%s 处理 %d 条变更耗时 %.0fms',
                    $handler::class,
                    count($changes),
                    $cost
                ));
            }
        }
    }

    /**
     * 分发 DDL 事件：只调用实现了 DdlHandlerInterface 的处理器
     *
     * 没实现该接口的处理器会被跳过——这是刻意的，
     * 避免强迫所有 handler 关心低频的 DDL。
     */
    public function handleDdl(\BinlogRelay\Event\DdlChange $ddl): void
    {
        foreach ($this->handlers as $handler) {
            if (!$handler instanceof DdlHandlerInterface) {
                continue;
            }
            $start = microtime(true);
            $handler->handleDdl($ddl);
            $cost = (microtime(true) - $start) * 1000;
            if ($cost > 500) {
                $this->logger->warning(sprintf(
                    '%s 处理 DDL 耗时 %.0fms（%s）',
                    $handler::class,
                    $cost,
                    $ddl->op
                ));
            }
        }
    }
}
