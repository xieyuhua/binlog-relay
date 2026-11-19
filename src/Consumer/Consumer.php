<?php

declare(strict_types=1);

namespace BinlogRelay\Consumer;

use BinlogRelay\Queue\Message;
use BinlogRelay\Sink\SinkInterface;
use BinlogRelay\Support\Logger;

/**
 * Redis Stream 消费循环
 *
 * 语义：
 * - 拉取未投递消息 → 整批交给 Sink 写入 → 成功后 XACK
 * - Sink 抛异常 → 不 ACK，消息留在 pending，指数退避后重试
 * - 连续失败超过 max_retries → 转入死信 stream 并 ACK 原消息
 * - 启动/空闲时用 XAUTOCLAIM 接管超时未确认的消息（进程崩溃后遗留）
 *
 * 只有 XACK 之后消息才算消费完成，任何时刻崩溃都只会重放、不会丢。
 */
final class Consumer
{
    private bool $running = true;
    private int $consecutiveFailures = 0;

    public function __construct(
        private readonly StreamInterface $stream,
        private readonly SinkInterface $sink,
        private readonly Logger $logger,
        private readonly string $streamName,
        private readonly string $group,
        private readonly string $consumerName,
        private readonly int $batchSize = 500,
        private int $blockMs = 5000,
        private readonly int $maxRetries = 5,
        private readonly string $deadLetterStream = '',
        private readonly int $claimMinIdleMs = 60000,
        private readonly float $retryBackoffSeconds = 2.0,
    ) {
    }

    public function name(): string
    {
        return $this->consumerName;
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function ensureGroup(): void
    {
        $this->stream->ensureGroup($this->streamName, $this->group);
    }

    /** 运行到指定时间点后停止（once 命令用） */
    public function runUntil(float $deadline): void
    {
        $originalBlock = $this->blockMs;
        $this->blockMs = min($this->blockMs, 200);
        try {
            $this->run($deadline);
        } finally {
            $this->blockMs = $originalBlock;
        }
    }

    public function run(?float $deadline = null): void
    {
        $this->ensureGroup();
        $this->sink->boot();
        $this->logger->info(sprintf(
            'consumer 启动：stream=%s group=%s consumer=%s sink=%s batch=%d',
            $this->streamName,
            $this->group,
            $this->consumerName,
            $this->sink->name(),
            $this->batchSize
        ));

        $this->claimPending();

        try {
            $this->running = true;
            while ($this->running) {
                if ($deadline !== null && microtime(true) >= $deadline) {
                    break;
                }

                $batch = $this->readNew();
                if ($batch === []) {
                    $this->claimPending();
                    continue;
                }
                $this->process($batch);
            }
        } finally {
            $this->sink->close();
            $this->logger->info('consumer 已停止');
        }
    }

    /** @return list<Message> */
    private function readNew(): array
    {
        try {
            $entries = $this->stream->readGroup(
                $this->streamName,
                $this->group,
                $this->consumerName,
                $this->batchSize,
                $this->blockMs
            );
        } catch (\Throwable $e) {
            $this->logger->error('读取队列失败：' . $e->getMessage());
            usleep((int)($this->retryBackoffSeconds * 1e6));
            return [];
        }

        if ($entries === null) {
            return [];
        }

        $messages = [];
        foreach ($entries as $id => $fields) {
            $messages[] = Message::fromEntry((string)$id, $fields);
        }
        return $messages;
    }

    /**
     * 处理一批：失败时原地重试，超过 max_retries 后进死信
     *
     * 原地重试而不是"丢回 pending 等 claim"，是因为 claim 的最小空闲时间
     * 通常是分钟级，会让下游抖动被放大成分钟级延迟。
     *
     * @param list<Message> $batch
     */
    private function process(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        for ($attempt = 1; $attempt <= max(1, $this->maxRetries); $attempt++) {
            try {
                $this->sink->write($batch);
                $this->ack($batch);
                $this->consecutiveFailures = 0;
                $this->logger->debug(sprintf('已处理并 ACK %d 条（第 %d 次尝试）', count($batch), $attempt));
                return;
            } catch (\Throwable $e) {
                $this->logger->error(sprintf(
                    '消费失败 第 %d/%d 次（%d 条，sink=%s）：%s',
                    $attempt,
                    $this->maxRetries,
                    count($batch),
                    $this->sink->name(),
                    $e->getMessage()
                ));

                if ($attempt >= $this->maxRetries) {
                    $this->handleDeadLetter($batch, $e);
                    return;
                }

                $sleep = min(30.0, $this->retryBackoffSeconds * (2 ** ($attempt - 1)));
                $this->logger->info(sprintf('%.1f 秒后重试', $sleep));
                usleep((int)($sleep * 1e6));
            }
        }
    }

    /** @param list<Message> $batch */
    private function ack(array $batch): void
    {
        $this->stream->ack(
            $this->streamName,
            $this->group,
            array_map(fn(Message $m) => $m->id, $batch)
        );
    }

    /** 接管超时未确认的消息（进程崩溃后遗留） */
    private function claimPending(): void
    {
        try {
            $entries = $this->stream->autoClaim(
                $this->streamName,
                $this->group,
                $this->consumerName,
                $this->claimMinIdleMs,
                $this->batchSize
            );
        } catch (\Throwable $e) {
            $this->logger->warning('接管 pending 失败：' . $e->getMessage());
            return;
        }

        if ($entries === []) {
            return;
        }

        $messages = [];
        foreach ($entries as [$id, $fields]) {
            $messages[] = Message::fromEntry($id, $fields);
        }
        $this->logger->info(sprintf('接管 %d 条超时未确认的消息', count($messages)));
        $this->process($messages);
    }

    /**
     * 超过最大重试次数：投递到死信 stream 并 ACK 原消息，避免队列被一条坏消息卡死
     *
     * @param list<Message> $batch
     */
    private function handleDeadLetter(array $batch, \Throwable $e): void
    {
        if ($this->deadLetterStream === '') {
            $this->logger->error('未配置死信队列，消息保留在 pending（需人工处理）');
            return;
        }

        try {
            foreach ($batch as $m) {
                $this->stream->add($this->deadLetterStream, [
                    'op'        => $m->op,
                    'db'        => $m->database,
                    'tbl'       => $m->table,
                    'pk'        => $m->pk,
                    'pos'       => $m->pos,
                    'ts'        => (string)$m->timestamp,
                    'data'      => json_encode($m->data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) ?: '{}',
                    'error'     => substr($e->getMessage(), 0, 500),
                    'source'    => $this->streamName,
                    'failed_at' => date('c'),
                ]);
            }
            $this->ack($batch);
            $this->logger->error(sprintf('已把 %d 条消息转入死信 %s', count($batch), $this->deadLetterStream));
        } catch (\Throwable $dlqError) {
            $this->logger->error('死信写入失败，消息保留在 pending：' . $dlqError->getMessage());
        }
    }
}
