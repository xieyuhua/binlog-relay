<?php

declare(strict_types=1);

namespace BinlogRelay\Checkpoint;

use BinlogRelay\Support\Logger;
use BinlogRelay\Replication\BinLog\BinLogCurrent;

/**
 * 点位管理：内存累积 → 按事件数/时间批量落盘（写放大降一个数量级）
 *
 * - 只保留最新点位，刷盘是幂等的
 * - 崩溃时最多回放 since-last-flush 的事件，下游需按主键幂等
 */
final class PositionManager
{
    private StoreInterface $store;
    private Logger $logger;
    private int $everyEvents;
    private float $everySeconds;
    private string $owner;

    private ?Checkpoint $pending = null;
    private int $eventsSinceFlush = 0;
    private int $baseEvents = 0;      // 上次启动时已累计的事件数
    private int $totalEvents = 0;     // 本次运行已处理的事件数
    private float $lastFlushAt = 0.0;
    private float $lastLockRefreshAt = 0.0;

    public function __construct(
        StoreInterface $store,
        Logger $logger,
        int $everyEvents = 100,
        float $everySeconds = 1.0,
        int $lockTtl = 30
    ) {
        $this->store = $store;
        $this->logger = $logger;
        $this->everyEvents = max(1, $everyEvents);
        $this->everySeconds = max(0.1, $everySeconds);
        $this->owner = sprintf('%s:%d:%s', gethostname() ?: 'php', getmypid(), bin2hex(random_bytes(4)));
        $this->lastFlushAt = microtime(true);
    }

    public function store(): StoreInterface
    {
        return $this->store;
    }

    public function owner(): string
    {
        return $this->owner;
    }

    public function lock(int $ttl = 30): void
    {
        if (!$this->store->acquireLock($this->owner, $ttl)) {
            throw new \RuntimeException(
                '点位已被其它 relay 实例占用（' . $this->store->name() . '），如需强制接管先执行 relay reset-position --force'
            );
        }
        $this->lastLockRefreshAt = microtime(true);
    }

    public function unlock(): void
    {
        $this->store->releaseLock($this->owner);
    }

    public function load(): ?Checkpoint
    {
        $cp = $this->store->load();
        // 累计事件数跨重启延续，status 里才有意义
        $this->baseEvents = $cp?->events ?? 0;
        return $cp;
    }

    /**
     * 是否累积 GTID 执行集合
     *
     * 关闭时只记录最后事务的 GTID（旧行为，开销几乎为零）；
     * 开启后维护完整集合，可用于跨 binlog 文件续传。
     */
    private bool $trackGtidSet = false;

    /** 累积中的 GTID 集合（仅在 trackGtidSet 为 true 时维护） */
    private ?GtidSet $gtidSet = null;

    /** 上一个已合并的 GTID，避免同一事务被重复合并（XID/ROTATE 会多次 track） */
    private string $lastGtid = '';

    /** 开启 GTID 集合累积，并用历史点位初始化 */
    public function enableGtidSet(string $initial = ''): void
    {
        $this->trackGtidSet = true;
        $this->gtidSet = $initial === '' ? new GtidSet() : GtidSet::parse($initial);
    }

    /** 当前累积的 GTID 集合（未开启时返回 null） */
    public function gtidSet(): ?GtidSet
    {
        return $this->gtidSet;
    }

    /** 记录当前 binlog 位置（每个事件调用一次，纯内存操作） */
    public function track(BinLogCurrent $current): void
    {
        // v9 中未启用的字段可能未初始化，getter 会抛 Error
        $file = $this->safeGet($current, 'getBinFileName');
        $pos = $this->safeGet($current, 'getBinLogPosition', '0');
        $gtid = $this->safeGet($current, 'getGtid');

        // 开启集合模式时，把当前事务并入集合；用 lastGtid 去重，
        // 因为同一事务的 ROWS/XID/ROTATE 事件会多次调用 track()
        if ($this->trackGtidSet && $gtid !== '' && $gtid !== $this->lastGtid) {
            try {
                $this->gtidSet?->addGtid($gtid);
                $this->lastGtid = $gtid;
            } catch (\InvalidArgumentException) {
                // 单事务 GTID 格式异常时跳过，不中断主流程
            }
        }

        $this->totalEvents++;
        $this->pending = new Checkpoint(
            $file,
            $pos,
            $this->trackGtidSet ? (string)$this->gtidSet : $gtid,
            time(),
            $this->baseEvents + $this->totalEvents,
            $this->owner
        );
        $this->eventsSinceFlush++;

        if ($this->eventsSinceFlush >= $this->everyEvents
            || microtime(true) - $this->lastFlushAt >= $this->everySeconds) {
            $this->flush();
        }

        // 锁续期（Redis TTL 场景）
        if (microtime(true) - $this->lastLockRefreshAt > 10) {
            $this->store->refreshLock($this->owner, 30);
            $this->lastLockRefreshAt = microtime(true);
        }
    }

    private function safeGet(object $obj, string $method, string $default = ''): string
    {
        try {
            return (string)$obj->{$method}();
        } catch (\Throwable) {
            return $default;
        }
    }

    public function flush(): void
    {
        if ($this->pending === null) {
            return;
        }
        try {
            $this->store->save($this->pending);
        } catch (\Throwable $e) {
            // 落盘失败不能静默：继续运行会造成点位回退，必须让上层感知
            $this->logger->error('点位保存失败：' . $e->getMessage());
            throw $e;
        }
        $this->lastFlushAt = microtime(true);
        $this->eventsSinceFlush = 0;
        // 累计数转成新的基准，pending 保留"已落盘到此处"的位置
        $this->baseEvents = $this->pending->events;
        $this->totalEvents = 0;
        $this->pending = new Checkpoint(
            $this->pending->binlogFile,
            $this->pending->binlogPos,
            $this->pending->gtid,
            $this->pending->updatedAt,
            $this->baseEvents,
            $this->owner
        );
    }
}
