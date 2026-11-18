# binlog-relay

用 PHP 写的轻量 MySQL binlog 中继框架，用于替代 **Canal / Maxwell / Debezium + Kafka** 这类重型 Java 链路。
核心只有三件事：**读 binlog → 归一化 → 交给你的 handler**，加上可插拔的点位存储（Redis / 文件）和一键打包单文件二进制。

底层 binlog 协议解析基于 [krowinski/php-mysql-replication](https://github.com/krowinski/php-mysql-replication)（纯 PHP 实现，支持 MySQL 5.5–8.0、MariaDB，含 `caching_sha2_password`）。

---

## 1. 它解决什么

| 场景 | 传统方案 | binlog-relay |
|---|---|---|
| 缓存失效 | Canal → Kafka → Java consumer | relay → Redis `DEL` 指令 |
| 搜索索引同步 | Canal → Kafka → ES writer | relay → ES bulk |
| 数仓增量 | Maxwell → Kafka → Flink | relay → 直接写目标库 |
| 业务审计 | 触发器 / 双写 | relay → 审计表 |

特点：

- **单文件部署**：`spc micro:combine` 打成 ~30MB 的可执行文件，目标机器不用装 PHP
- **点位零丢失**：Redis 原子 hash / 文件原子 rename，批量落盘降低 IO
- **单实例互斥**：Redis `SET NX PX` 或 `flock`，避免两个进程重复消费
- **背压可控**：按事务批量提交，handler 抛异常即停，点位停在上一批
- **稳定契约**：handler 只依赖 `BinlogRelay\Event\Change`，底层库升级不影响业务代码

---

## 2. 架构

```
┌──────────────┐   replication protocol   ┌──────────────────────────────┐
│  MySQL 主库   │ ◄──────────────────────► │  relay 进程（常驻）            │
│  binlog(Row) │                          │                              │
└──────────────┘                          │  MySQLReplicationFactory      │
                                          │    └─ Subscriber              │
                                          │         ├─ EventBuilder       │
                                          │         │    RowsDTO→Change[]  │
                                          │         ├─ Filter 库/表过滤    │
                                          │         └─ HandlerChain       │
                                          │              ├─ 你的 handler   │
                                          │              └─ StdoutHandler │
                                          │  PositionManager              │
                                          │    └─ Redis / File 点位        │
                                          └──────────────────────────────┘
                                                       │
                                       ┌───────────────┼────────────────┐
                                       ▼               ▼                ▼
                                   Redis 缓存       ES / 数仓       审计表/下游
```

数据流：binlog event → `RowsDTO` → `Change[]`（统一结构）→ 按事务攒批 → handler → **成功后**推进点位。

---

## 3. 快速开始

### 3.1 MySQL 准备

```ini
# my.cnf
[mysqld]
server-id        = 1
log_bin          = /var/log/mysql/mysql-bin
binlog_format    = ROW
binlog_row_image = FULL
binlog_row_metadata = FULL   # MySQL 8.0+，避免每张表首次出现查 information_schema
expire_logs_days = 7
max_binlog_size  = 512M
```

```sql
CREATE USER 'repl'@'%' IDENTIFIED BY '强密码';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
GRANT SELECT ON 你的库.* TO 'repl'@'%';   -- 用于读表结构
FLUSH PRIVILEGES;
```

> 注意 `slave_id`（配置里的 `RELAY_SLAVE_ID`）在集群内必须唯一；同一个 host 跑多个实例时尤其要改。

### 3.2 安装

```bash
composer install
cp .env.example .env      # 填入 MySQL / Redis 连接信息
php bin/relay check       # 部署自检
```

### 3.3 跑起来

```bash
php bin/relay start
# 另开窗口往库里写数据
mysql -e "INSERT INTO demo.orders VALUES (1, 99.50, 'new')"
```

标准输出（JSON Lines）：

```json
{"op":"insert","database":"demo","table":"orders","row":{"id":1,"amount":"99.50","status":"new"},"before":null,"table_key":["id"],"binlog_file":"mysql-bin.000003","binlog_pos":"4453","gtid":"","timestamp":1795013201,"server_id":1}
```

---

## 4. 配置

所有配置在 `config/relay.php`，全部支持环境变量覆盖。

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `RELAY_MYSQL_HOST` | 127.0.0.1 | 主库地址 |
| `RELAY_MYSQL_PORT` | 3306 | |
| `RELAY_MYSQL_USER` | repl | 复制账号 |
| `RELAY_MYSQL_PASSWORD` | | |
| `RELAY_SLAVE_ID` | 8811 | 从库 ID，**集群内唯一** |
| `RELAY_HEARTBEAT` | 30 | 心跳秒数，空闲保活 |
| `RELAY_DATABASES` | 空 | 只订阅这些库（逗号分隔） |
| `RELAY_TABLES` | 空 | 只订阅这些表 |
| `RELAY_IGNORE_DATABASES` | mysql,sys,... | 忽略的库 |
| `RELAY_CHECKPOINT` | file | `file` / `redis` |
| `RELAY_CHECKPOINT_KEY` | binlog_relay:checkpoint:default | Redis key |
| `RELAY_CHECKPOINT_EVERY` | 100 | 每 N 个事件落盘一次 |
| `RELAY_CHECKPOINT_SECONDS` | 1.0 | 或每 N 秒落盘一次 |
| `RELAY_BATCH_SIZE` | 200 | 每批交给 handler 的变更数 |
| `RELAY_LOG_LEVEL` | info | debug/info/warn/error |
| `RELAY_LOG_FILE` | 空 | 空则打 stderr |

> 点位存储选型：**多机部署或需要跨机器接管 → Redis**；单机/小规模 → 文件足够，零依赖。

---

## 5. 写业务 handler

### 5.1 最小示例：Redis 缓存失效

```php
<?php
// src/Handler/CacheInvalidateHandler.php
namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;

final class CacheInvalidateHandler implements HandlerInterface
{
    private \Redis $redis;

    public function __construct()
    {
        $this->redis = new \Redis();
        $this->redis->connect('127.0.0.1', 6379);
    }

    public function handle(array $changes): void
    {
        foreach ($changes as $c) {
            // orders:123 这种 key 约定
            $id = $c->row['id'] ?? null;
            if ($id === null) {
                continue;
            }
            $this->redis->del($c->table . ':' . $id);
        }
    }
}
```

注册到 `config/relay.php`：

```php
'handlers' => [
    BinlogRelay\Handler\CacheInvalidateHandler::class,
],
```

### 5.2 写数据库（幂等示例）

崩溃后会重放最后一批，**handler 必须幂等**：

```php
final class UpsertHandler implements HandlerInterface
{
    public function __construct(private \PDO $pdo) {}

    public function handle(array $changes): void
    {
        $this->pdo->beginTransaction();
        try {
            foreach ($changes as $c) {
                match ($c->op) {
                    Change::INSERT, Change::UPDATE => $this->upsert($c),
                    Change::DELETE => $this->delete($c),
                };
            }
            $this->pdo->commit();
        } catch (\Throwable $e) {
            $this->pdo->rollBack();
            throw $e;   // 抛出即停止推进点位，重启后重放
        }
    }

    private function upsert(Change $c): void
    {
        // 推荐：ON DUPLICATE KEY UPDATE，重放天然幂等
        $cols = array_keys($c->row);
        $sql = sprintf(
            'INSERT INTO mirror.%s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s',
            $c->table,
            implode(',', array_map(fn($x) => "`$x`", $cols)),
            implode(',', array_fill(0, count($cols), '?')),
            implode(',', array_map(fn($x) => "`$x`=VALUES(`$x`)", $cols))
        );
        $this->pdo->prepare($sql)->execute(array_values($c->row));
    }

    private function delete(Change $c): void
    {
        $where = implode(' AND ', array_map(fn($k) => "`$k`=?", $c->tableKey));
        $vals = array_map(fn($k) => $c->row[$k] ?? null, $c->tableKey);
        $this->pdo->prepare("DELETE FROM mirror.{$c->table} WHERE $where")->execute($vals);
    }
}
```

带构造参数的 handler 需要工厂，可在 `HandlerChain` 里注册实例：

```php
$chain->build([new UpsertHandler($pdo)]);
```

---

## 6. `Change` 结构

| 字段 | 说明 |
|---|---|
| `op` | `insert` / `update` / `delete` |
| `database`,`table` | 来源库表 |
| `row` | insert/delete 整行；update 为 **after** 行 |
| `before` | update 前的行，其他为 `null` |
| `tableKey` | 主键列名（来自 information_schema） |
| `binlogFile`,`binlogPos` | 该事件在 binlog 中的位置 |
| `gtid` | GTID（未启用时为空串） |
| `timestamp`,`serverId` | 该事件的源信息 |
| `rowIdentity()` | `db.table:pk1,pk2`，可直接做幂等键 |

二进制 / 非 UTF-8 列会被转成 `{"__binary__":"base64..."}`，避免 JSON 序列化失败。

---

## 7. 点位与一致性

```
事件到达 → 内存中记录最新 pos（不落盘）
        → 事务结束（XID）→ handler 处理整批
        → 每 100 个事件 / 1 秒 → 落盘一次
```

- **正常退出**：SIGTERM/SIGINT 只置标记，处理完当前批次后 flush + 释放锁
- **崩溃**：最多重放 `every_events` 个事件，handler 需幂等
- **想更严**：`RELAY_CHECKPOINT_EVERY=1`，代价是写放大（每秒几百次 fsync/HSET）

查看 / 重置：

```bash
php bin/relay status
php bin/relay reset-position --force   # 从最新 binlog 末尾重新开始
```

> `reset-position` 会先抢锁；如果 relay 还在跑会直接拒绝，避免把点位覆盖回去。

---

## 8. 性能

| 维度 | 建议 |
|---|---|
| 单实例吞吐 | 实测约 7k–8k events/s（PHP 8.4 / MySQL 5.7，历史基准）；行变更越大越慢 |
| handler 耗时 | 单批超过 1s 会打 WARN，说明要做批量写或异步化 |
| 大批量 | `RELAY_BATCH_SIZE=500` + handler 内 `INSERT ... VALUES (...),(...)` 批量 |
| IO | 默认每 100 事件落一次点位，比每事件落盘快 10–100 倍 |
| 内存 | 单批上限 8MB，超大事务自动切批；`memory_limit` 建议 512M |
| 网络 | MySQL 端 `net_read_timeout/net_write_timeout` 建议调到 3600 |

瓶颈通常不在解析而在 handler：**把逐条写改成批量写**，吞吐一般能翻 5–10 倍。

纵向扩展：一个实例一个库/一组表（`RELAY_DATABASES` 隔离），`slave_id` 各不相同。

---

## 9. 打包为单文件二进制

与 webman 的方式一致：**PHP 代码打成 phar + 静态 PHP 运行时（micro.sfx）合成一个可执行文件**。

### 9.1 构建运行时（只有在准备 micro.sfx 时做一次）

```bash
# 下载 spc（Linux x86_64 示例）
curl -fsSL -o spc https://dl.static-php.dev/static-php-cli/spc-bin/nightly/spc-linux-x86_64
chmod +x spc

# 构建带所需扩展的 micro 运行时
./spc build:php "sockets,bcmath,pdo_mysql,phar,pcntl,posix,redis" --build-micro
cp buildroot/bin/micro.sfx build/micro.sfx
```

> `sockets` 是必须的（binlog 复制协议走原生 socket），`bcmath` 是库要求的，`redis` 只有用 Redis 点位时才需要。
> Windows 构建需要 VS2022，且不能动态加载扩展，所有扩展必须静态编入。

### 9.2 打包

```bash
MICRO_SFX=build/micro.sfx ./build/build.sh
# 产物：dist/relay
```

脚本做的事：

1. `composer install --no-dev --classmap-authoritative`
2. 收集 `src/ config/ vendor/ plus bin/relay` 到 stage
3. `build/make-phar.php` 打成 `relay.phar`（GZ 压缩）
4. `spc micro:combine relay.phar --with-micro=micro.sfx --output=dist/relay`
5. 注入 ini：`memory_limit=512M`、`error_reporting=E_ALL & ~E_DEPRECATED` 等

### 9.3 部署

```bash
scp dist/relay server:/opt/binlog-relay/
scp -r config .env server:/opt/binlog-relay/    # 配置在外，二进制内
ssh server

cd /opt/binlog-relay
RELAY_CONFIG=./config/relay.php ./relay check
RELAY_CONFIG=./config/relay.php ./relay start
```

systemd：

```ini
[Unit]
Description=binlog-relay
After=network.target mysql.service

[Service]
Type=simple
WorkingDirectory=/opt/binlog-relay
EnvironmentFile=/opt/binlog-relay/.env
ExecStart=/opt/binlog-relay/relay start
Restart=always
RestartSec=3
TimeoutStopSec=30
KillSignal=SIGTERM
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

> `KillSignal=SIGTERM` + `TimeoutStopSec=30`：relay 收到信号后会处理完当前批次并落盘点位，不要用 `SIGKILL`。

---

## 10. 故障排查

| 现象 | 原因 / 处理 |
|---|---|
| `BinLogException: binlog format is not ROW` | 主库 `binlog_format` 必须是 `ROW` |
| 卡在 "未找到历史点位" 且没有事件 | 首次启动默认从**最新位置**开始，之后写入的才有事件 |
| `点位已被其它 relay 实例占用` | 另一个进程持有锁；确认无进程后 `reset-position --force` |
| 表名/列名看不懂或报错 | `binlog_row_metadata=FULL`；或在配置里固定 `RELAY_TABLES` |
| 反复重放同一批 | 崩溃后正常现象；handler 做幂等（`ON DUPLICATE KEY`） |
| 内存持续上涨 | 大事务 + 慢 handler；调小 `RELAY_BATCH_SIZE`，或检查 handler 是否有内存泄漏 |
| MySQL 8.4 报 `SHOW MASTER STATUS` 错误 | 库已适配（内部用 `SHOW BINARY LOG STATUS`） |
| Windows 下 phar 运行正常，二进制无法连接 | micro.sfx 构建时漏了 `sockets` 扩展 |

自检命令会逐项检查：连接、`log_bin`、`binlog_format`、`binlog_row_image`、复制权限、点位存储、handler 类是否存在。

---

## 11. 验证与基准

### 11.1 无外部依赖的测试（CI 可直接跑）

```bash
php tests/run-all.php
```

| 文件 | 覆盖内容 |
|---|---|
| `tests/selftest.php` | 点位批量落盘、锁互斥（第二个实例被拒）、过滤器、handler 异常传播 |
| `tests/EventBuilderTest.php` | 多行 insert 展开、update before/after 不串位、主键识别、二进制列降级、GTID 未初始化兜底 |
| `tests/HandlerTest.php` | HandlerChain 顺序/中断语义、RedisStreamHandler（有 Redis 时断言真实写入，无 Redis 自动跳过） |

全部不依赖 MySQL，`EventBuilderTest` 直接用真实 DTO 构造事件，可在提交前/流水线中执行。

### 11.2 需要环境的端到端验证

```bash
# MySQL（5.7/8.0 均可）
docker run -d --name relay-mysql -e MYSQL_ROOT_PASSWORD=root -p 33306:3306 \
  mysql:5.7 --log-bin=mysql-bin --server-id=1 --binlog-format=ROW --binlog-row-image=FULL

# 配好 .env 后
php bin/relay check
php bin/relay once --seconds=10 &
mysql -h127.0.0.1 -P33306 -uroot -proot demo \
  -e "INSERT INTO orders VALUES (1,99.5,'new'); UPDATE orders SET status='paid' WHERE id=1;"
```

预期：每条变更输出一行 JSON；进程退出后 `php bin/relay status` 显示新点位；
再次启动能从该点位续传（不回放已确认事件）。

### 11.3 框架自身开销基准

```bash
php tests/bench.php 20000
```

本机（PHP 8.4.8 / Windows）实测：

| 项目 | 吞吐 | 单次耗时 |
|---|---|---|
| EventBuilder（每批 10 行展开） | ~104,000 批/秒 | 9.6 µs/批（≈0.96 µs/行） |
| JSON 序列化（每条变更） | ~1,340,000 次/秒 | 0.74 µs |
| HandlerChain 调度（1 个空 handler） | ~4,980,000 次/秒 | 0.20 µs |
| FileStore 每次落盘（fsync+rename） | 57 次/秒 | 17.5 ms |

结论：

- 框架本身开销可忽略，**吞吐上限由 handler 的 IO 决定**
- FileStore 逐事件落盘极慢（机械盘/云盘 fsync 代价），这正是默认「每 100 事件或 1 秒批量落盘」的原因
- 追求极限吞吐时用 `StdoutHandler` 管道输出或 `RedisStreamHandler` pipeline，避免逐条落盘

---

## 12. 目录结构

```
binlog-relay/
├── bin/relay                 # CLI 入口：start / once / status / reset-position / check
├── config/relay.php          # 配置（支持环境变量覆盖）
├── src/
│   ├── Checkpoint/           # 点位：StoreInterface / RedisStore / FileStore / PositionManager
│   ├── Event/                # Change 契约、EventBuilder、Filter
│   ├── Handler/              # HandlerInterface、HandlerChain、StdoutHandler、RedisStreamHandler
│   ├── Relay/                # Relay 装配、Subscriber 订阅
│   └── Support/              # Logger、env()
├── build/                    # build.sh、make-phar.php
├── tests/                    # run-all / 单测 / bench
└── var/                      # 默认点位文件目录
```

## 13. 命令一览

```bash
php bin/relay check                     # 自检
php bin/relay start                     # 常驻消费
php bin/relay once --seconds=5          # 消费 5 秒后退出（联调）
php bin/relay status                    # 点位、持有者、累计事件
php bin/relay reset-position --force    # 重置点位

php tests/run-all.php                   # 全量测试
php tests/bench.php                     # 框架开销基准
```

## 14. 方式 A：relay → Redis Stream → 框架消费

`RedisStreamHandler` 已内置（`src/Handler/RedisStreamHandler.php`）：整批 pipeline 一次 RTT 写入，
使用近似裁剪 `MAXLEN ~ 200000`，防止消费端宕机时 Redis 无限增长。

```php
// config/relay.php
'handlers' => [
    new BinlogRelay\Handler\RedisStreamHandler('binlog:events'),
],
```

消费端（任意框架，裸 SQL 批量写，性能最优）：

```php
$redis = new Redis();
$redis->connect('127.0.0.1', 6379);
// 首次：$redis->xGroup('CREATE', 'binlog:events', 'cg:worker', '0', true);

while (true) {
    $res = $redis->xReadGroup('cg:worker', 'worker-1', ['binlog:events' => '>'], 1, 1000, 5000);
    if (!$res) {
        continue;
    }
    foreach ($res['binlog:events'] as $id => $fields) {
        $c = json_decode($fields['data'], true);
        // 批量 upsert（ON DUPLICATE KEY UPDATE 天然幂等）
    }
    $redis->xAck('binlog:events', 'cg:worker', array_keys($res['binlog:events']));
}
```

要点：

- **写库成功后再 XACK**：崩溃只会重放，不会丢
- `pos` 字段（`binlog_file:pos`）可直接作为幂等键
- 消费端用裸 SQL + 批量 values，不要逐条走 ORM（慢 3–10 倍）
- 同一 row 需要严格有序时，按 `pk` hash 分流到多条 stream 并行消费
