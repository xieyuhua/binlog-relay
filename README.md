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

- **单文件部署**：phar + micro.sfx 合成可执行文件，目标机器不用装 PHP，打包也不需要 spc
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
| `RELAY_FILE_FSYNC` | 0 | 点位文件是否 fsync；默认关闭（慢 1000 倍），需要更强持久性时开启 |
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

### 8.1 开销分布

单事件（10 行、约 4KB）的实测耗时构成：

| 环节 | 耗时 | 占比 |
|---|---|---|
| ① 网络读取 4KB | 51.5 µs | **86%** |
| ② EventBuilder 构建 Change[] | 8.3 µs | 14% |
| ③ 消息序列化 | 1.7 µs | — |

**网络占大头，但那是等待 MySQL 返回数据的时间，不是 CPU 计算。**
这意味着：换网络 IO 实现、换语言、上协程，都动不了这 86%。

调优方向因此很明确：**减少数据量** 比 **加快处理速度** 有效得多。

### 8.2 优化手段（按收益排序）

| 手段 | 预期收益 | 成本 |
|---|---|---|
| **列裁剪**（第 17 章） | 含大字段时队列体积降 90%+ | 改配置 |
| **`only_configured`**（第 19 章） | 未订阅表跳过解码与入队 | 改配置 |
| **减少事件数量**：`binlog_row_event_max_size` 调大 | 单次读更多，减少往返 | 改 MySQL 配置 |
| **handler 批量写**：多值 `INSERT` 替代逐条 | 5–10 倍 | 业务代码 |
| **批量 XADD**（已内置） | 一次 pipeline 写整批 | 已完成 |
| 换网络 IO 实现（swoole 等） | **< 3%** | 极高，见 8.4 |

```ini
# MySQL 端：默认值可能把大事务切成很多小块，调大可减少往返次数
[mysqld]
binlog_row_event_max_size = 8192
net_read_timeout  = 3600
net_write_timeout = 3600
```

### 8.3 参数建议

| 维度 | 建议 |
|---|---|
| 单实例吞吐 | 约 7k–8k events/s（PHP 8.4 / MySQL 5.7 基准）；行越大越慢 |
| handler 耗时 | 单批超过 1s 会打 WARN，说明要做批量写或异步化 |
| 大批量 | `RELAY_BATCH_SIZE=500` + 多值 INSERT |
| 点位 IO | 默认每 100 事件落一次，比逐事件落盘快 10–100 倍 |
| 内存 | 单批上限 8MB，超大事务自动切批；`memory_limit` 建议 512M |

纵向扩展：一个实例订阅一组库/表（`RELAY_DATABASES` / `RELAY_TABLES` 隔离），
`slave_id` 与 `RELAY_CHECKPOINT_KEY` 各不相同。

### 8.4 为什么不用 swoole 替换 sockets 扩展

结论：**不会提升性能，不建议换。** 实测数据：

| 实现 | 吞吐 | 单次耗时 |
|---|---|---|
| `sockets` 扩展（当前实现） | **19,424 次/秒** | 51.5 µs |
| `stream_socket` | 18,803 次/秒 | 53.2 µs |

（本地 TCP 回环，4KB 报文，2 万次往返。swoole 底层做的是同样的事，没有额外的优化空间。）

数据可复现：

```bash
php tests/netbench.php          # 默认 20000 次 × 4096 字节
php tests/netbench.php 50000 8192
```

四个原因：

**1. 瓶颈是等待，不是 syscall**
网络读取的 51.5 µs 里绝大部分在等 MySQL 返回数据。swoole 能省下的只是发起调用的几微秒。

**2. binlog 协议天然串行**
单连接、顺序读、顺序解析、顺序入队。swoole 的优势在**高并发连接**（数千客户端），
而 relay 只维护 **1 个连接**，协程调度派不上用场。

**3. 当前读法已是最优**
```php
socket_recv($this->socket, $buf, $length, MSG_WAITALL);
```
`MSG_WAITALL` 让内核等到凑满指定字节才返回，一次 syscall 拿完整报文。

**4. 引入成本很实在**

| 问题 | 影响 |
|---|---|
| 静态编译 | 二进制从 19.5 MB 涨到 40 MB 左右 |
| Windows | swoole 不支持 Windows，打包流程失效 |
| phar 兼容 | swoole 事件循环与 phpmicro 初始化顺序冲突 |
| 代码改造 | 需在协程容器内运行，改动扩散到整个启动流程 |
| 回归成本 | 内嵌解析库假定同步阻塞模型，每处都要重新验证 |

**唯一值得考虑的场景**：一台机器要跑几十个 relay 实例。但当前架构支持
单实例订阅多库 + `MultiStreamHandler` 扇出多 topic，本来就不需要多实例。
真要扩到那个量级，正确做法是**多机部署**（每台 2–3 个实例），而不是把单进程改成协程。

---

## 9. 打包为单文件二进制

与 webman 的方式一致：**PHP 代码打成 phar + 静态 PHP 运行时（micro.sfx）合成一个可执行文件**。

### 9.1 准备运行时（自动下载，无需 spc）

`build.sh` 会**自动下载 workerman 官方提供的 micro.sfx**，无需任何额外工具：

```bash
./build/build.sh
# ==> 本地无 micro.sfx，尝试下载：https://download.workerman.net/php/php8.4.micro.sfx
# ==> 下载完成：build/micro.sfx (20M)
```

已实测该运行时包含本项目所需的全部扩展：

| 必需 | 说明 |
|---|---|
| `sockets` | binlog 复制协议走原生 socket，**缺了无法连接主库** |
| `bcmath` | 解析库用于 binlog 位置的数值比较 |
| `json` | 消息序列化 |
| `pdo` + `pdo_mysql` | 读 information_schema 拿列定义与主键 |
| `redis` | 队列写入与 Redis 点位 |
| `phar` | 运行时加载打包进二进制的代码 |
| `pcntl` / `posix` | 优雅退出（SIGTERM） |

其余（`curl`/`openssl`/`mbstring`/`filter`/`tokenizer`/`ctype`）也都在，POST/HTTP Sink 可直接用。

可选的版本/地址覆盖：

```bash
PHP_VERSION=8.3 ./build/build.sh          # 换 PHP 版本
MICRO_URL=https://your/mirror.sfx ./build/build.sh
MICRO_SFX=/path/to/local.sfx ./build/build.sh   # 用本地文件，不下载
```

> **平台限制（重要）**：workerman 提供的这些 micro.sfx 是 **Linux x86-64** 静态二进制。
> 在 Windows/macOS 上打包得到的是 Linux 产物，需要拷到 Linux 运行；
> 若需要 Windows/macOS 原生可执行文件，得自己用 spc 构建对应平台的 micro.sfx。
>
> 合成脚本会按 micro.sfx 的魔数判断目标平台：Linux 产物不加 `.exe` 后缀，
> 并在输出里提示「这是 Linux x86-64 产物」。

### 9.2 打包

```bash
./build/build.sh                    # 三个入口全打
./build/build.sh relay              # 只打生产端
./build/build.sh consumer           # 只打消费端
./build/build.sh binlog2redis       # 只打单体服务
MICRO_SFX=/path/to/micro.sfx ./build/build.sh   # 指定其它位置的 micro.sfx
```

产物：

```
dist/relay          生产端二进制
dist/consumer       消费端二进制
dist/binlog2redis   单体服务二进制
dist/*.phar         对应的 phar（无 micro.sfx 时只有这个）
```

下载失败（离线/网络受限）时脚本仍会生成 phar，可直接用系统 PHP 运行：

```bash
php dist/relay.phar status
php dist/binlog2redis.phar check
```

实测产物：`dist/relay` 约 19.5 MB（micro.sfx 19.35 MB + 代码 113 KB），
在 Linux 容器内 `./relay help` 正常输出。

脚本做的事：

1. `composer install --no-dev --classmap-authoritative`（只为生成 autoload）
2. 收集 `src/ config/ vendor/` 到公共 stage（三个入口共用，只做一次）
3. `build/make-phar.php <stage> <phar> <entry>` 打出各自的 phar
4. `build/micro-combine.php` 把 micro.sfx + ini + phar 拼成一个文件（纯 PHP，无 spc）
5. 注入 ini：`memory_limit=512M`、`error_reporting=E_ALL & ~E_DEPRECATED` 等

### 9.2.1 合成格式（为什么不需要 spc）

`spc micro:combine` 做的事其实只是字节拼接，格式如下：

```
[ micro.sfx 原始内容 ]
[ 可选 ini 段：\xfd\xf6\x69\xe6 + uint32 大端长度 + ini 文本 ]
[ phar 或 php 脚本内容 ]
```

`build/micro-combine.php` 用 30 行 PHP 实现了同样的逻辑，因此**打包环节不再依赖 spc**：

```bash
# 也可以单独调用
php build/micro-combine.php dist/relay.phar dist/relay \
    --micro=build/micro.sfx \
    --ini="memory_limit=512M" \
    --ini="display_errors=stderr"
```

验证合成结果是否正确：

```bash
# 检查 ini 段魔数与内容
php -r '$d=file_get_contents("dist/relay");$p=strpos($d,"\xfd\xf6\x69\xe6");
$n=unpack("N",substr($d,$p+4,4))[1];echo "sfx长度=$p\n";echo substr($d,$p+8,$n),"\n";'
```

两个入口的 phar stub 指向不同 `entryFile`（`relay.php` / `consumer.php`），
所以 `./consumer status` 和 `./relay status` 是两套独立命令，不会混淆。

### 9.3 部署

```bash
# 生产端（读 binlog）
scp dist/relay server:/opt/binlog-relay/
scp -r config .env server:/opt/binlog-relay/    # 配置在外，二进制内
ssh server && cd /opt/binlog-relay
RELAY_CONFIG=./config/relay.php ./relay check
RELAY_CONFIG=./config/relay.php ./relay start

# 消费端（可以部署到另一台机器，只需要能连 Redis 与下游）
scp dist/consumer consumer-host:/opt/binlog-consumer/
scp config/consumer.php consumer-host:/opt/binlog-consumer/config/
ssh consumer-host && cd /opt/binlog-consumer
CONSUMER_CONFIG=./config/consumer.php ./consumer check
CONSUMER_CONFIG=./config/consumer.php CONSUMER_SINK=mysql ./consumer start
```

> 消费端不依赖 MySQL 主库，只依赖 Redis 队列与它要写入的下游（MySQL/CK/ES/HTTP/TCP）。
> 所以一台机器可以只装消费端，另一台（更靠近主库的）只装生产端。

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
| EventBuilder（每批 10 行展开） | ~135,000 批/秒 | 7.4 µs/批（≈0.74 µs/行） |
| JSON 序列化（每条变更） | ~1,300,000 次/秒 | 0.75 µs |
| HandlerChain 调度（1 个空 handler） | ~5,000,000 次/秒 | 0.20 µs |
| FileStore 落盘（rename，默认） | 1,524 次/秒 | 0.66 ms |
| FileStore 落盘（fsync+rename） | 55 次/秒 | 18 ms |

结论：

- 框架本身开销可忽略，**吞吐上限由 handler 的 IO 决定**
- `fsync` 默认关闭：云盘单次 fsync 约 18ms，是逐事件落盘慢的主因。关闭后崩溃最多丢最后一批点位，handler 幂等即可覆盖；需要更强持久性时设 `RELAY_FILE_FSYNC=1`
- 追求极限吞吐时用 `StdoutHandler` 管道输出或 `RedisStreamHandler` pipeline

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

**生产端**

```bash
php bin/relay check                     # 自检
php bin/relay start                     # 常驻消费 binlog
php bin/relay once --seconds=5          # 消费 5 秒后退出（联调）
php bin/relay status                    # 点位、持有者、累计事件
php bin/relay reset-position --force    # 重置点位
```

**消费端**

```bash
php bin/consumer check                     # Redis + sink 连通性自检
php bin/consumer create-group              # 创建 consumer group
php bin/consumer start                     # 常驻消费
php bin/consumer once --seconds=3          # 消费 3 秒后退出（联调）
php bin/consumer status                    # 长度 / pending / lag
php bin/consumer groups                    # 所有 group 与消费者
php bin/consumer retry-dlq --limit=100     # 死信重投主队列
```

**其他**

```bash
php tests/run-all.php                   # 全量测试
php tests/bench.php                     # 框架开销基准（EventBuilder / 序列化 / 点位）
php tests/netbench.php                  # 网络 IO 基准（sockets vs stream_socket）
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

---

## 15. 消费端架构（`bin/consumer`）

生产端只负责写队列，消费端独立进程、独立部署、独立扩缩容。

```
relay（1 个）                    consumer（N 个，按需扩）
┌──────────────┐   XADD     ┌──────────────────────────────┐
│ binlog 解析   │ ────────► │ Redis Stream                 │
└──────────────┘  pipeline  │  binlog:events               │
                            │  binlog:events:dead (DLQ)    │
                            └──────────────┬───────────────┘
                                           │ XREADGROUP
                        ┌──────────────────┼──────────────────┐
                        ▼                  ▼                  ▼
                   consumer-1         consumer-2         consumer-3
                   HTTP 推送          MySQL upsert       ES _bulk
```

### 15.1 消息格式（生产端与消费端共用契约）

Redis Stream entry 字段：

| 字段 | 含义 |
|---|---|
| `op` | `insert` / `update` / `delete` |
| `db` / `tbl` | 源库 / 源表 |
| `pk` | `db.table:主键值`，直接当幂等键 |
| `pos` | `binlog_file:pos`，用于审计与去重 |
| `ts` | 事件时间戳（秒） |
| `data` | 完整 Change 的 JSON（含 `row` / `before` / `table_key`） |

契约代码：`src/Queue/Message.php`。`data` 损坏时消费端会用扁平字段降级重建，老消息也能消费。

### 15.2 可靠性语义

- **至少一次**：`XACK` 在 sink 写入成功之后
- **原地重试**：失败后同批指数退避重试（默认 2s → 4s → 8s…最多 30s），不立即丢回队列
- **死信**：连续失败 `max_retries` 次后转 `binlog:events:dead` 并 ACK，避免一条坏消息卡死整个 group
- **接管遗留**：启动/空闲时用 `XAUTOCLAIM` 收回超过 `claim_min_idle_ms` 未确认的消息
- **幂等前提**：以上任意环节重放，都需要 sink 端按 `pk` 幂等（`ON DUPLICATE KEY` / ES `_id` / CK ReplacingMergeTree）

### 15.3 五种 Sink

配置 `config/consumer.php` 的 `sink.driver`：

```bash
CONSUMER_SINK=mysql CONSUMER_MYSQL_DB=mirror php bin/consumer start
```

| driver | 写入方式 | 幂等机制 | 适用 |
|---|---|---|---|
| `http` | 整批 POST `{events:[...]}` | 接收方按 `pk`+`pos` 去重 | 对接任意业务接口 |
| `tcp` | 长连接 + `[4B 长度][JSON]` 帧，服务端回 `0x06` | 接收方幂等 | 内网高吞吐、跨语言 |
| `mysql` | 按表分组，一条多值 `INSERT ... ON DUPLICATE KEY UPDATE` | 主键覆盖 | 镜像库、数仓 ODS |
| `clickhouse` | HTTP `INSERT ... FORMAT JSONEachRow`，按表分批 | ReplacingMergeTree（主键去重） | 大表分析、日志 |
| `elasticsearch` | `_bulk`，按体积切分（默认 5MB/请求） | `_id` = 主键值 | 搜索索引同步 |

**MySQL Sink 注意事项**

- 目标表名前缀默认 `mirror_`：源 `orders` → `mirror_orders`
- 目标表主键必须与源表一致，否则 `ON DUPLICATE KEY UPDATE` 会插重复
- `delete` 走 `WHERE 主键 IN (...)`；无主键表会直接抛错，请改用过滤规则跳过
- 二进制列自动 base64 解码回原始字节

**ClickHouse Sink 注意事项**

- 删除默认走软删：写 `_is_deleted=1`，目标表需有该列（`delete_mode=skip` 可忽略删除）
- 查询时用 `FINAL` 或按版本列去重
- 表结构需要自己建；CK 不做在线 schema 变更

**HTTP / TCP 的接收端契约**

```json
{ "events": [
  { "id":"1700000000000-0", "op":"update", "db":"shop", "tbl":"orders",
    "pk":"shop.orders:7", "pos":"mysql-bin.000012:4321", "ts":1795013415,
    "data": { "row": {...}, "before": {...}, "table_key": ["id"] } }
] }
```

接收方返回 2xx 即视为成功。4xx 会立刻失败（不重试），5xx 由消费端按退避重试。

### 15.4 扩缩容

```bash
# 多个消费者共享同一个 group：消息只会投递给其中一个
CONSUMER_NAME=worker-1 php bin/consumer start
CONSUMER_NAME=worker-2 php bin/consumer start
```

- 同一 `group` 下所有消费者**分摊**消息
- 不同业务用**不同 stream + 不同 group**，互不影响
- 容器里 `CONSUMER_NAME` 用 `$HOSTNAME`，避免重启后 pending 归属混乱

### 15.5 排障

| 现象 | 处理 |
|---|---|
| `lag` 持续增长 | 消费端处理不过来：加消费者，或把 sink 改成批量写 |
| `pending` 堆积且 `idle` 很大 | 消费者掉线/卡住；确认进程存活，`claim_min_idle_ms` 内会自动接管 |
| 一直重试同一批 | 看日志里的 sink 错误；修好后会自动恢复，或等它进死信 |
| 死信里堆消息 | 修复数据/下游后 `php bin/consumer retry-dlq --limit=500` |
| 消费端重启后重复处理 | 正常（至少一次）；确认 sink 幂等 |

---

## 16. 一条数据分发到多个业务 topic

需求：**同一个 binlog 事件要同时进入多个业务队列**（订单业务、库存业务、搜索索引、审计…），且各业务互不影响。

### 16.1 推荐架构

```
                       ┌── topic:order   → 订单 consumer  → MySQL/CK
relay ──MultiStream────┼── topic:stock   → 库存 consumer  → Redis/MySQL
      （一次 pipeline） └── topic:search  → 搜索 consumer  → ES
```

`MultiStreamHandler`（`src/Handler/MultiStreamHandler.php`）在**生产端**做扇出：

- 一个 Redis 连接，整批所有 topic 的 XADD 走**同一个 pipeline**，只多花一次网络往返
- 每条变更可命中**多条规则**，写入多个 stream
- 各 topic 独立 consumer group、独立 lag，一个业务卡住不影响其它业务

### 16.2 配置（`config/relay.php`）

```php
'handlers' => [
    new BinlogRelay\Handler\MultiStreamHandler([
        // 订单业务：订单主表与明细
        ['stream' => 'topic:order', 'tables' => ['orders', 'order_items']],

        // 库存业务：库存变化
        ['stream' => 'topic:stock', 'tables' => ['stock', 'stock_log']],

        // 搜索索引：全表（* 表示不筛表）
        ['stream' => 'topic:search', 'tables' => ['*']],

        // 审计：只要删除操作
        ['stream' => 'topic:audit', 'tables' => ['orders', 'order_items'], 'ops' => ['delete']],

        // 只同步某个库的某些表
        ['stream' => 'topic:crm', 'databases' => ['crm'], 'tables' => ['customers']],
    ], null, [
        'host'     => env('RELAY_REDIS_HOST', '127.0.0.1'),
        'port'     => (int) env('RELAY_REDIS_PORT', 6379),
        'password' => env('RELAY_REDIS_PASSWORD', ''),
        'database' => (int) env('RELAY_REDIS_DB', 0),
    ]),
],
```

规则匹配逻辑（`MultiStreamHandler::matches`）：

| 条件 | 未配置 | 配置后 |
|---|---|---|
| `databases` | 不限库 | 库名必须命中 |
| `tables` | 不限表 | 表名必须命中；`['*']` 等价于不限 |
| `ops` | 不限操作 | 操作必须命中 |

### 16.3 启动各业务的消费端

每个业务一个进程、一个 group：

```bash
# 订单业务写到 MySQL 镜像库
CONSUMER_STREAM=topic:order CONSUMER_GROUP=cg:order \
CONSUMER_NAME=order-1 CONSUMER_SINK=mysql \
CONSUMER_MYSQL_DB=mirror php bin/consumer start

# 搜索业务写 ES
CONSUMER_STREAM=topic:search CONSUMER_GROUP=cg:search \
CONSUMER_NAME=search-1 CONSUMER_SINK=elasticsearch \
CONSUMER_ES_URL=http://es:9200 php bin/consumer start

# 审计只落 CK
CONSUMER_STREAM=topic:audit CONSUMER_GROUP=cg:audit \
CONSUMER_NAME=audit-1 CONSUMER_SINK=clickhouse \
CONSUMER_CK_DB=audit php bin/consumer start
```

看各业务积压：

```bash
CONSUMER_STREAM=topic:order  php bin/consumer status
CONSUMER_STREAM=topic:stock  php bin/consumer status
CONSUMER_STREAM=topic:search php bin/consumer status
```

### 16.4 对业务影响最小化的 5 个要点

1. **按业务拆 topic，不要共用一条 stream**
   共用时一个消费慢的业务会拖高整条队列的 lag，且无法独立扩缩容。

2. **只订阅需要的表和操作**
   `MultiStreamHandler` 的 `tables` / `ops` / `databases` 在生产端就过滤掉，消费端不必自己判断，省 IO 也省带宽。

3. **扇出失败要整体失败**
   `MultiStreamHandler` 只要有一条 XADD 失败就抛异常 → relay 停止推进点位 → 重启后整批重放。
   这样不会出现"订单 topic 有数据、库存 topic 没有"的长期不一致（短暂窗口由下游幂等覆盖）。

4. **每个业务独立 group，独立消费者名**
   不要两个业务共用一个 group（会互相抢消息）；容器里 `CONSUMER_NAME=$HOSTNAME`。

5. **MySQL/CK 走批量，ES 走 _bulk**
   逐条写入是吞吐杀手；`MysqlSink` 已按表合并成一条多值 SQL，`ElasticsearchSink` 按 5MB 切 _bulk。

### 16.5 优先级与隔离（进阶）

如果需要"订单业务必须实时、搜索可以延迟"，不要靠调整消费速度，而是：

- **给关键业务单独部署 relay**（`RELAY_DATABASES` / `RELAY_TABLES` 隔离），物理隔离最彻底
- 或者**至少拆 topic**：先按 16.2 配好，再给关键 topic 多开消费者实例
- 极端场景下可以用 `XADD MAXLEN` 给非关键 topic 设更小的裁剪长度，避免磁盘被非关键业务撑爆

### 16.6 吞吐与成本权衡

| 方案 | 网络往返 | 可靠性 | 复杂度 |
|---|---|---|---|
| 单 stream + 多 sink（进程内） | 1 | 一损俱损 | 低 |
| **多 topic 扇出（推荐）** | 1（pipeline） | 独立 | 中 |
| 每个业务单独跑 relay | N | 完全隔离 | 高 |

`MultiStreamHandler` 的实测开销：一条变更命中 4 个 topic 时，多出 3 次 XADD 的 CPU 开销，
但因为走同一个 pipeline，网络往返仍是 1 次，整体吞吐相比单 topic 下降约 10–15%。

---

## 17. 列裁剪（丢弃不需要的字段）

binlog 行事件默认带整行数据，大字段（TEXT/BLOB/JSON）往往占消息体积的 90% 以上，
而业务可能只需要 `id` + `status`。在**入队前**裁掉不需要的列，能同时降低队列内存、
网络传输、消费端 JSON 解析和下游写入成本。

### 17.1 配置

`config/relay.php`：

```php
'filter' => [
    'columns' => [
        // 订单表只要这几列
        'orders' => ['id', 'status', 'amount', 'updated_at'],

        // 文章表丢掉 content 大字段
        'cms_article' => ['id', 'title', 'status', 'updated_at'],

        // 其余表只要主键（适合只关心"有没有变"的场景）
        '*' => ['id'],
    ],
],
```

规则：

| 项 | 说明 |
|---|---|
| 未配置的表 | 原样保留全部列 |
| `'*'` | 作为兜底，作用于所有未显式配置的表 |
| 主键列 | **永远保留**，即使没写进配置（否则下游无法幂等） |
| `before` 行 | 同样裁剪；下游做字段 diff 时注意被裁掉的列不会出现 |
| 空配置 | 完全不生效，零开销（验证见 `tests/ProjectorTest.php`） |

### 17.2 实测收益

| 行数据 | 完整体积 | 裁剪后 | 保留比例 |
|---|---|---|---|
| 窄表（8 列） | 103 B | 44 B | 43% |
| 含 TEXT（~8KB） | 24,089 B | 24,089 B | 100%（该列在保留列表里） |
| 含 BLOB（20KB） | 20,072 B | 60 B | **0.3%** |
| 含 JSON（600B） | 600 B | 44 B | 7% |

结论：**含大字段的表收益最大**。如果一张表有 `content`/`avatar`/`meta` 这类列，
只要业务不需要，裁掉后队列体积能降两个数量级。

### 17.3 注意事项

- 裁剪发生在 relay 侧，**所有** handler/topic 拿到的都是裁剪后的数据。如果不同业务需要不同列，
  有两种做法：
  1. 取所有业务的并集（简单，但队列体积由最宽的业务决定）
  2. 分多个 relay 实例，各自配置不同的 `columns`（隔离彻底，成本更高）
- 下游如果依赖 `before` 做字段级 diff，裁剪后只能看到保留列的差异
- 列裁剪不影响 `pk` / `pos` / `tableKey`，幂等键始终完整

---

## 18. 每个业务独立解析 binlog 是否更快？

**结论：通常不会更快，反而更慢；但在少数场景下值得做。** 下面是依据。

### 18.1 为什么"多实例各自解析"不划算

binlog 复制协议是**串行流**，每个实例都是一个独立的从库连接：

| 成本项 | 单实例 | N 个实例 |
|---|---|---|
| MySQL 侧连接 | 1 个 dump 线程 | N 个 dump 线程，每个都要维护 binlog 缓存 |
| 网络传输 | binlog 只传一遍 | **binlog 传 N 遍**（这是最大头，尤其大字段多时） |
| 解析 CPU | 解析一遍 | 解析 N 遍，但**每个实例只解析自己关心的表**，可用 `RELAY_TABLES` 过滤 |
| 内存 | 一份 table cache | N 份 |
| 运维 | 1 个点位 | N 个点位，N 个故障点 |

关键在于第二行：**binlog 从主库读出来是通过网络传输的**。一个实例里做扇出（`MultiStreamHandler`），
binlog 只传一次；N 个实例则是 N 倍传输。如果 binlog 里大字段多、写入量大，这个成本非常可观。

另外，krowinski 库的 `withTablesOnly()` 过滤是在 **TABLE_MAP 解析阶段**生效的——
命中过滤的表，连 `information_schema` 查询和行数据解码都会跳过。
但**字节仍然要从网络读入**，所以过滤只省 CPU，不省带宽。

### 18.2 什么时候确实该拆多个实例

| 场景 | 建议 |
|---|---|
| 某些业务对延迟极敏感（订单），其余可容忍秒级 | 拆分：关键业务单实例 + `RELAY_TABLES` 只订阅关键表 |
| 总量超过单实例处理能力（实测 > 6–7k events/s） | 拆分：按表分片，各实例订阅不同表集合 |
| 需要不同裁剪规则（A 业务要全字段，B 只要主键） | 拆分：各自配置 `filter.columns` |
| 需要独立发布/重启（一个业务改动不影响其它） | 拆分：进程级隔离 |
| 只是想让不同业务互不影响队列 | **不必拆分**，用 `MultiStreamHandler` 分 topic 即可 |

### 18.3 实测参考

| 场景 | 相对吞吐 | 说明 |
|---|---|---|
| 单实例 + 4 topic 扇出 | ~85–90% | 多 3 次 XADD，仍是一次 pipeline（实测口径有限，以实际压测为准） |
| 单实例 + 列裁剪 | 与不裁剪持平或略快 | 收益主要在下游和网络，不在 relay CPU |
| N 个实例各自解析 | 主库侧负载 ×N | 每个实例都要一份 binlog 网络传输 |
| N 个实例各自订阅不同表 | 每个实例 CPU 下降 | 字节仍要从网络读入，带宽不省 |

### 18.4 推荐的默认架构

```
                    ┌─ 关键业务 relay（RELAY_TABLES=orders,order_items）
                    │      └─ topic:order ──► 订单消费端
MySQL ──binlog──► 1 个 relay
                    │      └─ MultiStreamHandler 扇出
                    └─ 其它业务 topic（库存/搜索/审计）
                           └─ topic:stock / topic:search / topic:audit
```

**先单实例 + 扇出，遇到下面任一情况再拆：**

1. 单个 relay 的 CPU 持续 > 70%（用 `top` 看）
2. 端到端延迟（`pos` 时间戳 vs 消费时间）持续超过业务 SLA
3. 不同业务对列裁剪的需求冲突到无法取并集
4. 需要独立发布窗口

拆分时用 `RELAY_TABLES` 让每个实例只订阅自己的表，能把 CPU 降下来；
带宽仍然会翻倍，所以拆分数量控制在 2–3 个为宜。

### 18.5 如果真要拆分，怎么配

```bash
# 实例 1：订单（关键路径）
RELAY_TABLES=orders,order_items RELAY_SLAVE_ID=8801 \
RELAY_CHECKPOINT_KEY=relay:orders php bin/relay start

# 实例 2：其它业务
RELAY_TABLES=stock,stock_log,products RELAY_SLAVE_ID=8802 \
RELAY_CHECKPOINT_KEY=relay:catalog php bin/relay start
```

要点：

- `RELAY_SLAVE_ID` **必须不同**，否则 MySQL 会认为两个从库是同一个，可能互相踢掉
- `RELAY_CHECKPOINT_KEY` 必须不同，各自维护自己的点位
- 表集合不要重叠，否则同一变更会被写入两次（下游需幂等）

---

## 19. 只处理已配置的表（`only_configured`）

默认情况下，`tables` / `columns` 没提到过的表会**原样透传**给 handler。
如果业务只关心少数几张表，其余表既不消费也占队列空间，可以开启白名单模式：

```php
'filter' => [
    'tables' => ['orders', 'order_items'],
    'columns' => [
        'users' => ['id', 'name', 'phone'],   // 只在 columns 里出现也算"已配置"
    ],
    'only_configured' => true,
],
```

或环境变量：

```bash
RELAY_ONLY_CONFIGURED=1 php bin/relay start
```

### 19.1 判定规则

| 表 | 结果 |
|---|---|
| 在 `tables` 里 | ✅ 处理 |
| 在 `columns` 里 | ✅ 处理（即使不在 `tables`） |
| 两张表都不在 | ❌ 整表丢弃 |
| 在 `ignore_tables` 里 | ❌ 丢弃（忽略规则优先级最高） |

已配置表清单 = `tables` ∪ `columns` 的表名，**并集**而不是交集——
这样"我只想给 users 配裁剪规则"不会因为没写进 `tables` 而被误杀。

`'*'` 通配不参与这个清单（它表示"其它表用这套规则"），所以不能靠 `'*'` 让所有表都算已配置。

### 19.2 性能上的实际收益

开启后，未配置表的过滤会**下推到 TABLE_MAP 阶段**：

| 环节 | 未开启 | 开启 |
|---|---|---|
| 读取 binlog 字节 | ✅ | ✅（省不掉，必须顺序读） |
| 解析 TABLE_MAP | ✅ | ✅ |
| 查 `information_schema` 拿列定义 | ✅ | ❌ 跳过 |
| 解码行数据（大字段最贵） | ✅ | ❌ 跳过 |
| 构造 Change 对象 | ✅ | ❌ 跳过 |
| 入队 / 网络传输 | ✅ | ❌ 跳过 |

所以：**省 CPU、省内存、省队列带宽，但不省主库到 relay 的 binlog 传输带宽**。
如果 binlog 里大字段多、写入量大，开启后 CPU 下降通常很明显。

### 19.3 注意

- 开启后如果 `tables` 和 `columns` 都为空，等于**丢弃所有表**（配置里已注明，`relay check` 不会自动拦截，请自行确认）
- 新上线的表如果业务需要，记得同时加进 `tables` 或 `columns`，否则会被静默丢弃
- 与 `ignore_tables` 同时配置时，忽略规则优先

---

## 20. 零依赖单体：`bin/binlog2redis`

如果你只想要一件事——**读 binlog，按配置过滤，写 Redis**——用这个入口。
它把 binlog 解析库完整内嵌进项目，不需要 Composer，不需要 vendor 目录。

### 20.1 为什么可以不要 vendor

原依赖 krowinski/php-mysql-replication 共 80 个文件、7157 行，外部依赖只有 5 个，
且用法都很浅，已全部用内置实现替换：

| 原依赖 | 用途 | 替换方案 |
|---|---|---|
| `doctrine/dbal` | MySQL 连接、查 information_schema | `src/Replication/Support/PdoConnection.php`（PDO） |
| `doctrine/collections` | 3 个 Collection 类 | `src/Replication/Support/Collection.php` |
| `symfony/event-dispatcher` | 事件分发 | `src/Replication/Support/EventDispatcher.php` |
| `psr/log` | 日志接口 | `src/Replication/Support/LoggerInterface.php` |
| `psr/simple-cache` | 表结构缓存接口 | `src/Replication/Support/CacheInterface.php` |

命名空间从 `MySQLReplication\` 改为 `BinlogRelay\Replication\`，与项目统一。
`src/bootstrap.php` 是一个 40 行的 PSR-4 自动加载器，**存在 vendor 时优先用 composer，
不存在时自动回退到内置加载**，两种方式都不用改代码。

### 20.2 目录

```
src/Replication/         内嵌的 binlog 解析器（63 个文件）
src/Standalone/          单体服务组件
  StreamWriter.php       Change[] → XADD（pipeline）
  RelaySubscriber.php    过滤 → 归一化 → 裁剪 → 攒批
  Runner.php             装配 MySQLReplicationFactory
src/bootstrap.php        零依赖自动加载器
bin/binlog2redis         单文件入口
config/binlog2redis.php  配置
```

### 20.3 运行

```bash
# 不需要 composer install
php bin/binlog2redis check          # 自检 MySQL / Redis / 点位
php bin/binlog2redis start          # 前台运行
php bin/binlog2redis status         # 查看点位
```

依赖只有 PHP 扩展：`sockets`（binlog 协议）、`bcmath`、`json`、`redis`、`pdo_mysql`。

### 20.4 最小配置

```php
// config/binlog2redis.php
return [
    'mysql' => [
        'host' => '127.0.0.1', 'port' => 3306,
        'user' => 'repl', 'password' => '***',
        'slave_id' => 8811,          // 集群内唯一
    ],
    'filter' => [
        'databases' => ['shop'],
        'only_configured' => true,   // 只处理下面出现的表
        'columns' => [
            'orders' => ['id', 'status', 'amount'],   // 其余列丢弃
        ],
    ],
    'redis' => [
        'host' => '127.0.0.1', 'port' => 6379,
        'stream' => 'binlog:events',
    ],
    'checkpoint' => [
        'driver' => 'redis',         // 或 file
        'key' => 'relay:shop',
    ],
];
```

全部配置项都可以用环境变量覆盖，详见 `config/binlog2redis.php` 注释。

### 20.5 实测（MySQL 5.7 + 内嵌解析器）

已在真实 binlog 上验证：

```
insert demo.orders:333   ✅ 行数据完整（含 TEXT 字段）
update demo.orders:333   ✅ before/after 都正确
delete demo.orders:333   ✅
```

列裁剪 + 多 topic 扇出同时验证：

```
配置：columns.orders = [id, status]
      topics = [topic:order (全部), topic:audit (仅 delete)]
结果：
  topic:order  insert  row={'id':444,'status':'new'}   ← note 已被裁掉
  topic:order  delete  row={'id':444,'status':'new'}
  topic:audit  delete  row={'id':444,'status':'new'}   ← 只收 delete
```

### 20.6 与完整框架的关系

| | `bin/binlog2redis` | `bin/relay` + `bin/consumer` |
|---|---|---|
| 依赖 | 零依赖 | 零依赖（同样内置） |
| 输出 | 仅 Redis Stream | Redis Stream / HTTP / TCP / MySQL / CK / ES |
| 层级 | 生产端+入队合一 | 生产端与消费端分离 |
| 适用 | 只要 binlog → Redis | 需要多下游、多消费组 |

两者共用同一套 `src/Replication`、`EventBuilder`、`ColumnProjector`、`Filter`，
行为一致，可以先用单体跑通再迁移到完整框架。

### 20.7 打包为单文件

```bash
# 加入打包脚本（复用同一 micro.sfx）
./build/build.sh all
# 手动打包单体入口：
php -d phar.readonly=0 build/make-phar.php dist/stage-binlog2redis dist/binlog2redis.phar binlog2redis
```

---

## 21. SQL 在哪端构建？主键怎么处理？

### 21.1 结论：SQL 在消费端构建

本项目的分工：

```
解析端（relay / binlog2redis）          消费端（consumer + Sink）
  读 binlog                                MysqlSink  → INSERT ... ON DUPLICATE KEY UPDATE
  归一化成 Change（结构化 JSON）            ClickHouseSink → JSONEachRow
  过滤 / 裁剪列                             ElasticsearchSink → _bulk
  写 Redis Stream                          HttpSink → 业务接口（对方拼 SQL）
```

| 维度 | 解析端拼 SQL | 消费端拼 SQL（本项目采用） |
|---|---|---|
| 多目标 | 每个下游一套 SQL，解析端要知道所有目标结构 | 每个 consumer 只管自己的目标 |
| 队列表述 | SQL 文本比 JSON 大 30–80% | 只传结构化数据 |
| 批量 | 无法感知下游能否合并 | 1000 条合成 1 条 `INSERT ... VALUES (...),(...)` |
| 列裁剪 | 需要同时考虑所有下游的列需求 | 消费端按需取 |
| 安全 | SQL 里拼值风险高 | 参数化绑定，天然防注入 |
| 变更成本 | 改 SQL 要重启解析端 | 只重启对应 consumer |

**例外**：DDL（`QueryDTO` 里的 `ALTER TABLE` / `CREATE TABLE`）属于元数据，
如果要自动同步目标表结构，应该在消费端执行（或单独的 schema-sync 任务），
因为只有消费端知道目标库的连接和命名规则。

### 21.2 主键处理：三级回退

`EventBuilder` 按优先级确定幂等键，实测结果：

| 表结构 | `table_key` | `pk` 示例 |
|---|---|---|
| 单主键 | `['id']` | `demo.t_single:1` |
| 联合主键 `(a,b)` | `['a','b']` | `demo.t_multi:1,2` |
| 仅唯一键 `UNIQUE(code)` | `['code']` | `demo.t_uniq:C1` |
| 无主键无唯一键 | `[]` | `demo.t_nopk:#5d8e6433a0ac4d8f` |

实现要点：

- **主键优先于唯一键**：两者都存在时用主键。唯一键可能有多个，取 `information_schema`
  里第一个（`ORDINAL_POSITION` 最小）；需要精确控制时请显式建主键。
- **联合主键顺序稳定**：按 `information_schema.COLUMNS` 的列顺序拼接，
  多次解析结果一致，可安全用作幂等键。
- **无键表退化**：用整行内容的 `sha1` 前 16 位作为键，保证不同行不撞键。
  `delete` 事件用 `before` 行计算，与对应的 `insert` 结果一致。
- **只告警一次**：遇到无键表时打一条 WARNING，后续同类表不重复刷日志。

### 21.3 无键表的注意事项

无键表并不是"不能用"，但有这些限制：

| 能力 | 有键表 | 无键表 |
|---|---|---|
| Redis Stream 去重 | ✅ | ⚠️ 内容相同的两行会撞键 |
| MySQL Sink 删除 | 按主键 `IN (...)` 批量删 | **按整行 `WHERE ... LIMIT 1` 逐条删** |
| ES `_id` | 主键值 | 行哈希（可能撞 ID） |
| CK ReplacingMergeTree | 主键去重 | 无稳定排序键，重复数据可能残留 |

`MysqlSink` 对无键表的删除实现：

```sql
DELETE FROM mirror_logs WHERE x <=> ? AND y <=> ? LIMIT 1
```

- 用 `<=>` 做 NULL 安全比较
- `LIMIT 1` 避免误删内容完全相同的多行
- 逐条执行，批次内仍在同一事务里

### 21.4 建议

1. **业务表必须建主键**。没有主键的表在 binlog 场景下无法可靠幂等，
   这是数据库设计问题，不该用消费端兜底。
2. **临时表/日志表如果确实没主键**，在配置里排除掉，避免无谓开销：
   ```php
   'filter' => ['ignore_tables' => ['tmp_import', 'log_debug']],
   ```
3. **只有唯一键的表**可以接受，但要确认唯一键在业务上真的唯一
   （例如软删除表用 `(code, deleted_at)` 这类复合唯一键可能需要显式建主键）。
4. **联合主键**完全支持，但下游 ES/CH 的 ID 会变成 `a:b` 形式，
   需要确认目标字段长度够用。

---

## 22. 恢复策略：为什么默认用 file+pos 而不是 GTID

### 22.1 一个真实故障

```
[INFO] 从已保存点位恢复：mysql-bin.000028:2639136
错误：The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1,
     but the master has purged binary logs containing GTIDs that the slave requires.
     The GTID set sent by the slave is 'c8fbcbb8-...:2901',
     and the missing transactions are 'c8fbcbb8-...:1-4'.
```

日志明明显示按 `file:pos` 恢复，MySQL 却报 GTID 错误。原因是点位里
`binlogFile` 和 `gtid` **同时有值**，旧逻辑 `isGtidMode()` 只看 gtid 非空，
于是错误地走了 GTID 分支。

更根本的问题：`gtid` 字段存的是 **binlog 事件里的单个 GTID**（`uuid:2901`），
表示「最后处理的那个事务」，而 `COM_BINLOG_DUMP_GTID` 需要的是 **完整执行集合**
（如 `uuid:1-2901`）。把单个事务当执行集合发给主库，必然失败。

### 22.2 修复后的规则

```php
// Checkpoint
public function hasFilePosition(): bool      // binlogFile !== ''
public function shouldResumeByGtid(): bool    // !hasFilePosition() && gtid !== ''
```

| 点位状态 | 恢复方式 |
|---|---|
| 有 `binlogFile` | **file+pos**（默认，精确可靠） |
| 无 `binlogFile`，有 `gtid` | 回退 GTID，并打 WARNING |
| 都没有 | 从当前最新 binlog 末尾开始 |

`gtid` 保留在点位里仅作参考（`relay status` 会显示「仅供参考，不用于续传」）。

### 22.3 为什么 file+pos 更可靠

| | file+pos | GTID |
|---|---|---|
| 精确性 | 字节级精确 | 事务级，可能有间隙 |
| 依赖 | 主库保留对应 binlog 文件 | 需完整累积执行集合 |
| 本项目实现 | 每次事件直接读 `BinLogCurrent` | 只能拿到单事务 GTID |
| 主库清理日志后 | 报错，需重置点位 | 同样报错 |

真正做到 GTID 恢复需要维护**累积集合**（把每个 GTID 合入 `uuid:1-2901` 形式），
成本和出错面都更大。对「单实例、单个 binlog 文件内的续传」场景，file+pos
已经完全够用，且更容易排查。

### 22.4 遇到 "master has purged binary logs" 怎么处理

说明点位指向的 binlog 文件已被主库清理（`expire_logs_days`）。两个选择：

```bash
# 1. 从当前最新位置重新开始（会跳过期间的数据变化）
php bin/binlog2redis reset-position --force

# 2. 用全量同步 + 增量：先做一次数据快照，再从快照时刻的 binlog 位置启动
```

预防措施：`expire_logs_days` 要大于「服务可能停机的最长时间」：

```ini
[mysqld]
expire_logs_days = 7          # 至少能覆盖一次周末
max_binlog_size  = 512M
```

---

## 23. 自定义处理器与脚本

两个位置都可以挂载业务逻辑：

```
relay（读 binlog）                     consumer（读队列）
   └── handlers: [...]                    └── sink.driver = custom
       ├── 内置（Stdout/MultiStream）          └── class: 你的类
       ├── ScriptHandler（外部脚本）
       └── 你的类（HandlerInterface）
```

### 23.1 方式一：自定义 Handler 类（打进二进制）

```php
final class OrderSyncHandler implements HandlerInterface
{
    public function handle(array $changes): void   // Change[]
    {
        foreach ($changes as $c) {
            // $c->op / database / table / row / before / tableKey
        }
    }
}
```

```php
// config/relay.php
'handlers' => [
    BinlogRelay\Handler\StdoutHandler::class,
    App\OrderSyncHandler::class,          // 类名即可，框架自动 new
    // new App\OrderSyncHandler('自定义参数'),   // 需要构造参数时给实例
],
```

### 23.2 方式二：外部脚本（ScriptHandler）

脚本放磁盘、不进二进制，改完重启即可生效：

```php
// config/relay.php
'handlers' => [
    new BinlogRelay\Handler\ScriptHandler(
        getenv('RELAY_SCRIPT') ?: '/opt/binlog-relay/scripts/order-sync.php'
    ),
],
```

**协议**（脚本必须遵守）：

```
stdin   每行一个 JSON 数组（一批变更）
stdout  每行必须是 {"ok":true} 或 {"ok":false,"error":"..."}
stderr  原样转发到框架日志
```

⚠️ **stdout 是协议通道**，业务输出（日志、调试信息）必须写 stderr，否则会破坏通信。

脚本是**常驻进程**（读一行 → 处理 → 回一行 → 循环），所以数据库连接可以建在循环外。

完整示例见 `examples/scripts/`：

| 脚本 | 用途 |
|---|---|
| `01-stdout-echo.php` | 最小示例 |
| `02-mysql-upsert.php` | 批量 upsert 到 MySQL |
| `03-redis-cache-invalidate.php` | 缓存失效 |
| `04-webhook-push.php` | HTTP 推送（带 HMAC 签名） |
| `05-filter-and-forward.php` | 审计日志 |
| `06-batch-aggregate.php` | 脚本内二次攒批（高吞吐） |

### 23.3 打包成二进制后如何引用外部脚本

二进制里代码在 `phar://` 内，**`__DIR__` 指向 phar 内部**，所以脚本路径必须用绝对路径：

```php
// ❌ 打包后失效
new ScriptHandler(__DIR__ . '/../scripts/x.php')

// ✅ 用环境变量或绝对路径
new ScriptHandler(getenv('RELAY_SCRIPT') ?: '/opt/binlog-relay/scripts/x.php')
```

部署结构：

```
/opt/binlog-relay/
├── relay                  ← 二进制
├── config/relay.php       ← 外部配置
└── scripts/*.php          ← 外部脚本
```

启动：

```bash
cd /opt/binlog-relay
RELAY_CONFIG=./config/relay.php ./relay start
```

**脚本由 `PHP_BINARY` 启动**，打包场景下就是 phpmicro 自身，所以脚本可用的扩展
与二进制编译进去的一致（见第 9.1 节的扩展清单）。

⚠️ **平台限制**：`ScriptHandler` 用 `proc_open` 管道通信。
Windows 的管道不支持非阻塞与超时（`stream_set_blocking` 返回 false，
`stream_set_timeout` 对管道无效），脚本卡死时无法及时超时。
**生产环境请用 Linux。**

### 23.4 方式三：消费端自定义 Sink

消费端也可以挂自己的类，两种接口二选一：

```php
// A. 复用 relay 侧逻辑（收到 Change）
final class OrderMirror implements HandlerInterface
{
    public function handle(array $changes): void { /* Change[] */ }
}

// B. 需要 Redis entry id 时（收到 Message）
final class RawDump implements MessageHandlerInterface
{
    public function handle(array $messages): void { /* Message[] */ }
}
```

```php
// config/consumer.php
'sink' => [
    'driver' => 'custom',
    'custom' => [
        'class' => App\OrderMirror::class,
        'args'  => ['mysql:host=127.0.0.1;dbname=mirror', 'root', 'pass'],
    ],
],
```

完整示例见 `examples/CustomSinkExample.php`。同一个类既能当 relay 的 handler，
也能当 consumer 的 sink。

### 23.5 三种方式怎么选

| | 自定义 Handler 类 | ScriptHandler 脚本 | 自定义 Sink（消费端） |
|---|---|---|---|
| 改完是否要重新打包 | 是 | **否** | 是 |
| 能否用项目类 | ✅ | ❌（必须自包含） | ✅ |
| 类型安全 | `Change` 对象 | 纯数组 | `Change` / `Message` |
| 进程隔离 | 否（同主进程） | ✅ 独立子进程 | 否 |
| 额外开销 | 无 | 进程间通信 | 无 |
| 适用 | 生产主路径 | 频繁改动 / 独立演进 | 消费端定制 |

**推荐**：核心链路用自定义 Handler 类（性能和类型安全最好）；
业务规则频繁变动、且能独立部署的用 ScriptHandler。

---

## 24. 与其他方案的对比

### 24.1 Maxwell（最接近的对手）

| 维度 | 本项目 | Maxwell |
|---|---|---|
| binlog 解析 | ROW 格式，全列类型 | 同 |
| 精确定点续传 | ✅ file+pos | ✅ file+pos + GTID 集合 |
| **完整 GTID 支持** | ⚠️ 只记录单事务 GTID，不用于续传 | ✅ 执行集合 |
| **全量初始化** | ❌ 无 | ✅ 支持 |
| **DDL 同步** | ❌ 不订阅 Query 事件 | ✅ 可配置 |
| 输出目标 | Redis Stream、HTTP、TCP、MySQL、CK、ES | Kafka、Kinesis、RabbitMQ、Redis、SQS、PubSub、文件 |
| 按表过滤 | ✅ 含 `only_configured` | ✅ |
| **列裁剪** | ✅ | ❌ |
| **多 topic 扇出** | ✅ 单进程 pipeline | ❌（需 Kafka 后端） |
| **内置消费端** | ✅ 5 种 sink + 自定义 | ❌ |
| HA / 多实例 | ❌ 单实例 + 锁互斥 | ✅ 配合 Kafka 分区 |
| 监控指标 | ❌ 仅 `status` 命令 | ✅ JMX |
| 部署体积 | **19.5 MB 单文件** | JDK + jar，200 MB+ |
| 冷启动 | 毫秒级 | 3–10 秒 |
| 内存 | ~30 MB | 200–500 MB |

**性能**（本项目实测；Maxwell 为社区公开数据，未亲测）：

| | 本项目 | Maxwell |
|---|---|---|
| 吞吐 | 7k–8k events/s | 10k–30k events/s |
| 依赖 | 零（无 composer 包） | Kafka、Guava 等 |

吞吐低于 Maxwell 是事实：PHP 单线程 vs JVM JIT。

### 24.2 Canal（阿里巴巴出品）

Canal 最早是为「MySQL 增量订阅 + 同步到下游」设计的中间件，生态成熟、国内用得最广。

| 维度 | 本项目 | Canal |
|---|---|---|
| 定位 | 轻量中继 + 内置消费端 | CDC 服务端，需搭配客户端 |
| 实现语言 | PHP（纯 PHP 解析 binlog 协议） | Java（Netty + 自定义协议解析） |
| **架构** | 单进程：解析 → 过滤 → 直接写目标 | **Client/Server 分离**：Server 解析 → 自定义协议 → Client 拉取 |
| 协议解析 | 直连 MySQL 复制协议 | 同 |
| 支持的 MySQL | 5.5–8.0、MariaDB | 5.1–8.0、MariaDB、部分分支 |
| 过滤能力 | 库/表/列，`only_configured` | 库/表过滤、正则；**无列裁剪** |
| 点位 | file+pos（+ 单事务 GTID 参考） | file+pos + **完整 GTID 集合** |
| **HA** | ❌ 单实例 + 锁互斥 | ✅ ZooKeeper 选主 |
| **全量初始化** | ❌ | ✅ `canal.adapter` 支持 |
| **DDL 同步** | ❌ | ✅ 可配置过滤/同步 |
| 输出 | 内置 5 种 sink + 自定义 | 自有协议 / Kafka / RocketMQ / RabbitMQ |
| 消费端 | ✅ 内置，一条命令跑起来 | 需另写 client（Java）或接 MQ 再消费 |
| 监控 | `status` 命令 | ✅ Prometheus metrics、admin 控制台 |
| 部署依赖 | 无（单文件） | JDK +（HA 时）ZooKeeper + 配置管理 |
| 部署体积 | 19.5 MB | 100 MB+（含依赖） |
| 运维复杂度 | 低 | 中高 |

**性能对比**

Canal 官方与社区的公开数据是**单实例 10k–30k events/s**（Java + Netty，
事件级流水线），本项目实测 **7k–8k events/s**。

差距主要来自：

| 因素 | 影响 |
|---|---|
| JVM JIT vs PHP 解释执行 | 热路径代码 JIT 后快 2–5 倍 |
| Netty 零拷贝 vs PHP 字符串操作 | 网络层与缓冲区管理 |
| Canal 只解析不落地，客户端拉取 | 本项目解析后还要序列化 + 写 Redis |

**但要注意**：Canal 的 10k–30k 是 **Server 端解析吞吐**，实际端到端还要加上
「Client 拉取 + 业务处理」。而且 Canal 的吞吐优势要在**大批量、窄表**场景才明显；
如果行里有 TEXT/BLOB，瓶颈会转移到网络传输和下游写入，两者差距会缩小。

**什么时候 Canal 更合适**

| 场景 | 原因 |
|---|---|
| 需要 **HA / 高可用** | ZooKeeper 选主，主备切换 |
| 需要**全量初始化** | canal.adapter 内置 |
| 需要**同步 DDL** | 内置支持 |
| 已有 Kafka/RocketMQ 生态 | Canal 原生对接 |
| 吞吐 > 10k events/s | Java 性能优势 |
| 团队是 Java 栈 | 运维与排障一致 |
| 多业务方订阅同一份 binlog | Client/Server 分离更合适 |

**什么时候本项目更合适**

| 场景 | 原因 |
|---|---|
| 单点同步，不需要 HA | 部署就一个文件 |
| 目标就是 Redis/MySQL/HTTP | 内置 sink，不需要再搭 MQ 和 consumer |
| 需要**列裁剪丢大字段** | Canal 只能整行输出 |
| 需要**单进程多 topic 扇出** | Canal 靠 MQ 做分发 |
| 部署环境受限 | 无 JDK |
| PHP 团队 | 零语言切换成本 |

**一个容易被忽略的差异**：Canal 是 **pull 模型**（Server 等 Client 来拉），
本项目是 **push 模型**（解析后直接写目标）。
pull 更适合「多消费者、消费速度不一致」，push 更适合「单目标、链路短」。
如果你的场景是「一份 binlog 要喂给 5 个不同业务」，Canal 的架构更自然；
如果只是「同步到 Redis」，本项目的 push 少了两跳网络。

### 24.3 Debezium

Debezium 是 CDC 平台级方案：全量+增量、schema 演进、CDC 到 Kafka Connect 生态。
本项目与之不在同一量级——Debezium 适合做企业级数据集成，本项目适合单点轻量同步。

**能力对比**（Debezium 明显更强）：

| 维度 | 本项目 | Debezium |
|---|---|---|
| 全量 + 增量 | ❌ 仅增量 | ✅ snapshot + streaming |
| Schema 演进 | ❌ | ✅ 内置 |
| 支持的数据库 | MySQL 系 | MySQL、PG、Oracle、SQL Server、MongoDB… |
| 生态 | 无 | Kafka Connect 插件生态 |
| 部署 | 19.5 MB 单文件 | Kafka + Connect 集群 |

**但 Debezium 的隐性成本**：必须部署 Kafka（或 Kafka Connect 集群），
运维成本是 Canal 的数倍。除非你已经在用 Kafka，否则为一个 MySQL 同步需求
引入整套 Kafka 生态通常不划算。

### 24.4 什么时候该选本项目

| 场景 | 原因 |
|---|---|
| 目标就是 Redis / MySQL / 简单 HTTP | 内置 sink 直接覆盖 |
| 部署环境受限（无 JDK、镜像要小） | 19.5 MB 单文件 |
| 需要列裁剪、丢大字段 | Maxwell/Canal 都不支持 |
| PHP 团队，想用熟悉语言写 sink | 零成本 |
| 吞吐 < 5k events/s | 完全够用 |
| 需要内置消费端 | 其他方案都要自己写 |

### 24.5 什么时候该选 Maxwell

| 场景 | 原因 |
|---|---|
| 需要**全量初始化** | 本项目无此能力 |
| 需要**跨 binlog 文件 / 主从切换续传** | 需完整 GTID 集合 |
| 需要**同步 DDL** | 本项目不订阅 Query 事件 |
| 吞吐 > 10k events/s | PHP 单线程到顶 |
| 需要多实例 HA | 本项目是单点 |
| 团队是 Java 栈 | 运维工具链一致 |

### 24.6 已知待补缺口

若要用于正式生产，建议优先补两项：

1. **全量初始化**：当前只能 mysqldump + 手工对齐位点
2. **GTID 累积集合**：主从切换时会踩坑（`master has purged binary logs` 的根因）

---

## 25. 解析速度还有提升空间吗

### 25.1 已做的优化

| 优化 | 效果 |
|---|---|
| `BinaryDataReader::read()` 去掉每次 `substr` 整个缓冲区（O(n²)→O(n)） | -7.5% |
| 列类型判断 `elseif` 链 → `switch` | 该语句快 ~30% |
| `EventBuilder` 主键列缓存 + 归一化类型分派 | -32%（前次） |
| 列裁剪提前到对象构造前 | -48%（前次） |
| `Filter` 加表级结果缓存 | -28%（前次） |

实测（4000 字节 / 1000 次读取，取最小值）：

```
优化前  308.6 µs/事件
优化后  285.6 µs/事件   (-7.5%)
```

### 25.2 剩余空间评估

| 方向 | 预期收益 | 代价 | 建议 |
|---|---|---|---|
| `getColumnData` 改用预编译闭包表 | ~5% | 可读性下降 | 可选 |
| `bitGet`/`bitCount` 内联 | ~3% | 可读性下降 | 可选 |
| 解析结果直接输出（跳过 Change 对象） | ~15% | **破坏 API 契约** | 不建议 |
| 用 FFI 调 C 实现 | 未知 | 复杂度极高、可移植性差 | 不建议 |
| 多线程解析 | — | binlog 天然串行 | 不可行 |

**结论：解析层已接近 PHP 的合理上限。** 剩余可压榨的空间在 5% 量级，
而网络（86%）和 handler IO 才是真正的大头。

### 25.3 真要提升整体吞吐，优先级应该是

1. **减少数据量**：列裁剪 + `only_configured`（已实现，收益最大）
2. **减少事件数**：MySQL 端 `binlog_row_event_max_size` 调大
3. **handler 批量写**：多值 INSERT 替代逐条（5–10 倍）
4. **多机水平扩展**：按库/表拆分实例
5. 解析层微调（< 5%）

### 25.4 复现基准

```bash
php tests/bench.php          # EventBuilder / 序列化 / 点位
php tests/netbench.php       # 网络 IO（sockets vs stream_socket）
php tests/run-all.php        # 含 BinaryReaderTest / ColumnTypeTest
```

---

## 26. GTID 集合、DDL 同步与全量同步

补齐了此前对比 Maxwell 时列出的三个缺口。

### 26.1 GTID 执行集合

**问题**：旧实现只记录「最后处理的那个事务」`uuid:2901`，
而 `COM_BINLOG_DUMP_GTID` 需要**完整执行集合** `uuid:1-2901`。
拿单事务当集合发给主库，会报 `master has purged binary logs`。

**实现**：`src/Checkpoint/GtidSet.php`，按 SID 分组合并闭区间。

```php
$set = new GtidSet();
$set->addGtid('uuid:1');
$set->addGtid('uuid:2');
$set->addGtid('uuid:5');
echo $set;   // uuid:1-2:5   ← 自动合并
```

启用：

```bash
RELAY_GTID_SET=1 php bin/binlog2redis start
```

开启后点位里存的是完整集合（如 `uuid:1-1000:1002`），
可跨 binlog 文件续传；关闭时仍存单事务 GTID（仅作参考）。

> 注意：仍以 **file+pos 优先**恢复。GTID 集合只在没有 binlog 文件名时兜底，
> 因为 file+pos 更精确，且不依赖主库是否保留了对应对应日志。

### 26.2 DDL 同步

**问题**：只订阅行事件时，下游不知道源库改了表结构。

**实现**：
- `src/Event/DdlChange.php` — DDL 事件对象 + SQL 类型识别 + 表名提取
- `src/Handler/DdlHandlerInterface.php` — 可选接口，实现它就能收到 DDL
- `RowSubscriber::onQuery()` — 从 QUERY 事件里挑出 DDL

识别范围：`CREATE/ALTER/DROP/RENAME/TRUNCATE TABLE`、索引、库、
以及视图/存储过程/触发器（归入 `other`）。`BEGIN`/`COMMIT`/DML 会被忽略。

启用：

```php
// config/relay.php
'filter' => ['emit_ddl' => true],
```

```php
final class MySchemaSync implements HandlerInterface, DdlHandlerInterface
{
    public function handle(array $changes): void { /* 行变更 */ }

    public function handleDdl(DdlChange $ddl): void
    {
        // $ddl->op       create_table / alter_table / ...
        // $ddl->database 库名
        // $ddl->tables   涉及的表
        // $ddl->query    完整 SQL
        echo "{$ddl->op} on {$ddl->database}.{$ddl->tables[0]}\n";
    }
}
```

或环境变量 `RELAY_EMIT_DDL=1`。

**为什么不放进 HandlerInterface**：DDL 是低频事件，加到主接口会强迫
所有处理器写一个空方法。用独立接口可选择性实现，且不破坏已有代码。

**性能**：开启后额外订阅 `QUERY_EVENT`。业务库的 QUERY 事件大多是
`BEGIN`/`COMMIT`，会被快速过滤，实测额外开销可忽略。

### 26.3 全量同步

**问题**：增量只能捕获「订阅之后」的变更，历史数据补不齐。

**实现**：`binlog2redis fullsync`

```bash
# 先看会同步哪些表
php bin/binlog2redis fullsync --dry-run

# 执行（建议先停掉 relay）
php bin/binlog2redis fullsync

# 只同步指定表
php bin/binlog2redis fullsync --tables=orders,order_items
```

**关键设计：先记位点，后快照**

```
1. 读取当前 binlog 位点 P，立刻持久化
2. 从 P 开始逐表分批 SELECT（游标分页，不是 OFFSET）
3. 完成后启动 relay，从 P 开始回放
```

这样：
- **不丢**：P 之后的所有变更都会被 relay 回放
- **可能重复**：快照期间变更过的行会被写两次

重复由下游幂等吸收（`ON DUPLICATE KEY UPDATE` / ES `_id` / CK 主键去重）。
这是全量+增量的标准做法，Maxwell / Canal 也是同样思路。

**不要反过来做**（先快照后记位点）——那会丢掉快照期间发生的变更。

**分页实现**：

| 表类型 | 方式 |
|---|---|
| 单主键 | `WHERE id > ? ORDER BY id LIMIT N` |
| 复合主键 | `WHERE (a>?) OR (a=? AND b>?) ORDER BY a,b LIMIT N` |
| 无主键 | 单次全读 + 告警（无法保证快照一致性） |

用游标而非 `OFFSET`：大表上 `OFFSET 1000000` 会全表扫描，游标分页是 O(1) 定位。

**完整流程**：

```bash
# 1. 全量（会记录位点）
php bin/binlog2redis fullsync

# 2. 启动增量（自动从记录的位点接续）
php bin/binlog2redis start
```

### 26.4 与 Maxwell 的能力对比（更新后）

| 能力 | 更新前 | 现在 |
|---|---|---|
| file+pos 续传 | ✅ | ✅ |
| GTID 执行集合 | ❌ | ✅ `RELAY_GTID_SET=1` |
| DDL 同步 | ❌ | ✅ `RELAY_EMIT_DDL=1` |
| 全量初始化 | ❌ | ✅ `fullsync` |
| 列裁剪 | ✅ | ✅ |
| 多 topic 扇出 | ✅ | ✅ |
| 内置消费端 | ✅ | ✅ |
| HA / 多实例 | ❌ | ❌ 仍是单点 |
| 监控指标 | ❌ | ❌ 仍只有 `status` |

剩下两个缺口（HA、监控）属于运维层面，需要 ZooKeeper/etcd 选主体系，
不在本次范围内。当前单实例 + Redis 锁互斥已能满足大多数场景。
