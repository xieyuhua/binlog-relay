# binlog2redis (Go)

用 Go 重写的 MySQL binlog 中继。零外部依赖（除驱动库），单二进制部署，
输出支持 **Redis Stream / Kafka / RabbitMQ / HTTP / MySQL / stdout**。

功能与 PHP 版 `binlog-relay` 对齐，并补齐了彼时缺失的 GTID 集合、
DDL 同步与全量同步。

---

## 1. 特性

| 能力 | 说明 |
|---|---|
| binlog 解析 | 基于 [go-mysql](https://github.com/go-mysql-org/go-mysql)，ROW 格式全列类型 |
| 位点续传 | file+pos（默认，精确）；可选 GTID 执行集合 |
| **GTID 集合** | 区间自动合并，跨 binlog 文件续传 |
| **DDL 同步** | 识别 9 类 DDL，作为独立事件输出 |
| **全量同步** | 游标分页快照，先记位点保证不丢 |
| 行过滤 | 库/表/`only_configured` |
| **列裁剪** | 按表保留列，主键自动保留 |
| **多主题扇出** | 一条变更按规则分发到多个 stream/topic/queue |
| 输出后端 | Redis / Kafka / RabbitMQ / HTTP / MySQL / stdout |
| 优雅退出 | SIGTERM/SIGINT 处理完当前批次再退出 |

---

## 2. 快速开始

### 2.1 MySQL 准备

```ini
[mysqld]
server-id             = 1
log_bin               = /var/log/mysql/mysql-bin
binlog_format         = ROW
binlog_row_image      = FULL
binlog_row_metadata   = FULL   # 8.0+，否则拿不到列名
expire_logs_days      = 7
```

```sql
CREATE USER 'repl'@'%' IDENTIFIED BY '***';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
GRANT SELECT ON 你的库.* TO 'repl'@'%';   -- 读 information_schema 主键
FLUSH PRIVILEGES;
```

> `binlog_row_metadata=FULL` 很关键：go-mysql 从 TABLE_MAP 事件里读列名，
> 该参数为 MINIMAL 时列名拿不到，事件会是空对象。

### 2.2 构建

```bash
go build -o bin/binlog2redis ./cmd

# 交叉编译到 Linux
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/binlog2redis-linux ./cmd
```

### 2.3 配置

```bash
cp config.yaml.example config.yaml   # 或直接用仓库里的 config.yaml
```

最小配置：

```yaml
mysql:
  host: 127.0.0.1
  user: repl
  password: "***"
  server_id: 8811        # 集群内必须唯一

filter:
  databases: [shop]

producer:
  type: redis
  redis:
    addr: 127.0.0.1:6379
    stream: binlog:events

checkpoint:
  driver: file
  path: var/checkpoint.json
```

### 2.4 运行

```bash
./bin/binlog2redis check      # 自检
./bin/binlog2redis start      # 启动
```

---

## 3. 输出后端

### 3.1 Redis Stream

```yaml
producer:
  type: redis
  redis:
    addr: 127.0.0.1:6379
    stream: binlog:events
    max_len: 200000        # 近似裁剪，防止消费端宕机时无限增长
```

消费端用 consumer group 读取，消息字段：

| 字段 | 含义 |
|---|---|
| `id` | 幂等键，如 `shop.orders:7` |
| `kind` | `row` / `ddl` |
| `op` | insert / update / delete / create_table ... |
| `db` / `tbl` | 库表 |
| `pos` | `mysql-bin.000012:4321` |
| `ts` | 事件在源库的时间戳 |
| `data` | 完整事件 JSON |

```bash
XGROUP CREATE binlog:events cg:worker 0 MKSTREAM
XREADGROUP GROUP cg:worker w1 COUNT 100 BLOCK 5000 STREAMS binlog:events >
# 处理成功后 XACK binlog:events cg:worker <id>
```

### 3.2 Kafka

```yaml
producer:
  type: kafka
  kafka:
    brokers: [127.0.0.1:9092]
    topic: binlog-events
    required_acks: -1      # 全副本确认，不丢
    compression: snappy
    batch_bytes: 1048576
    batch_timeout: 500ms
```

要点：
- **消息 key 用幂等键**，同一行进入同一分区 → 保序
- `required_acks: -1` 保证不丢，代价是每批一次往返
- 关闭了 `AllowAutoTopicCreation`，主题需提前创建

### 3.3 RabbitMQ

```yaml
producer:
  type: rabbitmq
  rabbitmq:
    url: amqp://guest:guest@127.0.0.1:5672/
    exchange: binlog       # topic 类型交换机
    queue: binlog-events
    routing_key: binlog.events
    durable: true
    confirm: true          # publisher confirm，broker 已接收才返回
```

要点：
- 消息 `DeliveryMode=Persistent`，配合 durable 队列可抗 broker 重启
- `MessageId` 设为幂等键，消费端可据此去重
- Header 带 `db`/`table`/`op`/`pos`，便于按 header 路由

### 3.4 多主题扇出

一条变更可以同时进入多个目标：

```yaml
producer:
  type: redis          # 或 kafka / rabbitmq
  topics:
    - target: topic:order
      tables: [orders, order_items]
    - target: topic:audit
      tables: [orders]
      ops: [delete]
    - target: topic:search
      tables: ["*"]     # 全部表
```

- `target` 在 Redis 下是 stream 名，Kafka 下是 topic 名，RabbitMQ 下是 routing key
- 规则按声明顺序匹配，一条记录可命中多条
- 同一目标内的记录按批投递，保证顺序

### 3.5 其他

```yaml
# HTTP 推送（整批 POST）
producer:
  type: http
  http:
    url: http://127.0.0.1:8080/binlog
    timeout: 5s

# MySQL 镜像表（ON DUPLICATE KEY UPDATE）
producer:
  type: mysql
  mysqlsink:
    dsn: "root:@tcp(127.0.0.1:3306)/mirror?charset=utf8mb4&parseTime=true"
    table_prefix: mirror_

# 标准输出（JSON Lines，可管道给 jq）
producer:
  type: stdout
```

---

## 4. 过滤与列裁剪

```yaml
filter:
  databases: [shop]              # 只订阅这些库
  tables: [orders, order_items]  # 只订阅这些表
  ignore_tables: [tmp_import]
  only_configured: true          # 只处理 tables ∪ columns 里的表

  columns:
    orders: [id, status, amount] # 只保留这几列
    "*": [id]                    # 其余表只要主键
```

- 主键列**总会保留**，即使没写进 `columns`
- 未配置的表原样保留；配置了 `"*"` 则作为兜底
- 裁剪在构造事件之前完成，省内存也省队列体积
- 含大字段（TEXT/BLOB）的表收益最大：实测 20 KB → 60 B

---

## 5. GTID 集合

默认用 **file+pos** 续传（精确、不依赖主键是否清理日志）。
需要跨 binlog 文件续传时开启：

```yaml
checkpoint:
  gtid_set: true
```

开启后位点里存的是完整执行集合：

```
uuid:1-1000:1002,uuid2:1-50
```

区间会自动合并（处理 1,2,3,5 后得到 `1-3:5`），
可直接用于 `COM_BINLOG_DUMP_GTID`。

> 为什么不默认开：file+pos 更精确，且不依赖主库保留对应 binlog 文件。
> GTID 集合主要用于「文件已被清理，但主库还有 GTID」等场景。

---

## 6. DDL 同步

```yaml
filter:
  emit_ddl: true
```

识别类型：`create_table` / `alter_table` / `drop_table` / `rename_table` /
`truncate_table` / `create_index` / `drop_index` / `create_database` /
`drop_database` / `other`（视图、存储过程、触发器）。

输出示例：

```json
{
  "kind": "ddl",
  "op": "alter_table",
  "db": "shop",
  "table": "orders",
  "pos": "mysql-bin.000012:4321",
  "payload": {
    "op": "alter_table",
    "database": "shop",
    "tables": ["orders"],
    "query": "ALTER TABLE orders ADD COLUMN x INT"
  }
}
```

- 非 DDL 的 QUERY 事件（BEGIN/COMMIT/DML）会被快速过滤
- 收到 DDL 后会自动清空主键与列名缓存

---

## 7. 全量同步

增量只能捕获「订阅之后」的变化。下游为空或与源库有偏差时，必须先全量。

```bash
./bin/binlog2redis fullsync --dry-run     # 先看会同步哪些表
./bin/binlog2redis fullsync               # 执行
./bin/binlog2redis start                  # 接续增量
```

### 7.1 关键：先记位点，后快照

```
1. 读取当前 binlog 位点 P，立刻持久化
2. 从 P 开始逐表分批 SELECT
3. 完成后启动 relay，从 P 回放
```

- **不丢**：P 之后的所有变更都会被回放
- **可能重复**：快照期间改过的行会写两次

重复由下游幂等吸收（`ON DUPLICATE KEY UPDATE` / ES `_id` / CK 主键去重）。
**反过来做（先快照后记位点）会丢掉快照期间的变更。**

### 7.2 分页实现

| 表类型 | SQL |
|---|---|
| 单主键 | `WHERE id > ? ORDER BY id LIMIT N` |
| 复合主键 | `WHERE (a>?) OR (a=? AND b>?) ORDER BY a,b LIMIT N` |
| 无主键 | 单次全读 + 告警（无法保证一致性） |

用游标而非 `OFFSET`：大表上 `OFFSET 1000000` 会全表扫描。

---

## 8. 与 PHP 版对比

| | PHP 版 `binlog-relay` | Go 版 `binlog2redis` |
|---|---|---|
| 吞吐 | 7–8k events/s | **更高**（Go 编译执行 + 更少的分配） |
| 内存 | ~30 MB | ~20 MB |
| 二进制 | 19.5 MB | 18 MB |
| 冷启动 | 毫秒 | 毫秒 |
| 输出后端 | Redis/HTTP/TCP/MySQL/CK/ES | Redis/**Kafka**/**RabbitMQ**/HTTP/MySQL/stdout |
| 消费端 | 内置（5 种 sink） | 无（依赖 MQ 生态） |
| 部署 | 单文件 | 单文件 |

Go 版更适合「大量小事件、需要接入 MQ」的场景；
PHP 版更适合「已有 PHP 业务代码，想直接复用」的场景。

---

## 9. 运维

### 9.1 健康检查

```bash
./bin/binlog2redis check      # MySQL / 位点存储 / 输出端
./bin/binlog2redis status     # 当前位点
```

### 9.2 systemd

```ini
[Unit]
Description=binlog2redis
After=network.target mysql.service

[Service]
Type=simple
WorkingDirectory=/opt/binlog2redis
EnvironmentFile=/opt/binlog2redis/.env
ExecStart=/opt/binlog2redis/binlog2redis start
Restart=always
RestartSec=3
TimeoutStopSec=30
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

### 9.3 排障

| 现象 | 处理 |
|---|---|
| 事件里列名是空的 | MySQL 需 `binlog_row_metadata=FULL` |
| `master has purged binary logs` | 位点太旧，`reset-position --force` 后重新开始 |
| 位点被占用 | 有实例在跑；确认后 `reset-position --force` |
| Kafka 主题不存在 | 提前创建（已关闭自动创建） |
| 重复消费 | 正常（至少一次）；下游按 `id` 幂等 |

---

## 10. 测试

```bash
go test ./...              # 全部单元测试
go vet ./...               # 静态检查
```

覆盖：
- `checkpoint`：GTID 区间合并、跨段合并、多 SID、round-trip、非法输入
- `event`：主键/联合主键/无键表的幂等键、Envelope 结构、DDL 键稳定性
- `filter`：库表过滤、忽略优先级、only_configured 并集、列裁剪不污染共享配置
- `producer`：路由匹配、多主题扇出、空批次与未命中

---

## 11. 性能优化记录

### 11.1 消息格式扁平化（-41% 序列化耗时，-30% 体积）

**问题**：初版采用 `{meta..., payload:{...}}` 嵌套格式，导致：

1. `payload` 先 `json.Marshal` 一次填进 `RawMessage`，producer 再整体 `Marshal` 一次
   → **序列化两遍**
2. `payload` 里的引号被转义，体积膨胀
3. `op`/`db`/`table` 在信封和 payload 里各存一份

**实测**：

| 指标 | 嵌套 | 扁平 | 变化 |
|---|---|---|---|
| 单条投递耗时 | 5060 ns | **2980 ns** | **-41%** |
| 内存分配 | 1651 B / 27 次 | **1298 B / 26 次** | -21% |
| 消息体积 | 486 B | **341 B** | **-30%** |
| `NewEnvelope` 单独 | 2785 ns | **336 ns** | **-88%** |

扁平后的格式：

```json
{
  "id": "shop.orders:7",
  "kind": "row",
  "op": "update",
  "db": "shop", "table": "orders",
  "pos": "mysql-bin.000012:4321",
  "ts": 1795013415, "emitted_at": 1795013500,
  "row": {...}, "before": {...}, "table_key": ["id"],
  "server_id": 1
}
```

DDL 事件用 `ddl_tables` / `ddl_query` 字段，`kind` 为 `ddl`。

**消费端影响**：不再有 `payload` 层级，其余字段名不变。
MySQL sink 因此也省掉了一次反序列化。

### 11.2 修复：主键从未被注入（幂等键退化）

`Parser.SetTableKeys` 定义后**从未被调用**，导致所有表的 `table_key`
为空，幂等键退化成行内容哈希：

- 内容相同的多行会撞同一个键
- ES/ClickHouse 的去重失效
- MySQL sink 的删除会走「整行匹配」而非主键 `IN`

修复：`Syncer` 在处理 `RowsEvent` 前调用 `ensureTableKeys`，
首次遇到某表时查一次 `information_schema` 并注入，之后走缓存。
查询失败只告警不中断（退化为行哈希，仍可运行）。

### 11.3 全量同步投递批次可配置

原来硬编码「每 500 条投递一次」，大表同步时产生大量小批次。
现在按 `runtime.fullsync_batch / 2`（最小 100）计算，
读取批次与投递批次解耦。

### 11.4 复现基准

```bash
go test -bench=. -benchmem ./internal/event/
```

基准覆盖：
- `ChangeKey`：有主键 103 ns（1 次分配）
- `ChangeKeyNoKey`：无主键走 SHA1，2194 ns
- `PathOld` vs `PathNew`：嵌套 vs 扁平的完整投递路径对比
- `Batch200`：200 条的批量路径

### 11.5 还能优化什么

| 方向 | 预期收益 | 代价 |
|---|---|---|
| `json.Marshal` 换 `easyjson`/`sonic` | 序列化快 2–3 倍 | 引入代码生成，维护成本 |
| `map[string]any` 换预定义 struct | 减少反射开销 | 需按表生成类型，不现实 |
| `sync.Pool` 复用 Envelope | 减少 GC 压力 | 生命周期管理复杂，收益约 5–10% |
| 无主键表的 SHA1 换 xxhash | 快约 3 倍 | 需要额外依赖 |

当前吞吐瓶颈已不在序列化，而在**下游网络往返**（Redis/Kafka 的 RTT）。
继续优化序列化的边际收益很低。
