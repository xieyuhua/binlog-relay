# ScriptHandler 脚本示例

这些脚本由 `BinlogRelay\Handler\ScriptHandler` 启动为**常驻子进程**，
通过 stdin/stdout 与主进程通信。

## 协议

```
父进程 → 子进程 stdin   每行一个 JSON 数组（一批变更）
子进程 → 父进程 stdout  每行必须是 {"ok":true} 或 {"ok":false,"error":"..."}
子进程 stderr            原样转发到主进程日志
```

脚本是常驻的：读一行 → 处理 → 回一行 → 继续读。循环外可以建数据库连接。

---

## 重要：打包成二进制后怎么用

二进制内嵌的是 **phpmicro 运行时**，代码在 `phar://` 里。这带来两个约束：

| 约束 | 说明 |
|---|---|
| **脚本路径必须用绝对路径** | 二进制里的 `__DIR__` 指向 phar 内部，`__DIR__ . '/scripts/x.php'` 会失效 |
| **脚本内不要 `require` 项目类** | `vendor/autoload.php` 和 `src/` 都在 phar 内，外部脚本无法访问 |

所以配置要这么写：

```php
// config/relay.php —— 打包后
'handlers' => [
    new BinlogRelay\Handler\ScriptHandler(
        getenv('RELAY_SCRIPT') ?: '/opt/binlog-relay/scripts/order-sync.php',
        // 脚本参数、环境变量都可传
        args: [],
        env: ['TARGET_DSN' => getenv('TARGET_DSN') ?: ''],
    ),
],
```

部署目录结构：

```
/opt/binlog-relay/
├── relay                  ← 二进制
├── config/
│   └── relay.php          ← 配置文件（外部，不进二进制）
└── scripts/               ← 外部脚本（外部，不进二进制）
    ├── order-sync.php
    ├── webhook-push.php
    └── ...
```

启动：

```bash
cd /opt/binlog-relay
RELAY_CONFIG=./config/relay.php ./relay start
```

> **脚本运行在独立的 PHP 进程里**（由 `PHP_BINARY` 启动）。
> 打包场景下 `PHP_BINARY` 是 phpmicro 自身，所以脚本会用同一个静态运行时执行——
> **脚本里能用的扩展就是二进制编译进去的那些**（见 README 第 9.1 节）。

### 如果脚本需要项目类

三个选择：

1. **脚本自包含**（推荐）：不依赖项目类，自己建 PDO/Redis 连接
2. **改为继承 HandlerInterface 写类**，打进二进制（见下文"自定义 handler"）
3. 部署时把 `src/` 一起拷到服务器，脚本里手动 `require`

---

## 示例清单

| 脚本 | 用途 | 依赖扩展 |
|---|---|---|
| `01-stdout-echo.php` | 最小示例：把变更打到标准输出 | 无 |
| `02-mysql-upsert.php` | 批量 upsert 到 MySQL 镜像表 | pdo_mysql |
| `03-redis-cache-invalidate.php` | 删除相关缓存 key | redis |
| `04-webhook-push.php` | HTTP 推送到业务接口（带签名） | curl |
| `05-filter-and-forward.php` | 按表/操作过滤后写文件（审计日志） | 无 |
| `06-batch-aggregate.php` | 攒批聚合后一次性写库（高吞吐） | pdo_mysql |

---

## 自定义 Handler（比脚本更强）

脚本适合"逻辑独立、不依赖项目类"的场景。如果要用项目类、或需要类型安全，
直接写一个 handler 类放进 `src/Handler/`，随二进制一起打包：

```php
<?php
namespace BinlogRelay\Handler;

use BinlogRelay\Event\Change;

final class OrderSyncHandler implements HandlerInterface
{
    private \PDO $pdo;

    public function __construct()
    {
        // 用 env() 读取配置，打包后依然有效
        $this->pdo = new \PDO(
            (string)env('TARGET_DSN', 'mysql:host=127.0.0.1;dbname=mirror'),
            (string)env('TARGET_USER', 'root'),
            (string)env('TARGET_PASS', '')
        );
    }

    public function handle(array $changes): void
    {
        // ...
    }
}
```

```php
// config/relay.php
'handlers' => [
    BinlogRelay\Handler\OrderSyncHandler::class,   // 类名即可，框架自动 new
],
```

**脚本 vs 自定义类**：

| | ScriptHandler | 自定义 Handler 类 |
|---|---|---|
| 部署 | 脚本放磁盘，改完重启即可 | 要重新打包二进制 |
| 隔离 | 独立进程，崩溃不影响主进程 | 与主进程同生共死 |
| 性能 | 有进程间通信开销 | 无额外开销 |
| 可用的类 | 不能用项目类 | 全部可用 |
| 类型安全 | 纯数组 | `Change` 对象 |
