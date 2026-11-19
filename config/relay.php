<?php
/**
 * binlog-relay 配置
 *
 * 所有值都可以用环境变量覆盖，便于容器化与打包后部署。
 * 生产环境建议把密码放在环境变量里，不要提交到仓库。
 */

return [
    // ---------- MySQL 主库（读取 binlog 的源） ----------
    'mysql' => [
        'host'     => env('RELAY_MYSQL_HOST', '127.0.0.1'),
        'port'     => (int) env('RELAY_MYSQL_PORT', 3306),
        'user'     => env('RELAY_MYSQL_USER', 'repl'),
        'password' => env('RELAY_MYSQL_PASSWORD', ''),
        'charset'  => env('RELAY_MYSQL_CHARSET', 'utf8mb4'),

        // 从库身份，必须全局唯一（0~2^32-1）
        'slave_id' => (int) env('RELAY_SLAVE_ID', 8811),

        // 心跳：秒，0 表示关闭。建议 30，空闲时保活
        'heartbeat' => (float) env('RELAY_HEARTBEAT', 30),

        // 表结构缓存条数
        'table_cache_size' => (int) env('RELAY_TABLE_CACHE', 256),
    ],

    // ---------- 订阅范围 ----------
    'filter' => [
        // 为空表示不过滤
        'databases' => csv(env('RELAY_DATABASES', '')),
        'tables'    => csv(env('RELAY_TABLES', '')),
        'ignore_databases' => csv(env('RELAY_IGNORE_DATABASES', 'mysql,sys,information_schema,performance_schema')),
        'ignore_tables'    => csv(env('RELAY_IGNORE_TABLES', '')),

        // 列裁剪：只保留业务需要的列，其余在入队前丢弃
        // 收益：队列体积、网络传输、消费端解析、下游写入全面下降
        // 注意：主键列永远保留；before 行同样会被裁剪
        'columns' => [
            // 'orders' => ['id', 'status', 'amount', 'updated_at'],   // 只保留这几列
            // 'cms_article' => ['id', 'title', 'status'],             // 丢掉 content 大字段
            // '*' => ['id'],                                          // 其余表只要主键
        ],
    ],

    // ---------- 点位存储 ----------
    // driver: redis | file
    'checkpoint' => [
        'driver' => env('RELAY_CHECKPOINT', 'file'),

        // 同一个 key 上一个进程独占，避免两个实例同时消费产生重复
        'key' => env('RELAY_CHECKPOINT_KEY', 'binlog_relay:checkpoint:default'),

        'redis' => [
            'host'     => env('RELAY_REDIS_HOST', '127.0.0.1'),
            'port'     => (int) env('RELAY_REDIS_PORT', 6379),
            'password' => env('RELAY_REDIS_PASSWORD', ''),
            'database' => (int) env('RELAY_REDIS_DB', 0),
            'timeout'  => 2.0,
        ],

        'file' => [
            'path' => env('RELAY_FILE_PATH', __DIR__ . '/../var/checkpoint.json'),
            // 是否对点位文件做 fsync。关闭（默认）时崩溃最多丢掉最后一批点位，
            // handler 幂等即可；云盘上单次 fsync 约 17ms，逐事件落盘会被它拖死。
            'fsync' => (bool)env('RELAY_FILE_FSYNC', false),
        ],

        // 每处理 N 个事件落一次点位（批量提交，显著减少 IO）
        'every_events' => (int) env('RELAY_CHECKPOINT_EVERY', 100),
        // 或者每隔 N 秒落一次（两者先到先触发）
        'every_seconds' => (float) env('RELAY_CHECKPOINT_SECONDS', 1.0),
    ],

    // ---------- 输出 / 处理器 ----------
    'handlers' => [
        // 顺序执行。内置：stdout（JSON Lines 打到标准输出）
        // 自定义处理器写全类名，需实现 BinlogRelay\Handler\HandlerInterface
        BinlogRelay\Handler\StdoutHandler::class,

        // 同一个事件分发到多个业务 topic（多队列）：
        // 一条变更可命中多条规则，写入多条 stream，各自独立消费
        // new BinlogRelay\Handler\MultiStreamHandler([
        //     ['stream' => 'topic:order',  'tables' => ['orders', 'order_items']],
        //     ['stream' => 'topic:stock',  'tables' => ['stock', 'stock_log']],
        //     ['stream' => 'topic:search', 'tables' => ['*']],                        // 全表
        //     ['stream' => 'topic:audit',  'tables' => ['orders'], 'ops' => ['delete']],
        // ], null, [
        //     'host'     => env('RELAY_REDIS_HOST', '127.0.0.1'),
        //     'port'     => (int) env('RELAY_REDIS_PORT', 6379),
        //     'password' => env('RELAY_REDIS_PASSWORD', ''),
        //     'database' => (int) env('RELAY_REDIS_DB', 0),
        // ]),
    ],

    // 每批多少条事件交给 handler（单条事件可能含多行变更）
    'batch_size' => (int) env('RELAY_BATCH_SIZE', 200),

    // ---------- 日志 ----------
    'log' => [
        'level' => env('RELAY_LOG_LEVEL', 'info'),   // debug|info|warn|error
        'file'  => env('RELAY_LOG_FILE', ''),        // 为空则打到 stderr
    ],

    'timezone' => 'Asia/Shanghai',
];
