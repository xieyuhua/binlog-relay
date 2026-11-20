<?php
/**
 * binlog-relay 消费端配置
 *
 * 生产端（relay）把事件写进 Redis Stream，本配置决定消费端怎么读、写到哪。
 */

return [
    'redis' => [
        'host'     => env('RELAY_REDIS_HOST', '127.0.0.1'),
        'port'     => (int) env('RELAY_REDIS_PORT', 6379),
        'password' => env('RELAY_REDIS_PASSWORD', ''),
        'database' => (int) env('RELAY_REDIS_DB', 0),
        'timeout'  => (float) env('RELAY_REDIS_TIMEOUT', 2.0),
    ],

    'stream' => env('CONSUMER_STREAM', 'binlog:events'),

    'group' => env('CONSUMER_GROUP', 'cg:default'),

    // 默认 consumer 名，容器里建议用 POD_NAME / HOSTNAME
    'consumer' => env('CONSUMER_NAME', (gethostname() ?: 'php') . '-' . getmypid()),

    // 每次拉取条数、阻塞毫秒
    'batch_size' => (int) env('CONSUMER_BATCH', 500),
    'block_ms'   => (int) env('CONSUMER_BLOCK_MS', 5000),

    // 连续失败多少次后进死信（0 表示不进死信，永久重试）
    'max_retries' => (int) env('CONSUMER_MAX_RETRIES', 5),
    'retry_backoff' => (float) env('CONSUMER_RETRY_BACKOFF', 2.0),

    // 死信 stream；为空则失败消息一直留在 pending
    'dead_letter' => env('CONSUMER_DLQ', 'binlog:events:dead'),

    // 接管超过该毫秒数仍未确认的消息（崩溃重启后遗留）；0 关闭
    'claim_min_idle_ms' => (int) env('CONSUMER_CLAIM_IDLE_MS', 60000),

    // ---------- 下游 ----------
    // driver: http | tcp | mysql | clickhouse | elasticsearch
    'sink' => [
        'driver' => env('CONSUMER_SINK', 'http'),

        // 只处理这些表 / 这些操作；为空表示全部
        'tables' => csv(env('CONSUMER_TABLES', '')),
        'ops'    => csv(env('CONSUMER_OPS', '')),

        'http' => [
            'url'     => env('CONSUMER_HTTP_URL', 'http://127.0.0.1:8080/binlog'),
            'headers' => [],   // 例如 ['Authorization' => 'Bearer xxx']
            'timeout' => (float) env('CONSUMER_HTTP_TIMEOUT', 5.0),
            // 5xx/网络错误时的 sink 内重试次数（4xx 不重试，直接失败）
            'retries' => (int) env('CONSUMER_HTTP_RETRIES', 0),
        ],

        'tcp' => [
            'host'    => env('CONSUMER_TCP_HOST', '127.0.0.1'),
            'port'    => (int) env('CONSUMER_TCP_PORT', 9001),
            'timeout' => (float) env('CONSUMER_TCP_TIMEOUT', 3.0),
        ],

        'mysql' => [
            'host'     => env('CONSUMER_MYSQL_HOST', '127.0.0.1'),
            'port'     => (int) env('CONSUMER_MYSQL_PORT', 3306),
            'database' => env('CONSUMER_MYSQL_DB', 'mirror'),
            'user'     => env('CONSUMER_MYSQL_USER', 'root'),
            'password' => env('CONSUMER_MYSQL_PASSWORD', ''),
            'charset'  => 'utf8mb4',
            // 目标表名前缀：源表 orders → mirror_orders
            'table_prefix' => env('CONSUMER_MYSQL_PREFIX', 'mirror_'),
        ],

        'clickhouse' => [
            'url'         => env('CONSUMER_CK_URL', 'http://127.0.0.1:8123'),
            'database'    => env('CONSUMER_CK_DB', 'default'),
            'headers'     => [],   // ['X-ClickHouse-User' => 'default', 'X-ClickHouse-Key' => '']
            'timeout'     => (float) env('CONSUMER_CK_TIMEOUT', 10.0),
            // delete: mark（写 _is_deleted=1）| skip（忽略删除）
            'delete_mode' => env('CONSUMER_CK_DELETE', 'mark'),
            'delete_column' => '_is_deleted',
        ],

        // 自定义实现：driver=custom 时用
        // 类需实现 BinlogRelay\Handler\HandlerInterface（收到 Change）
        // 或 BinlogRelay\Sink\MessageHandlerInterface（收到 Message）
        'custom' => [
            'class' => env('CONSUMER_CUSTOM_CLASS', ''),
            'args'  => [],   // 按顺序传给构造函数
        ],

        'elasticsearch' => [
            'url'          => env('CONSUMER_ES_URL', 'http://127.0.0.1:9200'),
            'index_prefix' => env('CONSUMER_ES_PREFIX', ''),
            'user'         => env('CONSUMER_ES_USER', ''),
            'password'     => env('CONSUMER_ES_PASSWORD', ''),
            'headers'      => [],
            'batch_bytes'  => 5 * 1024 * 1024,
            'timeout'      => (float) env('CONSUMER_ES_TIMEOUT', 10.0),
        ],
    ],

    'log' => [
        'level' => env('CONSUMER_LOG_LEVEL', 'info'),
        'file'  => env('CONSUMER_LOG_FILE', ''),
    ],

    'timezone' => 'Asia/Shanghai',
];
