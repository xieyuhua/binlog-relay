<?php
/**
 * binlog2redis 配置
 *
 * 所有项都可以用环境变量覆盖，方便容器化部署。
 */

return [
    // ---------- MySQL 主库 ----------
    'mysql' => [
        'host'     => env('RELAY_MYSQL_HOST', '127.0.0.1'),
        'port'     => (int) env('RELAY_MYSQL_PORT', 3306),
        'user'     => env('RELAY_MYSQL_USER', 'repl'),
        'password' => env('RELAY_MYSQL_PASSWORD', ''),
        'charset'  => env('RELAY_MYSQL_CHARSET', 'utf8mb4'),

        // 从库身份，集群内必须唯一
        'slave_id' => (int) env('RELAY_SLAVE_ID', 8811),

        // 心跳秒数，空闲保活
        'heartbeat' => (float) env('RELAY_HEARTBEAT', 30),

        // 表结构缓存条数
        'table_cache_size' => (int) env('RELAY_TABLE_CACHE', 256),
    ],

    // ---------- 订阅范围 ----------
    'filter' => [
        'databases' => csv(env('RELAY_DATABASES', '')),
        'tables'    => csv(env('RELAY_TABLES', '')),
        'ignore_databases' => csv(env('RELAY_IGNORE_DATABASES', 'mysql,sys,information_schema,performance_schema')),
        'ignore_tables'    => csv(env('RELAY_IGNORE_TABLES', '')),

        // 1 = 只处理下面 tables / columns 里配置过的表，其余整表丢弃
        'only_configured' => (bool) env('RELAY_ONLY_CONFIGURED', false),

        // 是否同步 DDL（建表/改表等结构变更）
        // 开启后会额外订阅 QUERY 事件，处理器需实现 DdlHandlerInterface
        'emit_ddl' => (bool) env('RELAY_EMIT_DDL', false),

        // 列裁剪：只保留业务需要的列
        'columns' => [
            // 'orders' => ['id', 'status', 'amount', 'updated_at'],
            // 'cms_article' => ['id', 'title', 'status'],
            // '*' => ['id'],
        ],
    ],

    // ---------- Redis 输出 ----------
    'redis' => [
        'host'     => env('RELAY_REDIS_HOST', '127.0.0.1'),
        'port'     => (int) env('RELAY_REDIS_PORT', 6379),
        'password' => env('RELAY_REDIS_PASSWORD', ''),
        'database' => (int) env('RELAY_REDIS_DB', 0),
        'timeout'  => 2.0,

        // 单 topic（简单场景）
        'stream' => env('RELAY_STREAM', 'binlog:events'),

        // 多 topic（按表/操作扇出）；配置后忽略上面的 stream
        // 'topics' => [
        //     ['stream' => 'topic:order',  'tables' => ['orders', 'order_items']],
        //     ['stream' => 'topic:stock',  'tables' => ['stock', 'stock_log']],
        //     ['stream' => 'topic:search', 'tables' => ['*']],
        //     ['stream' => 'topic:audit',  'tables' => ['orders'], 'ops' => ['delete']],
        //
        //     // 每个 topic 可带静态标签，合并进消息顶层（消费端据此区分来源/环境）
        //     ['stream' => 'topic:prod', 'tables' => ['*'],
        //      'labels' => ['env' => 'prod', 'source' => 'mysql-main']],
        // ],

        // 近似裁剪（O(1)），防止消费端宕机时 Redis 无限增长
        'max_len' => (int) env('RELAY_STREAM_MAX_LEN', 200000),
    ],

    // ---------- 点位存储 ----------
    'checkpoint' => [
        'driver' => env('RELAY_CHECKPOINT', 'file'),   // redis | file
        'key'    => env('RELAY_CHECKPOINT_KEY', 'binlog_relay:checkpoint:default'),

        'redis' => [
            'host'     => env('RELAY_REDIS_HOST', '127.0.0.1'),
            'port'     => (int) env('RELAY_REDIS_PORT', 6379),
            'password' => env('RELAY_REDIS_PASSWORD', ''),
            'database' => (int) env('RELAY_REDIS_DB', 0),
            'timeout'  => 2.0,
        ],

        'file' => [
            'path'  => env('RELAY_FILE_PATH', __DIR__ . '/../var/checkpoint.json'),
            'fsync' => (bool) env('RELAY_FILE_FSYNC', false),
        ],

        // 每 N 个事件落一次点位
        'every_events' => (int) env('RELAY_CHECKPOINT_EVERY', 100),
        // 或每 N 秒（先到先触发）
        'every_seconds' => (float) env('RELAY_CHECKPOINT_SECONDS', 1.0),
    ],

    // 是否累积完整的 GTID 执行集合（跨 binlog 文件续传时需要）
    // 关闭时点位里只记最后事务的 GTID，仅作参考
    'gtid_set' => (bool) env('RELAY_GTID_SET', false),

    // 全量同步参数
    'fullsync' => [
        // 每批从源库读取的行数
        'batch_size' => (int) env('RELAY_FULLSYNC_BATCH', 1000),
        // 是否把视图也纳入全量同步
        // 注意：视图数据来自基表，会与基表数据重复入队，下游需按 table 区分
        'include_views' => (bool) env('RELAY_FULLSYNC_VIEWS', false),
    ],

    // 每批交给写入器的变更行数
    'batch_size' => (int) env('RELAY_BATCH_SIZE', 200),

    'log' => [
        'level' => env('RELAY_LOG_LEVEL', 'info'),
        'file'  => env('RELAY_LOG_FILE', ''),   // 空则输出到 stderr
    ],

    'timezone' => 'Asia/Shanghai',
];
