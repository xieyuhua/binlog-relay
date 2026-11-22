package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是完整配置。所有字段都可用环境变量覆盖，便于容器化部署。
type Config struct {
	MySQL      MySQLConfig      `yaml:"mysql"`
	Filter     FilterConfig     `yaml:"filter"`
	Producer   ProducerConfig   `yaml:"producer"`
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	Log        LogConfig        `yaml:"log"`
	Runtime    RuntimeConfig    `yaml:"runtime"`
}

type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Charset  string `yaml:"charset"`

	// ServerID 是复制身份，集群内必须唯一
	ServerID uint32 `yaml:"server_id"`
	// Flavor: mysql | mariadb
	Flavor string `yaml:"flavor"`
	// HeartbeatPeriod 空闲保活间隔
	HeartbeatPeriod time.Duration `yaml:"heartbeat_period"`
	// ReadTimeout 读超时，需大于心跳间隔
	ReadTimeout time.Duration `yaml:"read_timeout"`
}

type FilterConfig struct {
	Databases       []string `yaml:"databases"`
	Tables          []string `yaml:"tables"`
	IgnoreDatabases []string `yaml:"ignore_databases"`
	IgnoreTables    []string `yaml:"ignore_tables"`

	// OnlyConfigured 只处理 tables 或 columns 里出现过的表
	OnlyConfigured bool `yaml:"only_configured"`
	// EmitDDL 是否输出 DDL 事件
	EmitDDL bool `yaml:"emit_ddl"`

	// Columns 按表裁剪列；"*" 作为兜底
	Columns map[string][]string `yaml:"columns"`
}

type ProducerConfig struct {
	// Type: stdout | redis | kafka | rabbitmq | http | mysql
	Type string `yaml:"type"`

	// 多主题扇出（redis/kafka/rabbitmq 通用）
	Topics []TopicRule `yaml:"topics"`

	Redis  RedisConfig  `yaml:"redis"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	Rabbit RabbitConfig `yaml:"rabbitmq"`
	HTTP   HTTPConfig   `yaml:"http"`
	MySQL  MySQLSink    `yaml:"mysqlsink"`
}

// TopicRule 让一条变更可以路由到不同目标
type TopicRule struct {
	Target    string   `yaml:"target"` // 必填：stream / topic / queue / routing_key
	Databases []string `yaml:"databases"`
	Tables    []string `yaml:"tables"`
	Ops       []string `yaml:"ops"` // insert / update / delete / ddl
	// Labels 是该 topic 专属的静态标签，合并进消息的 labels 字段。
	// 消费端可据此区分来源/环境，例如 env=prod、source=mysql-main。
	Labels map[string]string `yaml:"labels"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	// Stream 单主题模式下的目标
	Stream string `yaml:"stream"`
	// MaxLen 近似裁剪长度，0 表示不裁剪
	MaxLen int64 `yaml:"max_len"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	// BatchBytes / BatchTimeout 影响吞吐与延迟
	BatchBytes   int           `yaml:"batch_bytes"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
	RequiredAcks int           `yaml:"required_acks"` // 0/1/-1
	Compression  string        `yaml:"compression"`   // none|gzip|snappy|lz4|zstd
}

type RabbitConfig struct {
	URL        string `yaml:"url"`
	Exchange   string `yaml:"exchange"`
	Queue      string `yaml:"queue"`
	RoutingKey string `yaml:"routing_key"`
	// Durable 队列/交换机持久化
	Durable bool `yaml:"durable"`
	// Confirm 开启 publisher confirm，保证写入成功
	Confirm bool `yaml:"confirm"`
}

type HTTPConfig struct {
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Timeout time.Duration     `yaml:"timeout"`
}

type MySQLSink struct {
	DSN         string `yaml:"dsn"`
	TablePrefix string `yaml:"table_prefix"`
	BatchSize   int    `yaml:"batch_size"`
}

type CheckpointConfig struct {
	// Driver: redis | file
	Driver string `yaml:"driver"`
	Key    string `yaml:"key"`

	Redis RedisConfig `yaml:"redis"`
	Path  string      `yaml:"path"`

	// EveryEvents / EverySeconds 批量落盘，降低写放大
	EveryEvents  int           `yaml:"every_events"`
	EverySeconds time.Duration `yaml:"every_seconds"`
	// Fsync 文件模式是否强制落盘（慢，默认关）
	Fsync bool `yaml:"fsync"`
	// GtidSet 是否累积完整 GTID 执行集合
	GtidSet bool `yaml:"gtid_set"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"` // text | json
	File   string `yaml:"file"`
}

type RuntimeConfig struct {
	// BatchSize 攒够多少行提交一次
	BatchSize int `yaml:"batch_size"`
	// FullSyncBatch 全量同步每批行数
	FullSyncBatch int `yaml:"fullsync_batch"`
	// IncludeViews 全量同步是否包含视图。
	// 视图数据来自基表，会与基表数据重复入队，下游需按 table 区分。
	IncludeViews bool `yaml:"include_views"`
}

// Default 返回带合理默认值的配置
func Default() *Config {
	return &Config{
		MySQL: MySQLConfig{
			Host:            "127.0.0.1",
			Port:            3306,
			User:            "repl",
			Charset:         "utf8mb4",
			ServerID:        8811,
			Flavor:          "mysql",
			HeartbeatPeriod: 30 * time.Second,
			ReadTimeout:     90 * time.Second,
		},
		Filter: FilterConfig{
			IgnoreDatabases: []string{"mysql", "sys", "information_schema", "performance_schema"},
		},
		Producer: ProducerConfig{
			Type: "stdout",
			Redis: RedisConfig{
				Addr:   "127.0.0.1:6379",
				Stream: "binlog:events",
				MaxLen: 200000,
			},
			Kafka: KafkaConfig{
				Topic:        "binlog-events",
				BatchBytes:   1 << 20,
				BatchTimeout: 500 * time.Millisecond,
				RequiredAcks: -1,
				Compression:  "snappy",
			},
			Rabbit: RabbitConfig{
				URL:      "amqp://guest:guest@127.0.0.1:5672/",
				Exchange: "binlog",
				Durable:  true,
				Confirm:  true,
			},
			HTTP: HTTPConfig{Timeout: 5 * time.Second},
			MySQL: MySQLSink{
				TablePrefix: "mirror_",
				BatchSize:   500,
			},
		},
		Checkpoint: CheckpointConfig{
			Driver:       "file",
			Key:          "binlog2redis:checkpoint:default",
			Path:         "var/checkpoint.json",
			EveryEvents:  100,
			EverySeconds: time.Second,
		},
		Log: LogConfig{Level: "info", Format: "text"},
		Runtime: RuntimeConfig{
			BatchSize:     200,
			FullSyncBatch: 1000,
		},
	}
}

// Load 读取 YAML 配置并用环境变量覆盖
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("解析配置文件失败: %w", err)
		}
	}

	applyEnv(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyEnv 用环境变量覆盖配置，命名规则：B2R_<段>_<字段>
func applyEnv(c *Config) {
	envStr(&c.MySQL.Host, "B2R_MYSQL_HOST")
	envInt(&c.MySQL.Port, "B2R_MYSQL_PORT")
	envStr(&c.MySQL.User, "B2R_MYSQL_USER")
	envStr(&c.MySQL.Password, "B2R_MYSQL_PASSWORD")
	envUint32(&c.MySQL.ServerID, "B2R_SERVER_ID")
	envStr(&c.MySQL.Flavor, "B2R_MYSQL_FLAVOR")

	envStr(&c.Producer.Type, "B2R_PRODUCER")
	envStr(&c.Producer.Redis.Addr, "B2R_REDIS_ADDR")
	envStr(&c.Producer.Redis.Password, "B2R_REDIS_PASSWORD")
	envInt(&c.Producer.Redis.DB, "B2R_REDIS_DB")
	envStr(&c.Producer.Redis.Stream, "B2R_REDIS_STREAM")
	envStr(&c.Producer.Kafka.Topic, "B2R_KAFKA_TOPIC")
	envStr(&c.Producer.Rabbit.URL, "B2R_RABBITMQ_URL")
	envStr(&c.Producer.HTTP.URL, "B2R_HTTP_URL")

	envStr(&c.Checkpoint.Driver, "B2R_CHECKPOINT")
	envStr(&c.Checkpoint.Key, "B2R_CHECKPOINT_KEY")
	envStr(&c.Checkpoint.Path, "B2R_CHECKPOINT_PATH")
	envBool(&c.Checkpoint.GtidSet, "B2R_GTID_SET")

	envStr(&c.Log.Level, "B2R_LOG_LEVEL")

	if v := os.Getenv("B2R_KAFKA_BROKERS"); v != "" {
		c.Producer.Kafka.Brokers = splitCSV(v)
	}
	if v := os.Getenv("B2R_DATABASES"); v != "" {
		c.Filter.Databases = splitCSV(v)
	}
	if v := os.Getenv("B2R_TABLES"); v != "" {
		c.Filter.Tables = splitCSV(v)
	}
	envBool(&c.Filter.OnlyConfigured, "B2R_ONLY_CONFIGURED")
	envBool(&c.Filter.EmitDDL, "B2R_EMIT_DDL")
	envInt(&c.Runtime.BatchSize, "B2R_BATCH_SIZE")
}

// Validate 检查必填项与依赖关系
func (c *Config) Validate() error {
	if c.MySQL.Host == "" {
		return fmt.Errorf("mysql.host 不能为空")
	}
	if c.MySQL.User == "" {
		return fmt.Errorf("mysql.user 不能为空")
	}
	if c.MySQL.ServerID == 0 {
		return fmt.Errorf("mysql.server_id 不能为 0（必须是集群内唯一的正整数）")
	}

	switch c.Producer.Type {
	case "stdout":
	case "redis":
		if c.Producer.Redis.Addr == "" {
			return fmt.Errorf("producer.type=redis 需要 producer.redis.addr")
		}
	case "kafka":
		if len(c.Producer.Kafka.Brokers) == 0 {
			return fmt.Errorf("producer.type=kafka 需要 producer.kafka.brokers")
		}
	case "rabbitmq":
		if c.Producer.Rabbit.URL == "" {
			return fmt.Errorf("producer.type=rabbitmq 需要 producer.rabbitmq.url")
		}
	case "http":
		if c.Producer.HTTP.URL == "" {
			return fmt.Errorf("producer.type=http 需要 producer.http.url")
		}
	case "mysql":
		if c.Producer.MySQL.DSN == "" {
			return fmt.Errorf("producer.type=mysql 需要 producer.mysqlsink.dsn")
		}
	default:
		return fmt.Errorf("未知的 producer.type: %s（可选 stdout/redis/kafka/rabbitmq/http/mysql）", c.Producer.Type)
	}

	if c.Checkpoint.Driver != "redis" && c.Checkpoint.Driver != "file" {
		return fmt.Errorf("checkpoint.driver 必须是 redis 或 file")
	}
	if c.Checkpoint.Driver == "redis" && c.Checkpoint.Redis.Addr == "" {
		return fmt.Errorf("checkpoint.driver=redis 需要 checkpoint.redis.addr")
	}

	return nil
}

func envStr(dst *string, key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		*dst = v
	}
}

func envInt(dst *int, key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}

func envUint32(dst *uint32, key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			*dst = uint32(n)
		}
	}
}

func envBool(dst *bool, key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		*dst = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
