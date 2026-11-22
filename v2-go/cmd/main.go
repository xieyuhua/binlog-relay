// binlog2redis —— MySQL binlog 中继，支持 Redis/Kafka/RabbitMQ/HTTP/MySQL 输出。
//
// 用法：
//
//	binlog2redis start                   启动增量同步
//	binlog2redis start --seconds=5       运行指定秒数后退出（联调）
//	binlog2redis fullsync                全量快照（会先记录位点）
//	binlog2redis fullsync --dry-run      只列出待同步的表
//	binlog2redis status                  查看位点
//	binlog2redis reset-position --force  重置位点
//	binlog2redis check                   连通性自检
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/biaoge/binlog2redis/internal/alert"
	"github.com/biaoge/binlog2redis/internal/checkpoint"
	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/fullsync"
	"github.com/biaoge/binlog2redis/internal/logx"
	"github.com/biaoge/binlog2redis/internal/producer"
	"github.com/biaoge/binlog2redis/internal/replication"
	"github.com/biaoge/binlog2redis/internal/version"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	command := os.Args[1]
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	configPath := fs.String("config", "", "配置文件路径（默认依次找 config.yaml / configs/config.yaml）")
	seconds := fs.Int("seconds", 0, "运行秒数后退出（0 表示常驻）")
	dryRun := fs.Bool("dry-run", false, "只列出待同步的表，不推送")
	tables := fs.String("tables", "", "只同步指定表，逗号分隔")
	force := fs.Bool("force", false, "重置位点时强制覆盖")
	alertTest := fs.Bool("alert-test", false, "发送一条测试告警，验证配置")
	_ = fs.Parse(os.Args[2:])

	cfg, err := config.Load(resolveConfigPath(*configPath))
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置错误: %v\n", err)
		os.Exit(1)
	}

	logger := logx.New(cfg.Log.Level, cfg.Log.Format, cfg.Log.File)
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var code int
	switch command {
	case "start":
		code = runStart(ctx, cfg, logger, *seconds)
	case "fullsync":
		code = runFullsync(ctx, cfg, logger, *dryRun, *tables)
	case "status":
		code = runStatus(ctx, cfg, logger)
	case "reset-position":
		code = runReset(ctx, cfg, logger, *force)
	case "check":
		code = runCheck(ctx, cfg, logger, *alertTest)
	case "version", "--version", "-v":
		showVersion()
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", command)
		usage()
		code = 2
	}
	os.Exit(code)
}

// ---------- start ----------

func runStart(ctx context.Context, cfg *config.Config, log *slog.Logger, seconds int) int {
	store, err := buildStore(ctx, cfg)
	if err != nil {
		log.Error("初始化位点存储失败", "err", err)
		return 1
	}
	defer store.Close()

	owner := fmt.Sprintf("%s-%d", hostname(), os.Getpid())
	ok, err := store.AcquireLock(ctx, owner, 60*time.Second)
	if err != nil {
		log.Error("获取位点锁失败", "err", err)
		return 1
	}
	if !ok {
		log.Error("位点已被其它实例占用（如确认无进程运行，请执行 reset-position --force）")
		return 1
	}
	defer store.ReleaseLock(ctx, owner)

	// Redis 锁有 TTL，必须定期续期；否则运行超过 TTL 后锁会失效，
	// 另一实例启动时就能抢到锁，造成双消费（文件锁不依赖 TTL，无需续期）。
	stopRefresh := startLockRefresh(ctx, store, owner, log)
	defer stopRefresh()

	prod, err := producer.New(cfg)
	if err != nil {
		log.Error("初始化输出端失败", "err", err)
		return 1
	}
	defer prod.Close()

	if seconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
		defer cancel()
		log.Info("限时运行", "seconds", seconds)
	}

	alerter := buildAlerter(cfg, log)
	syncer := replication.NewSyncer(cfg, log, prod, store, alerter)
	if err := syncer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("同步异常退出", "err", err)
		return 1
	}
	log.Info("已停止")
	return 0
}

// ---------- fullsync ----------

func runFullsync(ctx context.Context, cfg *config.Config, log *slog.Logger, dryRun bool, tablesFlag string) int {
	db, err := openMySQL(cfg)
	if err != nil {
		log.Error("连接 MySQL 失败", "err", err)
		return 1
	}
	defer db.Close()

	// 1. 先记录位点（必须在快照之前，否则会丢快照期间的变更）
	store, err := buildStore(ctx, cfg)
	if err != nil {
		log.Error("初始化位点存储失败", "err", err)
		return 1
	}
	defer store.Close()

	// 2. 列表
	var only []string
	if tablesFlag != "" {
		for _, t := range strings.Split(tablesFlag, ",") {
			if t = strings.TrimSpace(t); t != "" {
				only = append(only, t)
			}
		}
	}

	planner := fullsync.NewSyncer(db, log, nil, 0)
	tbls, err := planner.ListTables(ctx, cfg, only, cfg.Runtime.IncludeViews)
	if err != nil {
		log.Error("列出待同步表失败", "err", err)
		return 1
	}
	if len(tbls) == 0 {
		log.Error("没有匹配到待同步的表，请检查 filter.databases / filter.tables")
		return 1
	}

	log.Info("待同步对象", "count", len(tbls))
	if dryRun {
		for _, t := range tbls {
			kind := ""
			if t.IsView {
				kind = "[视图] "
			}
			fmt.Printf("  %s%s.%s\n", kind, t.Database, t.Table)
		}
		fmt.Println("\n（--dry-run，未推送任何数据）")
		return 0
	}

	// 3. 记录起始位点
	startPos, err := readMasterStatus(ctx, db)
	if err != nil {
		log.Error("读取 binlog 位点失败", "err", err)
		return 1
	}

	owner := fmt.Sprintf("fullsync-%d", os.Getpid())
	ok, err := store.AcquireLock(ctx, owner, 30*time.Minute)
	if err != nil || !ok {
		log.Error("位点被占用（relay 可能在运行）。全量同步请先停止 relay")
		return 1
	}
	_ = store.Save(ctx, &checkpoint.Position{
		BinlogFile: startPos.Name,
		BinlogPos:  int64(startPos.Pos),
		UpdatedAt:  time.Now().Unix(),
		Owner:      owner,
	})
	store.ReleaseLock(ctx, owner)
	log.Info("起始位点已记录", "file", startPos.Name, "pos", startPos.Pos)

	// 4. 推送快照
	prod, err := producer.New(cfg)
	if err != nil {
		log.Error("初始化输出端失败", "err", err)
		return 1
	}
	defer prod.Close()

	syncer := fullsync.NewSyncer(db, log, prod, cfg.Runtime.FullSyncBatch)
	var grand int64
	began := time.Now()

	for i, t := range tbls {
		n, err := syncer.SyncTable(ctx, t)
		if err != nil {
			log.Error("同步表失败", "table", t.Database+"."+t.Table, "err", err)
			return 1
		}
		grand += n
		log.Info("表同步完成",
			"progress", fmt.Sprintf("%d/%d", i+1, len(tbls)),
			"table", t.Database+"."+t.Table,
			"rows", n)
	}

	cost := time.Since(began)
	log.Info("全量同步完成",
		"tables", len(tbls),
		"rows", grand,
		"seconds", fmt.Sprintf("%.1f", cost.Seconds()),
		"rows_per_sec", int64(float64(grand)/cost.Seconds()))

	fmt.Printf("\n下一步：binlog2redis start（会从 %s:%d 回放增量）\n", startPos.Name, startPos.Pos)
	return 0
}

// ---------- status / reset / check ----------

func runStatus(ctx context.Context, cfg *config.Config, log *slog.Logger) int {
	store, err := buildStore(ctx, cfg)
	if err != nil {
		log.Error("初始化位点存储失败", "err", err)
		return 1
	}
	defer store.Close()

	pos, err := store.Load(ctx)
	if err != nil {
		log.Error("读取位点失败", "err", err)
		return 1
	}

	fmt.Printf("存储后端: %s\n", store.Name())
	if pos == nil {
		fmt.Println("尚无位点（首次启动将从最新 binlog 末尾开始）")
		return 0
	}
	fmt.Printf("binlog:   %s:%d\n", pos.BinlogFile, pos.BinlogPos)
	if pos.Gtid != "" {
		fmt.Printf("GTID:     %s\n", truncate(pos.Gtid, 120))
	}
	fmt.Printf("累计事件: %d\n", pos.Events)
	if pos.UpdatedAt > 0 {
		fmt.Printf("最后更新: %s\n", time.Unix(pos.UpdatedAt, 0).Format("2006-01-02 15:04:05"))
	}
	return 0
}

func runReset(ctx context.Context, cfg *config.Config, log *slog.Logger, force bool) int {
	store, err := buildStore(ctx, cfg)
	if err != nil {
		log.Error("初始化位点存储失败", "err", err)
		return 1
	}
	defer store.Close()

	pos, err := store.Load(ctx)
	if err != nil {
		log.Error("读取位点失败", "err", err)
		return 1
	}
	if pos != nil && !force {
		fmt.Fprintf(os.Stderr, "位点存在（%s:%d），确认重置请加 --force\n", pos.BinlogFile, pos.BinlogPos)
		return 2
	}

	owner := fmt.Sprintf("reset-%d", os.Getpid())
	ok, err := store.AcquireLock(ctx, owner, 10*time.Second)
	if err != nil || !ok {
		fmt.Fprintln(os.Stderr, "服务正在运行并持有锁，请先停止")
		return 3
	}
	defer store.ReleaseLock(ctx, owner)

	if err := store.Save(ctx, &checkpoint.Position{}); err != nil {
		log.Error("重置位点失败", "err", err)
		return 1
	}
	fmt.Println("位点已重置")
	return 0
}

func runCheck(ctx context.Context, cfg *config.Config, log *slog.Logger, alertTest bool) int {
	ok := true

	// 1. MySQL
	db, err := openMySQL(cfg)
	if err != nil {
		fmt.Printf("[FAIL] MySQL 连接: %v\n", err)
		ok = false
	} else {
		defer db.Close()
		fmt.Println("[OK]   MySQL 连接")

		vars := map[string]string{}
		if rows, err := db.QueryContext(ctx,
			`SHOW GLOBAL VARIABLES WHERE Variable_name IN
			 ('binlog_format','binlog_row_image','log_bin','binlog_row_metadata')`); err == nil {
			for rows.Next() {
				var k, v string
				_ = rows.Scan(&k, &v)
				vars[k] = v
			}
			rows.Close()
		}

		expect := map[string]string{"log_bin": "ON", "binlog_format": "ROW", "binlog_row_image": "FULL"}
		for k, want := range expect {
			got := strings.ToUpper(vars[k])
			if got == want {
				fmt.Printf("[OK]   %s=%s\n", k, got)
			} else {
				fmt.Printf("[FAIL] %s=%s（应为 %s）\n", k, got, want)
				ok = false
			}
		}
		if meta := strings.ToUpper(vars["binlog_row_metadata"]); meta != "" && meta != "FULL" {
			fmt.Printf("[WARN] binlog_row_metadata=%s（建议 FULL，否则列名可能拿不到）\n", meta)
		}

		// 复制权限：实际执行一次 SHOW MASTER STATUS 更有说服力
		if _, err := readMasterStatus(ctx, db); err != nil {
			fmt.Printf("[FAIL] REPLICATION CLIENT 权限: %v\n", err)
			ok = false
		} else {
			fmt.Println("[OK]   REPLICATION CLIENT（可读取位点）")
		}
	}

	// 2. 位点存储
	if store, err := buildStore(ctx, cfg); err != nil {
		fmt.Printf("[FAIL] 位点存储: %v\n", err)
		ok = false
	} else {
		fmt.Printf("[OK]   位点存储: %s\n", store.Name())
		store.Close()
	}

	// 3. 输出端
	if prod, err := producer.New(cfg); err != nil {
		fmt.Printf("[FAIL] 输出端: %v\n", err)
		ok = false
	} else {
		fmt.Printf("[OK]   输出端: %s\n", prod.Name())
		prod.Close()
	}

	// 4. 告警通道：配置校验 + 可选发送测试
	if !cfg.Alert.Enabled() {
		fmt.Println("[WARN] 未配置告警通道（企微/钉钉），故障时只能看日志")
	} else {
		if cfg.Alert.WeCom.Key != "" {
			key := cfg.Alert.WeCom.Key
			if len(key) > 8 {
				key = key[:8]
			}
			fmt.Printf("[OK]   企微告警已配置（key: %s...）\n", key)
		}
		if cfg.Alert.DingTalk.Token != "" {
			mode := "IP 白名单"
			if cfg.Alert.DingTalk.Secret != "" {
				mode = "加签"
			} else if cfg.Alert.DingTalk.Keyword != "" {
				mode = "关键词"
			}
			fmt.Printf("[OK]   钉钉告警已配置（安全模式：%s）\n", mode)
		}

		if alertTest {
			buildAlerter(cfg, log).Alert(
				"check-test",
				"binlog2redis 告警测试",
				"收到这条消息说明告警通道配置正确。",
				map[string]string{"主机": hostname(), "时间": time.Now().Format("2006-01-02 15:04:05")},
			)
			fmt.Println("[OK]   测试告警已发送，请检查群消息")
		} else {
			fmt.Println("[WARN] 加 --alert-test 可发送一条测试告警，验证配置是否生效")
		}
	}

	if ok {
		fmt.Println("\n自检通过")
		return 0
	}
	fmt.Println("\n自检未通过，请修复上面的 FAIL 项")
	return 1
}

// showVersion 打印版本与环境信息
func showVersion() {
	fmt.Printf("binlog2redis %s\n", version.String())
	for k, v := range version.Environment() {
		fmt.Printf("  %-7s %s\n", k, v)
	}
}

// ---------- 辅助 ----------

// buildAlerter 按配置构造告警管理器；未配置任何通道时返回空管理器（所有调用是空操作）
func buildAlerter(cfg *config.Config, log *slog.Logger) *alert.Manager {
	m := alert.NewManager(log, cfg.Alert.Cooldown, cfg.Alert.MaxPerMinute)

	if cfg.Alert.WeCom.Key != "" {
		m.Add(alert.NewWeCom(cfg.Alert.WeCom.Key))
		log.Info("告警通道已启用", "channel", "wecom")
	}
	if cfg.Alert.DingTalk.Token != "" {
		m.Add(alert.NewDingTalk(
			cfg.Alert.DingTalk.Token,
			cfg.Alert.DingTalk.Secret,
			cfg.Alert.DingTalk.Keyword,
		))
		log.Info("告警通道已启用", "channel", "dingtalk")
	}

	return m
}

// startLockRefresh 每 20 秒续期一次（TTL 60 秒，留足余量）。
// 返回的函数用于停止续期。
func startLockRefresh(
	ctx context.Context, store checkpoint.Store, owner string, log *slog.Logger,
) func() {
	refreshCtx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-ticker.C:
				if err := store.RefreshLock(refreshCtx, owner, 60*time.Second); err != nil {
					// 续期失败通常意味着 Redis 短暂不可用；下一轮会重试。
					// 若持续失败，锁会过期，需要人工介入。
					log.Warn("锁续期失败", "err", err)
				}
			}
		}
	}()

	return cancel
}

func buildStore(ctx context.Context, cfg *config.Config) (checkpoint.Store, error) {
	if cfg.Checkpoint.Driver == "redis" {
		client, err := newRedisClient(cfg.Checkpoint.Redis)
		if err != nil {
			return nil, err
		}
		// 连通性检查：配置错时尽早失败
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("Redis 连接失败: %w", err)
		}
		return checkpoint.NewRedisStore(client, cfg.Checkpoint.Key), nil
	}
	return checkpoint.NewFileStore(cfg.Checkpoint.Path)
}

func openMySQL(cfg *config.Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s&timeout=10s&parseTime=true",
		cfg.MySQL.User, cfg.MySQL.Password,
		cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func readMasterStatus(ctx context.Context, db *sql.DB) (pos mysqlPosition, err error) {
	var (
		file, doDB, ignoreDB, gtid sql.NullString
		position                   uint32
	)
	row := db.QueryRowContext(ctx, "SHOW MASTER STATUS")
	if err = row.Scan(&file, &position, &doDB, &ignoreDB, &gtid); err != nil {
		// MySQL 8.4 起改名
		row = db.QueryRowContext(ctx, "SHOW BINARY LOG STATUS")
		if err2 := row.Scan(&file, &position, &doDB, &ignoreDB, &gtid); err2 != nil {
			return pos, fmt.Errorf("需要 REPLICATION CLIENT 权限: %w", err)
		}
	}
	return mysqlPosition{Name: file.String, Pos: position}, nil
}

type mysqlPosition struct {
	Name string
	Pos  uint32
}

func resolveConfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if v := os.Getenv("B2R_CONFIG"); v != "" {
		return v
	}
	for _, p := range []string{"config.yaml", "configs/config.yaml", "config/config.yaml"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func usage() {
	fmt.Print(`binlog2redis —— MySQL binlog 中继

用法：
  binlog2redis start [--seconds=N]      启动增量同步（Ctrl+C 优雅退出）
  binlog2redis fullsync [--dry-run]     全量快照（会先记录位点）
  binlog2redis fullsync --tables=a,b    只同步指定表
  binlog2redis status                   查看位点
  binlog2redis reset-position --force   重置位点
  binlog2redis check [--alert-test]     MySQL / 存储 / 输出端 / 告警 自检
  binlog2redis version                  版本与环境信息

配置：
  --config=path   指定配置文件（默认依次找 config.yaml / configs/config.yaml）
  环境变量 B2R_CONFIG 也可指定路径

环境变量（覆盖配置）：
  B2R_MYSQL_HOST / B2R_MYSQL_PORT / B2R_MYSQL_USER / B2R_MYSQL_PASSWORD
  B2R_SERVER_ID            复制身份，集群内必须唯一
  B2R_PRODUCER             stdout | redis | kafka | rabbitmq | http | mysql
  B2R_REDIS_ADDR / B2R_REDIS_STREAM
  B2R_KAFKA_BROKERS / B2R_KAFKA_TOPIC
  B2R_RABBITMQ_URL
  B2R_HTTP_URL
  B2R_DATABASES / B2R_TABLES
  B2R_CHECKPOINT           file | redis
  B2R_GTID_SET             1 = 累积完整 GTID 集合
  B2R_EMIT_DDL             1 = 输出 DDL 事件
`)
}
