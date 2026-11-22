package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/biaoge/binlog2redis/internal/alert"
	"github.com/biaoge/binlog2redis/internal/checkpoint"
	"github.com/biaoge/binlog2redis/internal/config"
	"github.com/biaoge/binlog2redis/internal/event"
	"github.com/biaoge/binlog2redis/internal/filter"
	"github.com/biaoge/binlog2redis/internal/producer"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// Syncer 串联整条链路：读 binlog → 归一化 → 过滤 → 投递 → 推进位点。
type Syncer struct {
	cfg      *config.Config
	log      *slog.Logger
	parser   *Parser
	schema   *SchemaLoader
	producer producer.Producer
	store    checkpoint.Store
	alerter  *alert.Manager

	// 批量缓冲
	buf       []*event.Envelope
	bufRows   int
	lastFlush time.Time

	// 位点
	cdc    *checkpoint.Position
	store_ *sql.DB

	// binlog 复制连接
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	gtidSet   *checkpoint.GtidSet
	eventsCnt atomic.Int64
	running   atomic.Bool

	// 位点落盘控制：按事件数或时间间隔批量写，避免每事务一次 IO
	eventsSincePersist int
	lastPersistAt      time.Time
}

// NewSyncer 构造同步器
func NewSyncer(cfg *config.Config, log *slog.Logger,
	prod producer.Producer, store checkpoint.Store, alerter *alert.Manager) *Syncer {
	return &Syncer{
		cfg:           cfg,
		log:           log,
		parser:        NewParser(filter.New(cfg.Filter), cfg.Filter.EmitDDL),
		producer:      prod,
		store:         store,
		alerter:       alerter,
		lastFlush:     time.Now(),
		lastPersistAt: time.Now(),
	}
}

// Run 启动同步循环，直到 ctx 取消
func (s *Syncer) Run(ctx context.Context) error {
	if err := s.setup(); err != nil {
		return err
	}
	defer func() {
		// 退出前把缓冲投递出去，并保存位点。
		// 用独立 ctx：此时 ctx 通常已取消，但清理必须完成。
		cleanupCtx := context.WithoutCancel(ctx)

		if len(s.buf) > 0 {
			s.log.Info("退出前还有未投递的数据", "count", len(s.buf))
			if err := s.flush(cleanupCtx); err != nil {
				s.log.Error("退出前投递失败，这些数据会由重启后重放",
					"count", len(s.buf), "err", err)
			}
		}
		if err := s.persist(cleanupCtx); err != nil {
			s.log.Error("退出前保存位点失败", "err", err)
		}
	}()

	s.running.Store(true)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("收到停止信号，处理完当前批次后退出")
			return nil
		default:
		}

		be, err := s.streamer.GetEvent(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			// 读超时是正常的（心跳间隔内没有事件），继续循环
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}

			// 连接类错误可恢复：MySQL 重启、网络抖动、主从切换都会导致断流。
			// 直接退出会让运维必须人工拉起，所以这里自动重连。
			if s.reconnect(ctx, err) {
				continue
			}
			return fmt.Errorf("读取 binlog 失败: %w", err)
		}

		if err := s.handleEvent(ctx, be); err != nil {
			return err
		}
	}
}

// reconnect 断线后重建 binlog 连接。
//
// 返回 false 表示放弃（ctx 取消或重试耗尽），调用方应退出。
//
// 关键点：重连前先把缓冲投递出去并保存位点，否则重连后从旧位点重读会重复，
// 而缓冲里的数据会丢。
func (s *Syncer) reconnect(ctx context.Context, cause error) bool {
	const maxAttempts = 10

	s.log.Warn("binlog 连接中断，准备重连", "err", cause)

	// 先把已解析但未投递的数据落地
	if err := s.flush(context.WithoutCancel(ctx)); err != nil {
		s.log.Error("重连前投递失败", "err", err)
	}
	if err := s.persist(context.WithoutCancel(ctx)); err != nil {
		s.log.Error("重连前保存位点失败", "err", err)
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 指数退避，上限 30 秒：2s, 4s, 8s, 16s, 30s...
		backoff := time.Duration(1<<uint(attempt)) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}

		if err := s.reattach(ctx); err != nil {
			s.log.Warn("重连失败，稍后重试",
				"attempt", attempt, "max", maxAttempts, "err", err)
			continue
		}

		s.log.Info("重连成功，从当前位点继续", "attempt", attempt)
		return true
	}

	s.alerter.Alert(
		"reconnect",
		"binlog2redis 重连失败",
		fmt.Sprintf("重试 %d 次后仍然无法连接 MySQL，进程即将退出。", maxAttempts),
		map[string]string{
			"MySQL": s.cfg.MySQL.Host,
			"错误":    cause.Error(),
			"主机":    s.owner(),
		},
	)
	return false
}

// reattach 重建 BinlogSyncer 并从已保存的位点续传
func (s *Syncer) reattach(ctx context.Context) error {
	if s.syncer != nil {
		s.syncer.Close()
	}

	bs := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        s.cfg.MySQL.ServerID,
		Flavor:          s.cfg.MySQL.Flavor,
		Host:            s.cfg.MySQL.Host,
		Port:            uint16(s.cfg.MySQL.Port),
		User:            s.cfg.MySQL.User,
		Password:        s.cfg.MySQL.Password,
		Charset:         s.cfg.MySQL.Charset,
		HeartbeatPeriod: s.cfg.MySQL.HeartbeatPeriod,
		ReadTimeout:     s.cfg.MySQL.ReadTimeout,
		ParseTime:       true,
		Logger:          s.log,
	})

	// 从内存中的最新位点续传（比重读存储更准，因为可能还没落盘）
	pos := mysql.Position{}
	if s.cdc != nil && s.cdc.HasFilePosition() {
		pos = mysql.Position{Name: s.cdc.BinlogFile, Pos: uint32(s.cdc.BinlogPos)}
	} else {
		// 没有位点就从头读主库当前位置
		latest, err := s.masterStatus(ctx)
		if err != nil {
			bs.Close()
			return err
		}
		pos = latest
	}

	streamer, err := bs.StartSync(pos)
	if err != nil {
		bs.Close()
		return fmt.Errorf("重新开始同步失败: %w", err)
	}

	s.syncer = bs
	s.streamer = streamer
	return nil
}

func (s *Syncer) setup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. 连 MySQL，用于查 schema 与位点
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s&timeout=10s",
		s.cfg.MySQL.User, s.cfg.MySQL.Password,
		s.cfg.MySQL.Host, s.cfg.MySQL.Port, s.cfg.MySQL.Charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接 MySQL 失败: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("MySQL ping 失败: %w", err)
	}
	s.store_ = db
	s.schema = NewSchemaLoader(db)

	// 2. 读位点
	pos, err := s.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("读取位点失败: %w", err)
	}
	if pos != nil {
		s.cdc = pos
		s.log.Info("从已保存位点恢复",
			"file", pos.BinlogFile, "pos", pos.BinlogPos,
			"gtid", pos.Gtid, "events", pos.Events)
	} else {
		s.log.Info("无历史位点，将从当前 binlog 末尾开始")
	}

	// 3. GTID 集合
	if s.cfg.Checkpoint.GtidSet {
		gset, err := checkpoint.ParseGtidSet("")
		if err != nil {
			return err
		}
		if pos != nil && pos.Gtid != "" {
			if parsed, err2 := checkpoint.ParseGtidSet(pos.Gtid); err2 == nil {
				gset = parsed
			}
		}
		s.gtidSet = gset
		s.log.Info("已启用 GTID 集合累积", "initial", gset.String())
	}

	// 4. 建立 binlog 复制连接
	bs := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        s.cfg.MySQL.ServerID,
		Flavor:          s.cfg.MySQL.Flavor,
		Host:            s.cfg.MySQL.Host,
		Port:            uint16(s.cfg.MySQL.Port),
		User:            s.cfg.MySQL.User,
		Password:        s.cfg.MySQL.Password,
		Charset:         s.cfg.MySQL.Charset,
		HeartbeatPeriod: s.cfg.MySQL.HeartbeatPeriod,
		ReadTimeout:     s.cfg.MySQL.ReadTimeout,
		// 解析时间类型为 time.Time，序列化更友好
		ParseTime: true,
		Logger:    s.log,
	})
	s.syncer = bs

	// 5. 决定起点：位点 → file+pos；否则从最新开始
	startPos := mysql.Position{}
	if s.cdc != nil && s.cdc.HasFilePosition() {
		startPos = mysql.Position{Name: s.cdc.BinlogFile, Pos: uint32(s.cdc.BinlogPos)}
	} else {
		latest, err := s.masterStatus(ctx)
		if err != nil {
			return err
		}
		startPos = latest
	}

	streamer, err := bs.StartSync(startPos)
	if err != nil {
		return fmt.Errorf("启动 binlog 同步失败: %w", err)
	}
	s.streamer = streamer
	s.log.Info("binlog 同步已启动",
		"file", startPos.Name, "pos", startPos.Pos,
		"server_id", s.cfg.MySQL.ServerID,
		"producer", s.producer.Name())
	return nil
}

// masterStatus 读当前主库位点（8.4 起改名为 SHOW BINARY LOG STATUS）
func (s *Syncer) masterStatus(ctx context.Context) (mysql.Position, error) {
	var file string
	var pos uint32

	row := s.store_.QueryRowContext(ctx, "SHOW MASTER STATUS")
	var (
		doDB, ignoreDB, gtidExecuted sql.NullString
	)
	if err := row.Scan(&file, &pos, &doDB, &ignoreDB, &gtidExecuted); err != nil {
		// MySQL 8.4+
		if err2 := s.store_.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").
			Scan(&file, &pos, &doDB, &ignoreDB, &gtidExecuted); err2 != nil {
			return mysql.Position{}, fmt.Errorf(
				"读取 binlog 位点失败（需要 REPLICATION CLIENT 权限）: %w", err)
		}
	}
	return mysql.Position{Name: file, Pos: pos}, nil
}

// handleEvent 分派单个 binlog 事件
func (s *Syncer) handleEvent(ctx context.Context, be *replication.BinlogEvent) error {
	switch e := be.Event.(type) {
	case *replication.RotateEvent:
		if s.cdc != nil {
			s.cdc.BinlogFile = string(e.NextLogName)
			s.cdc.BinlogPos = int64(e.Position)
		}

	case *replication.GTIDEvent:
		// 记录当前事务 GTID，供后续 RowsEvent 使用
		gtid, err := s.formatGTID(e)
		if err == nil {
			s.parser.SetGTID(gtid)
			if s.gtidSet != nil {
				_ = s.gtidSet.AddGtid(gtid)
			}
		}

	case *replication.RowsEvent:
		// 首次遇到某表时查一次主键，之后走缓存。
		// 不注入的话幂等键会退化成行哈希，下游无法精确定位。
		if err := s.ensureTableKeys(ctx, e); err != nil {
			return err
		}
		changes := s.parser.ParseRows(e, be.Header.EventType,
			s.currentFile(), int64(be.Header.LogPos), int64(be.Header.Timestamp), be.Header.ServerID)
		for _, c := range changes {
			if err := s.append(ctx, c); err != nil {
				return err
			}
		}

	case *replication.QueryEvent:
		ddl := s.parser.ParseDDL(e, s.currentFile(), int64(be.Header.LogPos),
			int64(be.Header.Timestamp), be.Header.ServerID)
		if ddl != nil {
			s.log.Info("DDL", "op", ddl.Op, "db", ddl.Database,
				"tables", ddl.Tables, "sql", truncate(ddl.Query, 120))
			if err := s.append(ctx, ddl); err != nil {
				return err
			}
			// 表结构可能已变，清缓存
			s.schema.InvalidateAll()
			s.parser.ResetTableCache()
		}

	case *replication.XIDEvent:
		// 事务结束：把缓冲刷出去
		if err := s.flush(ctx); err != nil {
			return err
		}
		// 位点按配置的节奏落盘：每 N 个事件或每 N 秒，
		// 避免高并发小事务下单事务一次 IO
		s.eventsSincePersist++
		if s.shouldPersist() {
			if err := s.persist(ctx); err != nil {
				return err
			}
		}
	}

	// 记录最新位置（每个事件都更新，但只在 flush/事务结束时落盘）
	if s.cdc == nil {
		s.cdc = &checkpoint.Position{}
	}
	if be.Header.LogPos > 0 {
		s.cdc.BinlogPos = int64(be.Header.LogPos)
	}
	return nil
}

// ensureTableKeys 保证某张表的主键列已注入 parser。
//
// 只在首次遇到该表（或 DDL 之后）时查一次 information_schema，
// 之后 parser 内部按 tableID 缓存。
func (s *Syncer) ensureTableKeys(ctx context.Context, e *replication.RowsEvent) error {
	if e == nil || e.Table == nil {
		return nil
	}

	database := string(e.Table.Schema)
	table := string(e.Table.Table)
	if database == "" || table == "" {
		return nil
	}

	keys, err := s.schema.Load(ctx, database, table)
	if err != nil {
		// 查不到主键不应中断同步：退化为行哈希，但要告警
		s.log.Warn("查询主键失败，该表将使用行哈希作幂等键",
			"table", database+"."+table, "err", err)
		if s.alerter != nil {
			s.alerter.Alert(
				"missing-key",
				"存在无主键表",
				"幂等键会退化为行内容哈希，下游按主键去重会失效。",
				map[string]string{
					"表":  database + "." + table,
					"错误": err.Error(),
				},
			)
		}
		return nil
	}
	s.parser.SetTableKeys(database, table, keys)
	return nil
}

func (s *Syncer) currentFile() string {
	if s.cdc == nil {
		return ""
	}
	return s.cdc.BinlogFile
}

// append 把记录加入缓冲，攒够批量后提交
func (s *Syncer) append(ctx context.Context, rec event.Record) error {
	env, err := event.NewEnvelope(rec)
	if err != nil {
		return err
	}

	// 跳过被过滤的行（DDL 已在 ParseDDL 里判断过 emit_ddl）
	if rec.Kind() != event.KindDDL {
		meta := rec.Topic()
		if !s.parser.filter.Allow(meta.Database, meta.Table) {
			return nil
		}
	}

	s.buf = append(s.buf, env)
	s.bufRows++

	if s.bufRows >= s.cfg.Runtime.BatchSize {
		return s.flush(ctx)
	}
	return nil
}

// flush 把缓冲交给 producer
// flush 把缓冲交给 producer，失败时带指数退避重试。
//
// 为什么不能直接返回错误让进程退出：
// 下游（Redis/Kafka）临时不可用是常态，退出会导致缓冲区里的数据丢失，
// 且位点已推进到这批之前——下次启动虽然能重读，但当前进程已丢数据。
//
// 重试耗尽后仍然失败才退出，此时位点未推进，重启会重新读取。
func (s *Syncer) flush(ctx context.Context) error {
	if len(s.buf) == 0 {
		return nil
	}

	batch := s.buf
	s.buf = nil
	s.bufRows = 0
	s.lastFlush = time.Now()

	const maxAttempts = 5
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 用独立的超时，避免 ctx 已取消时无法完成最后一次尝试
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		err := s.producer.Send(sendCtx, batch)
		cancel()

		if err == nil {
			s.eventsCnt.Add(int64(len(batch)))
			s.log.Debug("已投递", "count", len(batch), "total", s.eventsCnt.Load())
			return nil
		}

		lastErr = err

		if attempt == maxAttempts {
			break
		}

		// 指数退避：1s, 2s, 4s, 8s
		backoff := time.Duration(1<<(attempt-1)) * time.Second
		s.log.Warn("投递失败，稍后重试",
			"attempt", attempt, "max", maxAttempts,
			"count", len(batch), "backoff", backoff, "err", err)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			// 收到停止信号：把数据放回缓冲，让 defer 里的最后一次 flush 处理
			s.buf = append(batch, s.buf...)
			s.bufRows = len(s.buf)
			return fmt.Errorf("收到停止信号时投递未完成: %w", lastErr)
		}
	}

	// 重试耗尽：放回缓冲（供日志/排障），告警后返回错误让上层退出
	s.buf = append(batch, s.buf...)
	s.bufRows = len(s.buf)

	if s.alerter != nil {
		s.alerter.Alert(
			"delivery",
			"binlog2redis 投递持续失败",
			fmt.Sprintf("重试 %d 次后仍失败，进程即将退出。位点未推进，重启后会重新读取。", maxAttempts),
			map[string]string{
				"下游":   s.producer.Name(),
				"本批条数": fmt.Sprintf("%d", len(batch)),
				"错误":   lastErr.Error(),
				"主机":   s.owner(),
			},
		)
	}

	return fmt.Errorf("投递失败，重试 %d 次后放弃（%d 条）: %w", maxAttempts, len(batch), lastErr)
}

// shouldPersist 判断是否该把位点落盘。
//
// 两个条件先到先触发：
//   - 累计事件数达到 every_events
//   - 距上次落盘超过 every_seconds
//
// 都未配置时每 100 事件或 1 秒落一次（默认值）。
func (s *Syncer) shouldPersist() bool {
	everyEvents := s.cfg.Checkpoint.EveryEvents
	if everyEvents <= 0 {
		everyEvents = 100
	}
	everySeconds := s.cfg.Checkpoint.EverySeconds
	if everySeconds <= 0 {
		everySeconds = time.Second
	}

	return s.eventsSincePersist >= everyEvents ||
		time.Since(s.lastPersistAt) >= everySeconds
}

// persist 保存位点
func (s *Syncer) persist(ctx context.Context) error {
	if s.cdc == nil {
		return nil
	}

	pos := *s.cdc
	pos.Events = s.eventsCnt.Load()
	pos.UpdatedAt = time.Now().Unix()
	pos.Owner = s.owner()

	if s.gtidSet != nil {
		pos.Gtid = s.gtidSet.String()
	} else if s.parser.CurrentGTID() != "" {
		pos.Gtid = s.parser.CurrentGTID()
	}

	if err := s.store.Save(ctx, &pos); err != nil {
		return fmt.Errorf("保存位点失败: %w", err)
	}

	s.eventsSincePersist = 0
	s.lastPersistAt = time.Now()
	return nil
}

func (s *Syncer) owner() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// formatGTID 把 go-mysql 的 GTIDEvent 拼成标准形式 uuid:gno
func (s *Syncer) formatGTID(e *replication.GTIDEvent) (string, error) {
	if len(e.SID) != 16 {
		return "", fmt.Errorf("非法 SID 长度: %d", len(e.SID))
	}
	u, err := uuid.FromBytes(e.SID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", u.String(), e.GNO), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
