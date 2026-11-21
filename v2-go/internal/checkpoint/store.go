// Package checkpoint 提供位点持久化（Redis / 文件）与 GTID 集合。
package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Position 是消费位点
type Position struct {
	BinlogFile string `json:"binlog_file"`
	BinlogPos  int64  `json:"binlog_pos"`
	// Gtid 在 gtid_set 关闭时是单个事务，开启时是完整执行集合
	Gtid      string `json:"gtid,omitempty"`
	UpdatedAt int64  `json:"updated_at"`
	Events    int64  `json:"events"`
	Owner     string `json:"owner"`
}

// HasFilePosition 判断是否有可续传的文件位点
func (p *Position) HasFilePosition() bool { return p.BinlogFile != "" }

// Store 是位点存储接口
type Store interface {
	Load(ctx context.Context) (*Position, error)
	Save(ctx context.Context, p *Position) error
	// AcquireLock 独占锁，避免两个实例同时消费
	AcquireLock(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	RefreshLock(ctx context.Context, owner string, ttl time.Duration) error
	ReleaseLock(ctx context.Context, owner string)
	Name() string
	Close() error
}

// ---------- 文件实现 ----------

// FileStore 用原子写（临时文件 + rename）保存位点，锁用 O_EXCL 占位文件。
type FileStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("创建位点目录失败: %w", err)
	}
	return &FileStore{path: path}, nil
}

func (f *FileStore) Name() string { return "file:" + f.path }

func (f *FileStore) Load(_ context.Context) (*Position, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p Position
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("位点文件损坏: %w", err)
	}
	return &p, nil
}

func (f *FileStore) Save(_ context.Context, p *Position) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path) // 原子替换，读侧永远看到完整内容
}

// AcquireLock 用 O_EXCL 创建锁文件。
//
// 进程崩溃后锁文件会残留，所以这里会检查持有者是否还活着：
// 同机进程用 signal 0 探测，探测失败则视为陈旧锁并清理。
func (f *FileStore) AcquireLock(_ context.Context, owner string, _ time.Duration) (bool, error) {
	lockPath := f.path + ".lock"

	for attempt := 0; attempt < 2; attempt++ {
		file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			defer file.Close()
			_, _ = fmt.Fprintf(file, "%s\n%d\n%s", owner, os.Getpid(), time.Now().Format(time.RFC3339))
			return true, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return false, err
		}

		// 锁已存在：检查持有者是否还活着
		alive, pid := lockHolderAlive(lockPath)
		if alive {
			return false, nil
		}

		// 陈旧锁：删除后重试一次
		fmt.Fprintf(os.Stderr, "检测到陈旧锁（pid=%d 已不存在），清理后继续\n", pid)
		if err := os.Remove(lockPath); err != nil {
			return false, fmt.Errorf("清理陈旧锁失败: %w", err)
		}
	}

	return false, nil
}

// lockHolderAlive 判断锁文件里的进程是否仍存在。
// 跨主机场景无法探测，保守返回 true（需人工 `reset-position --force`）。
func lockHolderAlive(lockPath string) (bool, int) {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return false, 0
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return false, 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[1]))
	if err != nil || pid <= 0 {
		return false, 0
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, pid
	}
	// Unix 下 signal 0 只探测存在性；Windows 上 FindProcess 本身即可
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return false, pid
	}
	return true, pid
}

func (f *FileStore) RefreshLock(context.Context, string, time.Duration) error { return nil }

func (f *FileStore) ReleaseLock(_ context.Context, _ string) {
	_ = os.Remove(f.path + ".lock")
}

func (f *FileStore) Close() error { return nil }

// ---------- Redis 实现 ----------

// RedisStore 用 HSET 存位点，SET NX PX 做独占锁。
type RedisStore struct {
	client *redis.Client
	key    string
}

func NewRedisStore(client *redis.Client, key string) *RedisStore {
	return &RedisStore{client: client, key: key}
}

func (r *RedisStore) Name() string { return "redis:" + r.key }

func (r *RedisStore) Load(ctx context.Context) (*Position, error) {
	m, err := r.client.HGetAll(ctx, r.key).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}

	p := &Position{
		BinlogFile: m["binlog_file"],
		Gtid:       m["gtid"],
		Owner:      m["owner"],
	}
	p.BinlogPos, _ = parseInt64(m["binlog_pos"])
	p.UpdatedAt, _ = parseInt64(m["updated_at"])
	p.Events, _ = parseInt64(m["events"])

	if p.BinlogFile == "" && p.Gtid == "" && p.Events == 0 {
		return nil, nil
	}
	return p, nil
}

func (r *RedisStore) Save(ctx context.Context, p *Position) error {
	return r.client.HMSet(ctx, r.key, map[string]any{
		"binlog_file": p.BinlogFile,
		"binlog_pos":  p.BinlogPos,
		"gtid":        p.Gtid,
		"updated_at":  p.UpdatedAt,
		"events":      p.Events,
		"owner":       p.Owner,
	}).Err()
}

func (r *RedisStore) AcquireLock(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, r.key+":lock", owner, ttl).Result()
}

func (r *RedisStore) RefreshLock(ctx context.Context, owner string, ttl time.Duration) error {
	// 只续自己的锁，避免误续他人
	script := redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0`)
	return script.Run(ctx, r.client, []string{r.key + ":lock"}, owner, ttl.Milliseconds()).Err()
}

func (r *RedisStore) ReleaseLock(ctx context.Context, owner string) {
	script := redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0`)
	_ = script.Run(ctx, r.client, []string{r.key + ":lock"}, owner).Err()
}

func (r *RedisStore) Close() error { return r.client.Close() }

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
