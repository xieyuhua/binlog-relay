package producer

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/biaoge/binlog2redis/internal/event"
)

// Stdout 把每批事件以 JSON Lines 输出，便于管道给其它程序：
//
//	binlog2redis start | jq -c 'select(.table=="orders")'
type Stdout struct {
	mu  sync.Mutex
	out *bufio.Writer
}

func NewStdout() (*Stdout, error) {
	return &Stdout{out: bufio.NewWriterSize(os.Stdout, 64*1024)}, nil
}

func (s *Stdout) Name() string { return "stdout" }

func (s *Stdout) Send(_ context.Context, envs []*event.Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, env := range envs {
		line, err := json.Marshal(env)
		if err != nil {
			return err
		}
		if _, err := s.out.Write(line); err != nil {
			return err
		}
		if err := s.out.WriteByte('\n'); err != nil {
			return err
		}
	}
	return s.out.Flush()
}

func (s *Stdout) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.out.Flush()
}
