package producer

import (
	"encoding/json"
	"time"
)

func jsonMarshal(v any) ([]byte, error)   { return json.Marshal(v) }
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// timeSecond 抽出来便于测试时替换
func timeSecond() time.Duration { return time.Second }
