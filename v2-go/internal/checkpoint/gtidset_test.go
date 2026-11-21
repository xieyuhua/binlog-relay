package checkpoint

import "testing"

const (
	sidA = "c8fbcbb8-a012-11ef-85be-525400bcd3ec"
	sidB = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)

func TestGtidSetMerge(t *testing.T) {
	g := NewGtidSet()
	for _, n := range []int64{1, 2, 3, 5, 6, 10} {
		if err := g.AddGtid(sidA + ":" + itoa(n)); err != nil {
			t.Fatal(err)
		}
	}
	want := sidA + ":1-3:5-6:10"
	if got := g.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if g.Count() != 6 {
		t.Fatalf("count=%d want 6", g.Count())
	}
}

func TestGtidSetOutOfOrder(t *testing.T) {
	g := NewGtidSet()
	for _, n := range []int64{5, 1, 3, 2} {
		_ = g.AddGtid(sidA + ":" + itoa(n))
	}
	if got, want := g.String(), sidA+":1-3:5"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGtidSetParseCrossMerge(t *testing.T) {
	g, err := ParseGtidSet(sidA + ":1-5:10," + sidA + ":6-9")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := g.String(), sidA+":1-10"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGtidSetAdjacent(t *testing.T) {
	g, _ := ParseGtidSet(sidA + ":1-5")
	_ = g.AddGtid(sidA + ":6")
	if got, want := g.String(), sidA+":1-6"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGtidSetMultiSID(t *testing.T) {
	g := NewGtidSet()
	_ = g.AddGtid(sidA + ":1")
	_ = g.AddGtid(sidB + ":3")
	// 输出按 SID 字典序：aaa... 在前
	want := sidB + ":3," + sidA + ":1"
	if got := g.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGtidSetEmpty(t *testing.T) {
	g, err := ParseGtidSet("")
	if err != nil {
		t.Fatal(err)
	}
	if !g.IsEmpty() || g.String() != "" {
		t.Fatal("空集合处理错误")
	}
}

func TestGtidSetInvalid(t *testing.T) {
	if _, err := ParseGtidSet("not-a-gtid"); err == nil {
		t.Fatal("应拒绝非法输入")
	}
	if _, err := ParseGtidSet("zzz:1-2"); err == nil {
		t.Fatal("应拒绝非法 SID")
	}
}

func TestGtidSetRoundTrip(t *testing.T) {
	orig := sidB + ":1-100," + sidA + ":1-3:8:10-12" // 输出按 SID 字典序
	g, err := ParseGtidSet(orig)
	if err != nil {
		t.Fatal(err)
	}
	if got := g.String(); got != orig {
		t.Fatalf("round-trip 不一致:\n got=%q\nwant=%q", got, orig)
	}
}

func TestGtidSetMergeSets(t *testing.T) {
	a, _ := ParseGtidSet(sidA + ":1-5")
	b, _ := ParseGtidSet(sidA + ":4-10," + sidB + ":1")
	a.Merge(b)
	want := sidB + ":1," + sidA + ":1-10"
	if got := a.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGtidSetDedup(t *testing.T) {
	g := NewGtidSet()
	_ = g.AddGtid(sidA + ":1")
	n1 := g.Count()
	_ = g.AddGtid(sidA + ":1") // 重复
	if g.Count() != n1 {
		t.Fatal("重复添加不应改变计数")
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
