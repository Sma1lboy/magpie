package gateway

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/proc"
)

func idleTestBridge() *subscriptionBridge {
	return &subscriptionBridge{runs: map[string]*subscriptionRun{}, calls: map[string]*subscriptionRun{}, idle: map[string]*subscriptionRun{}, shelf: map[string]*savedSession{}}
}

// Completion and rewind both keep a run under a conversation key. Re-keying
// must move that run, not leave an alias that its next eviction cannot remove.
func TestClaudeIdleRunMoves(t *testing.T) {
	b := idleTestBridge()
	r := &subscriptionRun{bridge: b}
	b.keepIdle(r, "before", "conversation")
	b.keepIdle(r, "after", "conversation")
	if len(b.idle) != 1 || b.idle["after"] != r {
		t.Fatalf("one run retained under %d conversation keys; old key still present: %t", len(b.idle), b.idle["before"] != nil)
	}
	b.removeRun(r)
	if len(b.idle) != 0 {
		t.Fatal("ended run still available for another request")
	}
}

// A displaced run may finish or be kept again after another run took its
// old key. Moving it must not remove the replacement's conversation.
func TestClaudeIdleReplacementKeepsItsKey(t *testing.T) {
	b := idleTestBridge()
	old := &subscriptionRun{bridge: b}
	replacement := &subscriptionRun{bridge: b}
	b.keepIdle(old, "same", "")
	b.keepIdle(replacement, "same", "")
	b.keepIdle(old, "other", "")
	if b.idle["same"] != replacement || b.idle["other"] != old {
		t.Fatal("moving a displaced run removed its replacement")
	}
}

// The production sample spun in keepIdle -> shelve -> maps.Clone while the
// pending drop slice grew. Exercise a stale index in a separate process: the
// broken loop must not hang the suite or exhaust the machine's memory.
func TestClaudeIdleEvictionMakesProgress(t *testing.T) {
	const child = "MAGPIE_TEST_IDLE_EVICTION"
	if scenario := os.Getenv(child); scenario != "" {
		b := idleTestBridge()
		stale := &subscriptionRun{bridge: b, idleAt: time.Unix(1, 0), tools: map[string]bool{"read": true}}
		if scenario == "stale" {
			b.idle["before"] = stale
		} else {
			b.keepIdle(stale, "before", "")
			b.keepIdle(stale, "after", "")
		}
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		done := make(chan struct{})
		go func() {
			for i := 0; i < idleMost; i++ {
				b.keepIdle(&subscriptionRun{bridge: b}, fmt.Sprint(i), "")
			}
			close(done)
		}()
		// On failure the child exits, taking the stuck worker with it. A
		// post-GC live-heap check distinguishes retained drop entries from
		// the temporary maps shelve clones on every pass through the loop.
		tick := time.NewTicker(25 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				if len(b.idle) != idleMost || b.idle["before"] != nil || b.idle["after"] != nil || !stale.closed {
					t.Fatal("oldest stale entry was not removed and ended")
				}
				return
			case <-tick.C:
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > before.HeapAlloc+(8<<20) {
					t.Fatalf("idle eviction retained %d bytes above baseline while failing to finish (budget 8 MiB)", after.HeapAlloc-before.HeapAlloc)
				}
			}
		}
	}
	for _, scenario := range []string{"rekey", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := proc.CommandContext(ctx, os.Args[0], "-test.run=^TestClaudeIdleEvictionMakesProgress$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), child+"="+scenario, "GOMEMLIMIT=64MiB")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("idle eviction did not finish: %v\n%s", err, out)
			}
		})
	}
}

// Exercise the retained heap as well as the index: every run owns a stderr
// buffer as real Claude processes do, and the bridge owns it until abort
// removes it. Thousands of evictions must not retain those old buffers.
func TestClaudeIdleRetainedMemory(t *testing.T) {
	const child = "MAGPIE_TEST_IDLE_MEMORY"
	if os.Getenv(child) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := proc.CommandContext(ctx, os.Args[0], "-test.run=^TestClaudeIdleRetainedMemory$", "-test.v", "-test.timeout=45s")
		cmd.Env = append(os.Environ(), child+"=1", "GOMEMLIMIT=64MiB")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("idle retained-memory check: %v\n%s", err, out)
		}
		t.Logf("%s", out)
		return
	}
	b := idleTestBridge()
	payload := strings.Repeat("x", 64<<10)
	churn := func(start, count int) {
		for i := start; i < start+count; i++ {
			key := fmt.Sprint(i)
			r := &subscriptionRun{bridge: b, token: key}
			r.stderr.WriteString(payload)
			b.runs[key] = r
			b.keepIdle(r, key, "")
		}
	}
	churn(0, 64) // warm maps and the allocator before taking the baseline
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for batch := 0; batch < 4; batch++ {
		churn(64+batch*1024, 1024)
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("%d evictions: post-GC heap growth %d bytes; allocated %d bytes", (batch+1)*1024, retained, after.TotalAlloc-before.TotalAlloc)
		if retained > 4<<20 {
			t.Fatalf("idle runs retained %d bytes; budget 4 MiB", retained)
		}
		if len(b.runs) != idleMost || len(b.idle) != idleMost {
			t.Fatalf("kept %d runs and %d idle entries", len(b.runs), len(b.idle))
		}
	}
	runtime.KeepAlive(b)
}
