package policy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	asink "dirpx.dev/dlog/apis/sink"
	spolicy "dirpx.dev/dlog/apis/sink/policy"
)

// flakySink is a simple sink used to simulate failures/successes.
type flakySink struct {
	name string

	mu        sync.Mutex
	attempts  int
	failUntil int // number of initial attempts that should fail
	flushes   int
	closes    int
}

func (f *flakySink) Name() string { return f.name }

func (f *flakySink) Write(ctx context.Context, entry []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failUntil {
		return errors.New("boom")
	}
	return nil
}

func (f *flakySink) Flush(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	return nil
}

func (f *flakySink) Close(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *flakySink) snapshot() (attempts, flushes, closes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts, f.flushes, f.closes
}

func TestRetrySink_Name_DefaultAndOverride(t *testing.T) {
	inner := &flakySink{name: "inner"}
	opt := RetryOptions{
		Policy: spolicy.Retry{
			Enable:     true,
			MaxRetries: 3,
		},
	}

	s1 := WithRetry(inner, opt).(asink.Sink)
	if got, want := s1.Name(), "retry(inner)"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}

	opt2 := RetryOptions{
		Policy: opt.Policy,
		Name:   "myretry",
	}
	s2 := WithRetry(inner, opt2).(asink.Sink)
	if got, want := s2.Name(), "myretry"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
}

func TestRetrySink_Disabled_NoRetries(t *testing.T) {
	inner := &flakySink{name: "inner", failUntil: 100} // always fail
	s := WithRetry(inner, RetryOptions{
		Policy: spolicy.Retry{
			Enable:     false,
			MaxRetries: 5,
			Initial:    10 * time.Millisecond,
		},
	}).(asink.Sink)

	err := s.Write(context.Background(), []byte("x"))
	if err == nil {
		t.Fatalf("Write with retries disabled returned nil error, want non-nil")
	}

	attempts, _, _ := inner.snapshot()
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retries)", attempts)
	}
}

func TestRetrySink_SuccessWithoutRetry(t *testing.T) {
	inner := &flakySink{name: "inner", failUntil: 0} // succeeds immediately
	s := WithRetry(inner, RetryOptions{
		Policy: spolicy.Retry{
			Enable:     true,
			MaxRetries: 3,
		},
	}).(asink.Sink)

	err := s.Write(context.Background(), []byte("ok"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	attempts, _, _ := inner.snapshot()
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retries needed)", attempts)
	}
}

func TestRetrySink_RetriesUntilSuccess(t *testing.T) {
	inner := &flakySink{name: "inner", failUntil: 2} // first 2 attempts fail
	s := WithRetry(inner, RetryOptions{
		Policy: spolicy.Retry{
			Enable:     true,
			MaxRetries: 5,
			Initial:    0,
		},
	}).(asink.Sink)

	err := s.Write(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	attempts, _, _ := inner.snapshot()
	// failUntil=2: attempt1 fail, attempt2 fail, attempt3 succeed → 3 attempts.
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (2 failures + 1 success)", attempts)
	}
}

func TestRetrySink_StopsAfterMaxRetries(t *testing.T) {
	inner := &flakySink{name: "inner", failUntil: 100} // always fail
	s := WithRetry(inner, RetryOptions{
		Policy: spolicy.Retry{
			Enable:     true,
			MaxRetries: 2,
			Initial:    0,
		},
	}).(asink.Sink)

	err := s.Write(context.Background(), []byte("x"))
	if err == nil {
		t.Fatalf("Write should have failed after max retries")
	}

	attempts, _, _ := inner.snapshot()
	// First attempt + 2 retries = 3 attempts total.
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (1 + MaxRetries)", attempts)
	}
}

func TestRetrySink_RespectsPreCancelledContext(t *testing.T) {
	inner := &flakySink{name: "inner", failUntil: 0}
	s := WithRetry(inner, RetryOptions{
		Policy: spolicy.Retry{
			Enable:     true,
			MaxRetries: 5,
		},
	}).(asink.Sink)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling Write

	err := s.Write(ctx, []byte("x"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Write err = %v, want context.Canceled", err)
	}

	attempts, _, _ := inner.snapshot()
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 when context is pre-cancelled", attempts)
	}
}

func TestRetryNextDelay_BackoffSequence(t *testing.T) {
	// 10 → 20 → 40, capped by Max=50 → next would be 80 → 50.
	current := 10 * time.Millisecond
	mult := 2.0
	mx := 50 * time.Millisecond

	d1 := nextDelay(current, mult, mx) // 20
	d2 := nextDelay(d1, mult, mx)      // 40
	d3 := nextDelay(d2, mult, mx)      // 50 (capped)

	want := []time.Duration{
		20 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}
	got := []time.Duration{d1, d2, d3}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delay[%d] = %v, want %v", i, got[i], want[i])
		}
	}

	// Edge case: zero delay → always zero.
	if d := nextDelay(0, mult, mx); d != 0 {
		t.Fatalf("nextDelay(0, ...) = %v, want 0", d)
	}
}

func TestNormalizeRetryPolicy_SetsDefaultsAndClamps(t *testing.T) {
	p := spolicy.Retry{
		Enable:     true,
		MaxRetries: -1,
		Initial:    -5 * time.Second,
		Max:        -1 * time.Second,
		Multiplier: 0,
	}

	n := normalizeRetryPolicy(p)

	if n.MaxRetries != 0 {
		t.Fatalf("MaxRetries = %d, want 0", n.MaxRetries)
	}
	if n.Initial != 0 {
		t.Fatalf("Initial = %v, want 0", n.Initial)
	}
	if n.Max != 0 {
		t.Fatalf("Max = %v, want 0", n.Max)
	}
	if n.Multiplier != 2.0 {
		t.Fatalf("Multiplier = %v, want 2.0", n.Multiplier)
	}
}

func TestRetrySink_FlushAndClose_Delegated(t *testing.T) {
	inner := &flakySink{name: "inner"}
	s := WithRetry(inner, RetryOptions{
		Policy: spolicy.Retry{
			Enable:     true,
			MaxRetries: 2,
		},
	}).(asink.Sink)

	ctx := context.Background()

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, flushes, closes := inner.snapshot()
	if flushes != 1 {
		t.Fatalf("inner flushes = %d, want 1", flushes)
	}
	if closes != 1 {
		t.Fatalf("inner closes = %d, want 1", closes)
	}
}
