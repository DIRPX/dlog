/*
   Copyright 2025 The DIRPX Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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

// fakeSink is a simple in-memory sink used for testing batchSink.
type fakeSink struct {
	name string

	mu      sync.Mutex
	writes  [][]byte
	flushes int
	closes  int
}

func (f *fakeSink) Name() string { return f.name }

func (f *fakeSink) Write(ctx context.Context, entry []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	buf := make([]byte, len(entry))
	copy(buf, entry)
	f.writes = append(f.writes, buf)
	return nil
}

func (f *fakeSink) Flush(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	return nil
}

func (f *fakeSink) Close(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *fakeSink) snapshot() (writes [][]byte, flushes, closes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.writes))
	for i, e := range f.writes {
		cp := make([]byte, len(e))
		copy(cp, e)
		out[i] = cp
	}
	return out, f.flushes, f.closes
}

func TestBatchSink_Name_DefaultAndOverride(t *testing.T) {
	inner := &fakeSink{name: "inner"}

	s1 := WithBatch(inner, BatchOptions{}).(asink.Sink)
	if got, want := s1.Name(), "batch(inner)"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}

	s2 := WithBatch(inner, BatchOptions{Name: "mybatch"}).(asink.Sink)
	if got, want := s2.Name(), "mybatch"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
}

func TestBatchSink_BasicWriteAndClose_DrainsQueue(t *testing.T) {
	inner := &fakeSink{name: "inner"}
	s := WithBatch(inner, BatchOptions{
		QueueSize: 8,
		Batch:     spolicy.Batch{}, // no auto-flush, only on Close
	}).(asink.Sink)

	ctx := context.Background()

	// Enqueue several entries.
	for i := 0; i < 5; i++ {
		if err := s.Write(ctx, []byte{byte('a' + i)}); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}

	// Close should drain queue.
	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	writes, flushes, closes := inner.snapshot()
	if got, want := len(writes), 5; got != want {
		t.Fatalf("inner writes = %d, want %d", got, want)
	}
	if closes != 1 {
		t.Fatalf("inner closes = %d, want 1", closes)
	}
	if flushes == 0 {
		t.Fatalf("inner flushes = %d, want > 0", flushes)
	}
}

func TestBatchSink_RespectsMaxEntries(t *testing.T) {
	inner := &fakeSink{name: "inner"}
	s := WithBatch(inner, BatchOptions{
		QueueSize: 8,
		Batch: spolicy.Batch{
			MaxEntries: 2,
			Interval:   0,
		},
	}).(asink.Sink)

	ctx := context.Background()

	// Write 3 entries; with MaxEntries=2 мы ожидаем хотя бы один flush.
	for i := 0; i < 3; i++ {
		if err := s.Write(ctx, []byte{byte('x' + i)}); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	writes, _, _ := inner.snapshot()
	if len(writes) < 2 {
		t.Fatalf("inner writes = %d, want at least 2 (due to MaxEntries=2)", len(writes))
	}
}

func TestBatchSink_Flush_DoesNotDrainQueue(t *testing.T) {
	inner := &fakeSink{name: "inner"}
	s := WithBatch(inner, BatchOptions{
		QueueSize: 4,
	}).(asink.Sink)

	ctx := context.Background()

	// Write but do not close; flush should call inner.Flush but not necessarily
	// force the worker to drain the queue immediately (best-effort semantics).
	if err := s.Write(ctx, []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Give worker some time to run; we don't assert strict ordering here.
	time.Sleep(50 * time.Millisecond)

	_, flushes, _ := inner.snapshot()
	if flushes == 0 {
		t.Fatalf("inner flushes = %d, want > 0", flushes)
	}
}

func TestBatchSink_OverflowDrop_ReturnsErrQueueFull(t *testing.T) {
	inner := &fakeSink{name: "inner"}

	// We construct batchSink manually without starting the worker, so the queue
	// never drains and we can deterministically test overflow behavior.
	s := &batchSink{
		next: inner,
		opt: BatchOptions{
			QueueSize:    1,
			Backpressure: spolicy.BackpressureDrop,
		},
		queue: make(chan []byte, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}

	ctx := context.Background()

	// First write should succeed (queue goes from 0 to 1).
	if err := s.Write(ctx, []byte("a")); err != nil {
		t.Fatalf("Write(1): %v", err)
	}
	// Second write should see queue full and return ErrQueueFull.
	if err := s.Write(ctx, []byte("b")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Write(2) err = %v, want ErrQueueFull", err)
	}
}

func TestBatchSink_OverflowBlock_RespectsContext(t *testing.T) {
	inner := &fakeSink{name: "inner"}

	s := &batchSink{
		next: inner,
		opt: BatchOptions{
			QueueSize:    1,
			Backpressure: spolicy.BackpressureBlock,
		},
		queue: make(chan []byte, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}

	ctx := context.Background()

	// Fill the queue.
	if err := s.Write(ctx, []byte("a")); err != nil {
		t.Fatalf("Write(1): %v", err)
	}

	// Second write should block until ctx is cancelled.
	blockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.Write(blockCtx, []byte("b"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Write(2) err = %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatalf("Write(2) returned too quickly; blocking behavior may be broken")
	}
}

func TestBatchSink_WriteAndClose_AfterClose(t *testing.T) {
	inner := &fakeSink{name: "inner"}
	s := WithBatch(inner, BatchOptions{
		QueueSize: 4,
	}).(asink.Sink)

	ctx := context.Background()

	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := s.Write(ctx, []byte("x")); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("Write after Close err = %v, want ErrBatchClosed", err)
	}
	if err := s.Flush(ctx); !errors.Is(err, ErrBatchClosed) {
		t.Fatalf("Flush after Close err = %v, want ErrBatchClosed", err)
	}
}
