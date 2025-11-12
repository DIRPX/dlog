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

package sink

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	asink "dirpx.dev/dlog/apis/sink"
)

// ---- fakes ----

type fakeSink struct {
	name     string
	writeErr error
	flushErr error
	closeErr error
	blockCh  chan struct{} // if non-nil, Write blocks until closed

	mu      sync.Mutex
	writes  int
	flushes int
	closes  int
}

func (f *fakeSink) Name() string { return f.name }

func (f *fakeSink) Write(ctx context.Context, entry []byte) error {
	if f.blockCh != nil {
		select {
		case <-f.blockCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	return f.writeErr
}

func (f *fakeSink) Flush(ctx context.Context) error {
	f.mu.Lock()
	f.flushes++
	f.mu.Unlock()
	return f.flushErr
}

func (f *fakeSink) Close(ctx context.Context) error {
	f.mu.Lock()
	f.closes++
	f.mu.Unlock()
	return f.closeErr
}

func (f *fakeSink) counts() (w, fl, cl int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes, f.flushes, f.closes
}

// ---- helpers ----

func sorted(ss []string) []string {
	cp := append([]string(nil), ss...)
	sort.Strings(cp)
	return cp
}

// ---- tests ----

func TestNewGroup_FiltersNilEmptyDupesAndListsNames(t *testing.T) {
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b"}
	dupA := &fakeSink{name: "a"} // duplicate by name
	empty := &fakeSink{name: ""} // ignored

	g := NewGroup("G", nil, a, b, dupA, empty).(asink.Group)

	if got := g.Name(); got != "G" {
		t.Fatalf("Name() = %q, want %q", got, "G")
	}

	names := g.List()
	got := sorted(names)
	want := []string{"a", "b"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("List() = %v, want %v", got, want)
	}
}

func TestAdd_Remove_List(t *testing.T) {
	g := NewGroup("G").(asink.Group)

	// Add nil
	if err := g.Add(nil); !errors.Is(err, ErrNilSink) {
		t.Fatalf("Add(nil) err = %v, want ErrNilSink", err)
	}

	// Add ok
	a := &fakeSink{name: "a"}
	if err := g.Add(a); err != nil {
		t.Fatalf("Add(a) err = %v", err)
	}
	// Add duplicate
	if err := g.Add(&fakeSink{name: "a"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("Add(dup) err = %v, want ErrDuplicate", err)
	}

	// List (order is map-iteration; compare as sets)
	got := sorted(g.List())
	want := []string{"a"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("List() = %v, want %v", got, want)
	}

	// Remove missing
	if err := g.Remove("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Remove(nope) err = %v, want ErrNotFound", err)
	}

	// Remove existing
	if err := g.Remove("a"); err != nil {
		t.Fatalf("Remove(a) err = %v", err)
	}
	if len(g.List()) != 0 {
		t.Fatalf("List() after remove not empty: %v", g.List())
	}
}

func TestWrite_FansOutAndAggregatesErrors(t *testing.T) {
	errA := errors.New("writeA")
	errB := errors.New("writeB")

	a := &fakeSink{name: "a", writeErr: errA}
	b := &fakeSink{name: "b"} // ok
	c := &fakeSink{name: "c", writeErr: errB}

	g := NewGroup("G", a, b, c).(asink.Group)

	ctx := context.Background()
	entry := []byte(`{"x":1}`)

	err := g.Write(ctx, entry)
	if err == nil {
		t.Fatal("Write() err = nil, want joined error")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("joined error %v does not contain both errA and errB", err)
	}

	aw, _, _ := a.counts()
	bw, _, _ := b.counts()
	cw, _, _ := c.counts()
	if aw != 1 || bw != 1 || cw != 1 {
		t.Fatalf("writes a/b/c = %d/%d/%d, want 1/1/1", aw, bw, cw)
	}
}

func TestFlush_ParallelAndAggregatesErrors(t *testing.T) {
	errA := errors.New("flushA")
	a := &fakeSink{name: "a", flushErr: errA}
	b := &fakeSink{name: "b"}
	g := NewGroup("G", a, b).(asink.Group)

	err := g.Flush(context.Background())
	if err == nil || !errors.Is(err, errA) {
		t.Fatalf("Flush err = %v, want contains flushA", err)
	}

	_, af, _ := a.counts()
	_, bf, _ := b.counts()
	if af != 1 || bf != 1 {
		t.Fatalf("flushes a/b = %d/%d, want 1/1", af, bf)
	}
}

func TestClose_ParallelAndAggregatesErrors_MarksClosed(t *testing.T) {
	errC := errors.New("closeC")
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b", closeErr: errC}
	g := NewGroup("G", a, b).(asink.Group)

	err := g.Close(context.Background())
	if err == nil || !errors.Is(err, errC) {
		t.Fatalf("Close err = %v, want contains closeC", err)
	}

	_, _, ac := a.counts()
	_, _, bc := b.counts()
	if ac != 1 || bc != 1 {
		t.Fatalf("closes a/b = %d/%d, want 1/1", ac, bc)
	}

	// Add after Close should fail.
	if err := g.Add(&fakeSink{name: "x"}); !errors.Is(err, ErrGroupClosed) {
		t.Fatalf("Add after Close err = %v, want ErrGroupClosed", err)
	}
}

func TestEmptyGroup_NoOp(t *testing.T) {
	g := NewGroup("G").(asink.Group)
	ctx := context.Background()
	if err := g.Write(ctx, []byte("x")); err != nil {
		t.Fatalf("Write on empty group: %v", err)
	}
	if err := g.Flush(ctx); err != nil {
		t.Fatalf("Flush on empty group: %v", err)
	}
	if err := g.Close(ctx); err != nil {
		t.Fatalf("Close on empty group: %v", err)
	}
}

func TestWrite_UsesSnapshot_AllowsConcurrentAdd(t *testing.T) {
	// s1 blocks until we release it; during the block we Add(s2) and ensure
	// the in-flight Write call completes (s2 not required to run in that call).
	s1Block := make(chan struct{})
	s1 := &fakeSink{name: "s1", blockCh: s1Block}
	g := NewGroup("G", s1).(asink.Group)

	done := make(chan struct{})
	go func() {
		_ = g.Write(context.Background(), []byte("entry"))
		close(done)
	}()

	// Give goroutine time to start and block.
	time.Sleep(50 * time.Millisecond)

	// Add a new sink while Write is in progress.
	if err := g.Add(&fakeSink{name: "s2"}); err != nil {
		t.Fatalf("Add during Write err = %v", err)
	}

	// Unblock s1 so the write can finish.
	close(s1Block)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Write did not complete in time")
	}

	// Next Write should hit both sinks.
	if err := g.Write(context.Background(), []byte("x")); err != nil {
		t.Fatalf("second Write err = %v", err)
	}
}
