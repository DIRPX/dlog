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
	"sync"
	"sync/atomic"

	asink "dirpx.dev/dlog/apis/sink"
)

// NewGroup constructs a fan-out group with a custom name.
// Nil sinks are ignored; duplicate names are skipped.
func NewGroup(name string, sinks ...asink.Sink) asink.Group {
	g := &group{
		name:   name,
		byName: make(map[string]asink.Sink, len(sinks)),
	}
	for _, s := range sinks {
		if s == nil {
			continue
		}
		n := s.Name()
		if n == "" {
			// anonymous sinks would break Remove/List; skip silently
			continue
		}
		if _, exists := g.byName[n]; exists {
			continue
		}
		g.byName[n] = s
	}
	return g
}

// group fans out records to multiple sinks concurrently and exposes
// group-level operations (name, add, flush, close).
//
// Concurrency & semantics:
//   - Writes go to a snapshot of the current sinks to avoid holding locks.
//   - On the first observed error during Write, a derived context is canceled to
//     let cooperative sinks stop early (best-effort).
//   - All errors from children are aggregated with errors.Join (Go 1.20+).
//   - Flush attempts to call Flush(ctx) on sinks that support it; others are skipped.
//   - Add is safe for concurrent use; adding after Close returns ErrGroupClosed.
//   - Close runs sinks' Close(ctx) in parallel, aggregates errors, and marks the group closed.
type group struct {
	// name is the group name.
	name string
	// mu protects sinks and closed.
	mu sync.RWMutex
	// byName maps sink names to sinks for quick lookup.
	byName map[string]asink.Sink
	// closed indicates whether the group is closed.
	closed atomic.Bool
}

// Compile-time safety: *group implements asink.Group.
var _ asink.Group = (*group)(nil)

var (
	// ErrGroupClosed indicates the group is closed.
	ErrGroupClosed = errors.New("sink/group: closed")
	// ErrNilSink indicates a nil sink was provided.
	ErrNilSink = errors.New("sink/group: nil sink")
	// ErrDuplicate indicates a duplicate sink name.
	ErrDuplicate = errors.New("sink/group: duplicate sink name")
	// ErrNotFound indicates the sink was not found.
	ErrNotFound = errors.New("sink/group: sink not found")
)

// Add adds a sink to the group.
// Returns ErrGroupClosed if the group is closed,
// ErrNilSink if the sink is nil,
// or ErrDuplicate if a sink with the same name already exists.
func (g *group) Add(s asink.Sink) error {
	if s == nil {
		return ErrNilSink
	}
	if g.closed.Load() {
		return ErrGroupClosed
	}
	n := s.Name()
	if n == "" {
		return ErrNilSink
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.byName == nil {
		g.byName = make(map[string]asink.Sink, 1)
	}
	if _, exists := g.byName[n]; exists {
		return ErrDuplicate
	}
	g.byName[n] = s

	return nil
}

// Remove deletes a sink by name. Returns ErrNotFound if missing.
func (g *group) Remove(name string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.byName[name]; !ok {
		return ErrNotFound
	}
	delete(g.byName, name)

	return nil
}

// List returns the names of all sinks in the group.
func (g *group) List() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	names := make([]string, 0, len(g.byName))
	for name := range g.byName {
		names = append(names, name)
	}
	return names
}

// Name returns the group name.
func (g *group) Name() string {
	return g.name
}

// Write delivers a single encoded entry to all sinks in parallel.
// Uses a snapshot of current sinks to avoid holding locks during I/O.
func (g *group) Write(ctx context.Context, entry []byte) error {
	sinks := g.snapshotSinks()
	if len(sinks) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(sinks))
	var wg sync.WaitGroup
	wg.Add(len(sinks))

	for _, s := range sinks {
		go func(s asink.Sink) {
			defer wg.Done()
			if err := s.Write(ctx, entry); err != nil {
				errs <- err
				// best-effort: cancel siblings on first error
				cancel()
			}
		}(s)
	}

	wg.Wait()
	close(errs)

	return joinErrors(errs)
}

// Flush flushes all sinks in parallel and joins errors.
func (g *group) Flush(ctx context.Context) error {
	sinks := g.snapshotSinks()
	if len(sinks) == 0 {
		return nil
	}

	errs := make(chan error, len(sinks))
	var wg sync.WaitGroup
	wg.Add(len(sinks))

	for _, s := range sinks {
		go func(s asink.Sink) {
			defer wg.Done()
			if err := s.Flush(ctx); err != nil {
				errs <- err
			}
		}(s)
	}

	wg.Wait()
	close(errs)

	return joinErrors(errs)
}

// Close closes all sinks in parallel, aggregates errors, and marks the group closed.
func (g *group) Close(ctx context.Context) error {
	// Mark closed (idempotent signal for Add()).
	g.closed.Store(true)

	sinks := g.snapshotSinks()
	if len(sinks) == 0 {
		return nil
	}

	errs := make(chan error, len(sinks))
	var wg sync.WaitGroup
	wg.Add(len(sinks))

	for _, s := range sinks {
		go func(s asink.Sink) {
			defer wg.Done()
			if err := s.Close(ctx); err != nil {
				errs <- err
			}
		}(s)
	}

	wg.Wait()
	close(errs)

	return joinErrors(errs)
}

// snapshotSinks returns a snapshot of current sinks.
func (g *group) snapshotSinks() []asink.Sink {
	g.mu.RLock()
	defer g.mu.RUnlock()
	sinks := make([]asink.Sink, 0, len(g.byName))
	for _, s := range g.byName {
		sinks = append(sinks, s)
	}
	return sinks
}

// joinErrors drains an error channel and joins all errors (Go 1.20+).
func joinErrors(errs <-chan error) error {
	var agg error
	for err := range errs {
		agg = errors.Join(agg, err)
	}
	return agg
}
