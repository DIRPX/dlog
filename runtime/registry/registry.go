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

package registry

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Builder builds a concrete value T from a specification S.
// Implementations must be safe to call concurrently.
type Builder[T any, S any] func(ctx context.Context, spec S) (T, error)

// Entry holds a registered builder with its key and optional documentation.
type Entry[T any, S any] struct {
	Key     Key
	Builder Builder[T, S]
	Doc     string // optional human-readable description
}

// Options control registry behavior.
type Options struct {
	// Normalizer canonicalizes keys on Register/Lookup/Build.
	// For example: lowercase kind/name to ensure case-insensitive matching.
	// If nil, keys are used as-is.
	Normalizer func(Key) Key

	// AllowReplace, when true, permits replacing an existing builder for the
	// same key on Register(). Disabled by default for safety.
	AllowReplace bool
}

// Option modifies Options.
type Option func(*Options)

// WithNormalizer sets a custom key normalizer.
func WithNormalizer(fn func(Key) Key) Option { return func(o *Options) { o.Normalizer = fn } }

// WithCaseFoldLower enables lowercase normalization for kind and name.
func WithCaseFoldLower() Option {
	return WithNormalizer(func(k Key) Key {
		k.Kind = strings.ToLower(k.Kind)
		k.Name = strings.ToLower(k.Name)
		return k
	})
}

// WithAllowReplace allows re-registering an existing key.
// Use with caution; prefer the default (disallow).
func WithAllowReplace() Option { return func(o *Options) { o.AllowReplace = true } }

// RegOption modifies per-entry registration parameters.
type RegOption func(*regOpts)

type regOpts struct {
	doc string
}

// WithDoc attaches a human-readable note to the entry.
func WithDoc(doc string) RegOption { return func(o *regOpts) { o.doc = doc } }

// Registry is a tiny generic registry keyed by (Kind, Name).
// It is safe for concurrent use.
type Registry[T any, S any] struct {
	mu     sync.RWMutex
	data   map[Key]Entry[T, S]
	opt    Options
	sealed atomic.Bool // when true, further Register calls fail
}

// New creates an empty registry with the provided options.
func New[T any, S any](opts ...Option) *Registry[T, S] {
	var o Options
	for _, fn := range opts {
		fn(&o)
	}
	return &Registry[T, S]{
		data: make(map[Key]Entry[T, S]),
		opt:  o,
	}
}

var (
	// ErrDuplicate indicates an attempt to register a duplicate key.
	ErrDuplicate = errors.New("registry: duplicate registration")
	// ErrUnknown indicates a lookup or build for an unregistered key.
	ErrUnknown = errors.New("registry: unknown key")
	// ErrSealed indicates an attempt to register in a sealed registry.
	ErrSealed = errors.New("registry: sealed registry")
)

// normalize applies the configured normalizer (if any).
func (r *Registry[T, S]) normalize(k Key) Key {
	if r.opt.Normalizer != nil {
		return r.opt.Normalizer(k)
	}
	return k
}

// Sealed reports whether the registry is sealed (no further registrations allowed).
func (r *Registry[T, S]) Sealed() bool { return r.sealed.Load() }

// Seal prevents further registrations. It is idempotent and safe for concurrent use.
// Returns true if this call changed the state from unsealed to sealed.
func (r *Registry[T, S]) Seal() bool { return !r.sealed.Swap(true) }

// Register adds a builder for the given key. Returns an error if the key already
// exists (unless WithAllowReplace was set) or if the registry is sealed.
func (r *Registry[T, S]) Register(k Key, b Builder[T, S], ropts ...RegOption) error {
	if r.Sealed() {
		return ErrSealed
	}
	if k.IsZero() || b == nil {
		return errors.New("registry: invalid key or builder")
	}
	k = r.normalize(k)

	var o regOpts
	for _, fn := range ropts {
		fn(&o)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.data[k]; exists && !r.opt.AllowReplace {
		return ErrDuplicate
	}
	r.data[k] = Entry[T, S]{Key: k, Builder: b, Doc: o.doc}
	return nil
}

// MustRegister panics on registration error. Useful from init() blocks.
func MustRegister[T any, S any](r *Registry[T, S], k Key, b Builder[T, S], ropts ...RegOption) {
	if err := r.Register(k, b, ropts...); err != nil {
		panic(err)
	}
}

// Lookup returns the builder for key k, if present.
func (r *Registry[T, S]) Lookup(k Key) (Builder[T, S], bool) {
	k = r.normalize(k)
	r.mu.RLock()
	e, ok := r.data[k]
	r.mu.RUnlock()
	return e.Builder, ok
}

// Build locates the builder for key k and invokes it with spec.
// Returns ErrUnknown when no builder is registered for k.
func (r *Registry[T, S]) Build(ctx context.Context, k Key, spec S) (T, error) {
	k = r.normalize(k)
	r.mu.RLock()
	e, ok := r.data[k]
	r.mu.RUnlock()
	if !ok {
		var zero T
		return zero, ErrUnknown
	}
	return e.Builder(ctx, spec)
}

// Keys returns all registered keys in deterministic (lexicographic) order.
func (r *Registry[T, S]) Keys() []Key {
	r.mu.RLock()
	keys := make([]Key, 0, len(r.data))
	for k := range r.data {
		keys = append(keys, k)
	}
	r.mu.RUnlock()

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Kind == keys[j].Kind {
			return keys[i].Name < keys[j].Name
		}
		return keys[i].Kind < keys[j].Kind
	})
	return keys
}

// Entries returns a snapshot of all registered entries in deterministic order.
func (r *Registry[T, S]) Entries() []Entry[T, S] {
	r.mu.RLock()
	items := make([]Entry[T, S], 0, len(r.data))
	for _, e := range r.data {
		items = append(items, e)
	}
	r.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		ki, kj := items[i].Key, items[j].Key
		if ki.Kind == kj.Kind {
			return ki.Name < kj.Name
		}
		return ki.Kind < kj.Kind
	})
	return items
}
