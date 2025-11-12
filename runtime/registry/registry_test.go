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
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
)

// simple builder used in many tests
func mkBuilder[T ~string, S any](prefix T) Builder[T, S] {
	return func(ctx context.Context, spec S) (T, error) {
		return T(fmt.Sprintf("%s%v", prefix, spec)), nil
	}
}

func TestRegisterAndBuild_Success(t *testing.T) {
	r := New[string, int]()
	k := Key{Kind: "sink", Name: "stdout"}

	if err := r.Register(k, mkBuilder[string, int]("ok:")); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := r.Build(context.Background(), k, 42)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if want := "ok:42"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDuplicateRegistration_DefaultDisallows(t *testing.T) {
	r := New[string, int]()
	k := Key{Kind: "sink", Name: "stdout"}

	if err := r.Register(k, mkBuilder[string, int]("a:")); err != nil {
		t.Fatalf("Register(1): %v", err)
	}
	if err := r.Register(k, mkBuilder[string, int]("b:")); err == nil {
		t.Fatal("expected ErrDuplicate, got nil")
	} else if err != ErrDuplicate {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
}

func TestAllowReplace(t *testing.T) {
	r := New[string, int](WithAllowReplace())
	k := Key{Kind: "sink", Name: "stdout"}

	if err := r.Register(k, mkBuilder[string, int]("a:")); err != nil {
		t.Fatalf("Register(1): %v", err)
	}
	if err := r.Register(k, mkBuilder[string, int]("b:")); err != nil {
		t.Fatalf("Register(2): %v", err)
	}
	got, err := r.Build(context.Background(), k, 7)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if want := "b:7"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSeal_PreventsFurtherRegistration(t *testing.T) {
	r := New[string, int]()
	k := Key{Kind: "sink", Name: "stdout"}

	// register before sealing
	if err := r.Register(k, mkBuilder[string, int]("a:")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// seal and verify
	if !r.Seal() {
		t.Fatal("Seal() = false, want true on first call")
	}
	if !r.Sealed() {
		t.Fatal("Sealed() = false, want true")
	}
	// second seal is idempotent
	if r.Seal() {
		t.Fatal("Seal() = true on second call, want false")
	}

	// further registration should fail
	if err := r.Register(Key{Kind: "sink", Name: "file"}, mkBuilder[string, int]("x:")); err == nil {
		t.Fatal("expected ErrSealed, got nil")
	} else if err != ErrSealed {
		t.Fatalf("want ErrSealed, got %v", err)
	}

	// existing entry still builds
	got, err := r.Build(context.Background(), k, 1)
	if err != nil || got != "a:1" {
		t.Fatalf("Build after Seal got %q, %v", got, err)
	}
}

func TestLookupUnknownAndBuildUnknown(t *testing.T) {
	r := New[string, int]()
	k := Key{Kind: "sink", Name: "missing"}

	if b, ok := r.Lookup(k); ok || b != nil {
		t.Fatalf("Lookup unknown: got ok=%v, builder=%v; want ok=false,nil", ok, b)
	}
	got, err := r.Build(context.Background(), k, 0)
	if err == nil {
		t.Fatalf("Build should fail with ErrUnknown, got value %q", got)
	}
	if err != ErrUnknown {
		t.Fatalf("want ErrUnknown, got %v", err)
	}
	if got != "" { // zero value for string
		t.Fatalf("got %q, want zero value", got)
	}
}

func TestInvalidRegistration(t *testing.T) {
	r := New[string, int]()

	// empty key
	if err := r.Register(Key{}, mkBuilder[string, int]("x:")); err == nil {
		t.Fatal("expected error for zero key")
	}

	// nil builder
	var nilBuilder Builder[string, int]
	if err := r.Register(Key{Kind: "sink", Name: "x"}, nilBuilder); err == nil {
		t.Fatal("expected error for nil builder")
	}
}

func TestKeysAndEntries_DeterministicOrder(t *testing.T) {
	r := New[string, int]()
	// shuffled registration order
	items := []Key{
		{Kind: "stage", Name: "b"},
		{Kind: "sink", Name: "a"},
		{Kind: "sink", Name: "c"},
		{Kind: "provider", Name: "z"},
		{Kind: "stage", Name: "a"},
	}
	for _, k := range items {
		if err := r.Register(k, mkBuilder[string, int]("x:")); err != nil {
			t.Fatalf("Register(%s): %v", k.String(), err)
		}
	}

	// Keys should be sorted by Kind, then Name.
	keys := r.Keys()
	wantKeys := append([]Key(nil), keys...)
	sort.Slice(wantKeys, func(i, j int) bool {
		if wantKeys[i].Kind == wantKeys[j].Kind {
			return wantKeys[i].Name < wantKeys[j].Name
		}
		return wantKeys[i].Kind < wantKeys[j].Kind
	})
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("Keys() not sorted deterministically:\n got: %v\nwant: %v", keys, wantKeys)
	}

	// Entries() should mirror the same order by Key.
	entries := r.Entries()
	gotKeys := make([]Key, 0, len(entries))
	for _, e := range entries {
		gotKeys = append(gotKeys, e.Key)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("Entries() order mismatch:\n got: %v\nwant: %v", gotKeys, wantKeys)
	}
}

func TestNormalizer_CaseFoldLower(t *testing.T) {
	r := New[string, int](WithCaseFoldLower())

	upper := Key{Kind: "SiNk", Name: "StdOut"}
	lower := Key{Kind: "sink", Name: "stdout"}

	if err := r.Register(upper, mkBuilder[string, int]("y:")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Lookup/Build by lowercased key should succeed due to normalizer.
	if _, ok := r.Lookup(lower); !ok {
		t.Fatal("Lookup(lower) failed, expected ok=true")
	}
	got, err := r.Build(context.Background(), lower, 5)
	if err != nil || got != "y:5" {
		t.Fatalf("Build(lower) got %q, %v; want y:5, nil", got, err)
	}
}

func TestMustRegister_PanicsOnError(t *testing.T) {
	r := New[string, int]()
	k := Key{Kind: "sink", Name: "stdout"}

	MustRegister(r, k, mkBuilder[string, int]("a:"))

	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("expected panic from MustRegister on duplicate registration")
		}
	}()
	// duplicate should cause panic
	MustRegister(r, k, mkBuilder[string, int]("b:"))
}

func TestConcurrentBuild_Safe(t *testing.T) {
	r := New[string, int]()
	k := Key{Kind: "sink", Name: "stdout"}
	if err := r.Register(k, mkBuilder[string, int]("v:")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)

	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(val int) {
			defer wg.Done()
			got, err := r.Build(context.Background(), k, val)
			if err != nil {
				errs <- fmt.Errorf("build err: %w", err)
				return
			}
			want := fmt.Sprintf("v:%d", val)
			if got != want {
				errs <- fmt.Errorf("got %q, want %q", got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Build: %v", err)
	}
}
