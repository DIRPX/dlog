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

package provider

import (
	"dirpx.dev/dxlog/apis/field"
	"dirpx.dev/dxlog/apis/level"
	"dirpx.dev/dxlog/apis/pipeline"
)

// Specification is a declarative dxlog configuration fragment.
// It is intentionally small and merge-friendly.
type Specification struct {
	// MinLevel optionally overrides the minimum logging level.
	MinLevel *level.Level `json:"minLevel,omitempty" yaml:"minLevel,omitempty"`

	// Fields are static fields added to every record (appended on merge).
	Fields []field.Field `json:"fields,omitempty" yaml:"fields,omitempty"`

	// Pipeline optionally replaces the processing pipeline.
	Pipeline *pipeline.Specification `json:"pipeline,omitempty" yaml:"pipeline,omitempty"`

	// Sinks is an ordered list of sink names to write to.
	// Binding/validation happen in runtime against the sink registry.
	Sinks []string `json:"sinks,omitempty" yaml:"sinks,omitempty"`
}

// Validate performs shallow validation of the Specification.
// Runtime builders may enforce stricter rules.
func (s *Specification) Validate() error {
	if s == nil {
		return nil
	}
	if s.MinLevel != nil {
		if err := s.MinLevel.Validate(); err != nil {
			return err
		}
	}
	// Field-level validation (optional).
	type validator interface{ Validate() error }
	for _, f := range s.Fields {
		if v, ok := any(f).(validator); ok {
			if err := v.Validate(); err != nil {
				return err
			}
		}
	}
	// Pipeline/Sinks validation is deferred to runtime (registry-aware).
	return nil
}

// Merge applies override over base according to provider precedence.
// Slices are replaced, except Fields which are appended. Nils are skipped.
// The result is a new Specification (input objects are not mutated).
func Merge(base, override *Specification) *Specification {
	switch {
	case base == nil && override == nil:
		return &Specification{}
	case base == nil:
		return cloneSpec(override)
	case override == nil:
		return cloneSpec(base)
	}

	out := cloneSpec(base)

	// MinLevel: last non-nil wins.
	if override.MinLevel != nil {
		lv := *override.MinLevel
		out.MinLevel = &lv
	}

	// Fields: append (preserve earlier, add later).
	if len(override.Fields) > 0 {
		out.Fields = append(cloneFields(out.Fields), override.Fields...)
	}

	// Pipeline: full replace.
	if override.Pipeline != nil {
		out.Pipeline = override.Pipeline
	}

	// Sinks: full replace.
	if len(override.Sinks) > 0 {
		out.Sinks = append([]string(nil), override.Sinks...)
	}

	return out
}

// MergeAll merges specs in order (lowest priority first, highest last).
// Nil specs are ignored. Returns a new Specification.
func MergeAll(specs ...*Specification) *Specification {
	var out *Specification
	for _, s := range specs {
		if s == nil {
			continue
		}
		out = Merge(out, s)
	}
	if out == nil {
		out = &Specification{}
	}
	return out
}

// cloneSpec makes a deep copy of the Specification.
func cloneSpec(s *Specification) *Specification {
	if s == nil {
		return nil
	}
	cp := &Specification{}
	if s.MinLevel != nil {
		lv := *s.MinLevel
		cp.MinLevel = &lv
	}
	cp.Fields = cloneFields(s.Fields)
	if s.Pipeline != nil {
		cp.Pipeline = s.Pipeline
	}
	if len(s.Sinks) > 0 {
		cp.Sinks = append([]string(nil), s.Sinks...)
	}
	return cp
}

// cloneFields makes a shallow copy of the fields slice.
func cloneFields(in []field.Field) []field.Field {
	if len(in) == 0 {
		return nil
	}
	out := make([]field.Field, len(in))
	copy(out, in)
	return out
}
