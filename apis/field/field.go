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

// Package field defines the basic contract for structured fields in dxlog.
//
// The goal of this package is to provide a minimal, implementation-agnostic
// representation of a "field" that can be attached to a log record, passed
// through a pipeline, and finally encoded by a concrete runtime (zap, slog, ...).
//
// This package intentionally does NOT depend on any logging backend.
package field

// Field represents a single structured key/value pair.
//
// Rules:
//   - Key MUST be non-empty for a field to be meaningful.
//   - Value is intentionally typed as `any` to keep the contract open;
//     concrete runtimes may apply type-based optimizations (ints, strings, ...).
//   - Field is expected to be a small, copyable value type.
//
// Field does NOT do validation on its own to keep the contract minimal.
// Validation belongs to higher-level components (record, pipeline, encoder).
type Field struct {
	// Key is the structured name of the field (e.g. "service", "correlation_id").
	Key string

	// Value is the field payload. It can be any Go value; the concrete encoder
	// decides how to serialize it (e.g. JSON encoder may inspect the type).
	Value any
}

// New creates a new Field from key and value.
// This is a convenience constructor for call sites.
func New(key string, value any) Field {
	return Field{Key: key, Value: value}
}

// Validator is an optional interface that a Field-like type can implement
// if it wants to provide semantic validation (e.g. non-empty key).
// The core dxlog contracts do not require validation by default.
type Validator interface {
	Validate() error
}
