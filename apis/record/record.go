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

package record

import (
	"fmt"
	"time"

	"dirpx.dev/dxlog/apis/context"
	"dirpx.dev/dxlog/apis/field"
	"dirpx.dev/dxlog/apis/level"
)

// Record is the canonical log event shape inside dxlog.
//
// Implementations are free to treat Record as immutable and use copy-on-write
// when plugins need to modify fields.
type Record struct {
	// Time is the event time (UTC is recommended but not enforced here)
	Time time.Time
	// Level defines the severity
	Level level.Level
	// Message is the human-readable text
	Message string
	// Ctx is the well-known, pre-extracted context (trace/correlation/node/...)
	Ctx context.Pack
	// Fields is the structured payload (caller-supplied or plugin-enriched)
	Fields []field.Field
	// Err is the original error, if any (implementations may project it via ErrorAdapter)
	Err error
}

// NewRecord builds a Record with the required parts.
// This is a convenience constructor for code that wants an explicit shape.
// It does NOT perform deep copies of fields; callers should pass owned slices.
func NewRecord(
	t time.Time,
	lvl level.Level,
	msg string,
	ctx context.Pack,
	fields []field.Field,
	err error,
) Record {
	return Record{
		Time:    t,
		Level:   lvl,
		Message: msg,
		Ctx:     ctx,
		Fields:  fields,
		Err:     err,
	}
}

// Validate checks that the record has a valid level and a non-zero timestamp.
// This is a contract-level check; runtime implementations may add stricter rules
// (e.g. require UTC, require non-empty message, limit field counts).
func (r Record) Validate() error {
	if err := r.Level.Validate(); err != nil {
		return fmt.Errorf("dxlog: invalid record level: %w", err)
	}
	if r.Time.IsZero() {
		return fmt.Errorf("dxlog: record time is zero")
	}
	// Ctx and Fields are allowed to be empty.
	return nil
}

// WithFields returns a shallow copy of the record with additional fields appended.
// This is useful for plugins that want to enrich the record while keeping the
// original value semantics.
//
// NOTE: this helper lives in apis because enriching records is a very common
// operation for all implementations; keeping it here ensures consistent behavior.
func (r Record) WithFields(extra ...field.Field) Record {
	if len(extra) == 0 {
		return r
	}
	// shallow copy
	out := r
	out.Fields = append(append([]field.Field(nil), r.Fields...), extra...)
	return out
}

// WithError returns a shallow copy of the record with a new error attached.
func (r Record) WithError(err error) Record {
	out := r
	out.Err = err
	return out
}
