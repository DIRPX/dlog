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

// Package record defines the canonical log entry shape used across dxlog.
//
// This package intentionally contains only stable, minimal data structures and
// helper methods. It performs no I/O, encoding, buffering, or registry logic.
// Implementations, encoders, and runtime behavior live outside apis/.
//
// # Record contract
//
// Record is a value type that represents a single log entry. It carries:
//   - Time:   event timestamp
//   - Level:  severity (see apis/level)
//   - Message:text message
//   - Ctx:    contextual identity (see apis/context Pack)
//   - Fields: additional structured fields (see apis/field and apis/field/fields)
//   - Err:    optional error associated with the event
//
// Producers SHOULD include the schema version in Fields using the standard key
// from apis/field/fields (e.g., fields.SchemaVersion) with value set to
// apis/version.LogSchemaVersion.
//
// # Immutability & helpers
//
// Record follows an immutable style: helper methods (e.g., WithFields, WithError)
// return a shallow copy with the requested modification, leaving the original
// instance unchanged. Callers must treat returned slices as read-only.
//
// # Separation of concerns
//
//   - Encoding (e.g., JSON for dxlog.v1) is defined by runtime encoders.
//   - Processing (filtering, enrichment, redaction) is performed by the pipeline
//     (see apis/pipeline).
//   - Delivery to outputs is handled by sinks (see apis/sink), which accept
//     already-encoded bytes.
//
// # Example
//
//	rec := NewRecord(now, lvl, "started",
//		pack, // context.Pack
//		nil,  // no fields yet
//		nil,  // no error
//	).
//		WithFields(
//			field.Field{Key: fields.Component, Value: "api"},
//			field.Field{Key: fields.Subsystem, Value: "auth"},
//		)
//
// Encoders decide how Err is rendered; duplicating Err into Fields is optional
// and encoder-specific.
package record
