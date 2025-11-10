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

import "context"

// Sink is a destination for encoded log entries.
//
// Notes:
//   - Sink works with already encoded []byte (e.g. JSON) to keep this package
//     independent of encoders.
//   - Sink should be safe to call from multiple goroutines unless
//     stated otherwise by the implementation.
//   - Sink should avoid panicking: it is the end of the pipeline.
type Sink interface {
	// Name returns a human-friendly identifier of the sink.
	// It is used for diagnostics, metrics and config lookups.
	Name() string

	// Write delivers a single encoded log entry to the destination.
	// Implementations may buffer, batch or enqueue the entry.
	// Returned error means the entry was not persisted/sent.
	Write(ctx context.Context, entry []byte) error

	// Flush ensures that all buffered/logically accepted entries
	// are actually written to the underlying destination.
	// Implementations that do not buffer may return nil.
	Flush(ctx context.Context) error

	// Close releases underlying resources (files, connections, buffers).
	// After Close, the sink should not be used.
	Close(ctx context.Context) error
}
