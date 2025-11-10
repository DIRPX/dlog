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

package pipeline

import (
	"context"

	"dirpx.dev/dxlog/apis/record"
)

// Pipeline is an ordered chain of stages, typically:
//
//	pre-plugins -> encoder -> sinks -> post-plugins
//
// This contract does not dictate what those stages are, only that
// they are executed in order.
type Pipeline interface {
	// Emit pushes a single record through all stages.
	// Implementations should:
	//   1. stop on Decision=Drop;
	//   2. collect/emit errors in a consistent way;
	//   3. be safe for concurrent use (but that is runtime’s job).
	Emit(ctx context.Context, r record.Record) error

	// Flush forces all flushable stages (typically sinks) to write
	// their buffered data. Not all stages have to support flushing.
	Flush(ctx context.Context) error
}
