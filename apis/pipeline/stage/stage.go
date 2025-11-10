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

package stage

import (
	"context"

	"dirpx.dev/dxlog/apis/record"
)

// Stage is the minimal processing unit in the pipeline.
// A stage receives a record and returns (possibly) a modified record
// plus a decision. Errors are allowed and should be handled by the pipeline
// implementation (e.g. log, count, continue, or stop — up to runtime).
//
// This interface is intentionally small so that plugins and other
// processing components can all implement the same shape.
type Stage interface {
	// Process processes a record and returns the (possibly) modified record,
	// a decision, and an error (if any).
	//
	// The context can be used to carry deadlines, cancellation signals,
	// and other request-scoped values across API boundaries and between
	// processes.
	//
	// If an error is returned, the pipeline implementation should decide
	// how to handle it (e.g. log, count, continue, or stop — up to runtime).
	//
	// The returned record can be the same as the input record (i.e. no modification).
	//
	// The decision tells the pipeline what to do next (continue, drop, etc.).
	Process(ctx context.Context, r record.Record) (record.Record, Decision, error)

	// Name returns the name of the stage.
	Name() string

	// Enabled returns whether the stage is enabled.
	Enabled() bool
}
