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

// Package stage defines the minimal executable unit of the dxlog pipeline.
//
// In dxlog the pipeline is a sequence of stages. Each stage receives a
// fully-formed log record, may inspect or modify it, and returns a
// decision that tells the pipeline whether to continue processing or to
// drop the record entirely.
//
// This package is intentionally small and does not depend on any concrete
// logging backend, encoders, sinks, or plugins. Higher-level concepts
// (such as "plugin", "redactor", "sampler") are built on top of this
// contract in sibling packages.
//
// The key ideas are:
//
//  1. A stage must be addressable and diagnosable — that's why every
//     stage exposes Name().
//  2. A stage may be turned on or off without removing it from the
//     pipeline — that's why every stage exposes Enabled().
//  3. A stage must clearly signal what to do with the record — that's
//     why Process(...) returns a Decision (Continue or Drop).
//
// Implementations are expected to be safe for concurrent use if the
// runtime executes the pipeline from multiple goroutines.
package stage
