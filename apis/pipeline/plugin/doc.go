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

// Package plugin defines the executable, runtime-facing extension point
// for dxlog pipelines.
//
// While the pipeline package describes *what* should be in the pipeline
// (via pipeline.Specification and Specification), this package describes *how* a single
// processing unit should look at runtime.
//
// A plugin is simply a specialized pipeline stage that:
//
//  1. Has a stable, human-readable name (used in configs, logs, metrics).
//  2. Can be enabled or disabled without being removed from the pipeline.
//  3. Processes a dxlog record and returns a decision whether to continue
//     or to drop the record.
//
// Typical plugins include redaction, sampling, throttling, rate limiting,
// field injection, and deduplication. All of them conform to the same
// interface so that the pipeline can orchestrate them in a uniform way.
//
// This package depends only on the shared dxlog record type and on the
// minimal stage decision contract; it does not pull in any concrete
// logging backend (zap/slog), encoders, or sinks.
package plugin
