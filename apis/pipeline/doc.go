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

// Package pipeline declares the high-level contracts for building and
// describing a dxlog logging pipeline.
//
// A pipeline in dxlog is a logical sequence of processing steps that a
// log record goes through before it is finally encoded and written to one
// or more sinks. This package focuses on the *declarative* side of that
// process: it provides data structures that say “which plugins to run,
// in what order, and which sinks to fan out to”, but it does not execute
// anything by itself.
//
// The typical flow described by a Specification looks like this:
//
//  1. Run pre-processing plugins (e.g. redaction, sampling, throttling,
//     correlation/context injection).
//  2. Encode the record (implementation detail, not part of this package).
//  3. Deliver the encoded record to the configured sinks (also an
//     implementation detail).
//  4. Optionally run post-processing plugins (e.g. metrics taps).
//
// This separation lets runtime packages take a Specification produced from
// configuration (file/env/remote) and assemble an executable pipeline
// using concrete plugin registries, encoders and sinks.
//
// The pipeline package intentionally does *not* import the plugin package
// to avoid cyclic dependencies. It only defines declarative specs; the
// executable plugin contract lives in the sibling package
// "dxlog/apis/pipeline/plugin".
package pipeline
