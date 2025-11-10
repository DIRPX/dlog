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

// Package provider defines configuration providers for dxlog.
//
// A provider loads a partial logging Specification, declares a Priority, and can
// optionally watch for changes. Multiple providers can be combined;
// higher-priority providers override lower ones during merge.
//
// This package only defines contracts and small merge utilities.
// Concrete implementations (file/env/remote/runtime overrides) live in runtime.
//
// Priority convention (recommendation):
//
//	0  - defaults/builtin
//	10 - file (yaml/json)
//	20 - env
//	30 - remote/control-plane
//	40 - runtime/CLI overrides
//
// Watch semantics:
//   - If supported, Watch MUST emit an initial Change (ChangeInitial) with the
//     current Version and Specification, followed by updates (ChangeUpdate).
//   - If watching is not supported, providers may return (nil, nil) and callers
//     should poll Snapshot.
//
// Merge semantics (see merge.go):
//   - MinLevel: last non-nil wins.
//   - Fields: appended (earlier + later).
//   - Pipeline: replaced as a whole (it has its own schema/versioning).
//   - Sinks: replaced as a whole (binding happens in runtime against registry).
package provider
