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

// Package registry provides a small, concurrency-safe generic registry used
// across dlog runtime (sinks, stages, providers, encoders).
//
// Design goals:
//   - Vendor-neutral and minimal: no external deps.
//   - Deterministic: keys may be normalized (e.g., case-folding).
//   - Fast lookups: O(1) by (Kind, Name).
//   - Safe by default: duplicate registrations are prevented.
//   - Freeze option: Seal() prevents further registrations once the process
//     is fully configured.
//
// Typical usage:
//
//	var Sinks = registry.New[asink.Sink, asink.Specification](registry.WithCaseFoldLower())
//	func init() {
//	    registry.MustRegister(Sinks, registry.Key{"sink", "stdout"}, buildStdout, registry.WithDoc("writes records to stdout"))
//	}
package registry
