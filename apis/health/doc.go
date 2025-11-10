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

// Package health defines minimal contracts for reporting service/component
// health in DIRPX.
//
// The key ideas:
//   - health is represented as a small, strongly-typed status;
//   - every check returns a Result with status, name and optional error;
//   - multiple checkers can be aggregated into a single Report;
//   - package is intentionally small to be used by HTTP/gRPC adapters.
//
// This package does NOT perform I/O. It only defines types and helpers.
// Concrete transports (HTTP endpoints, gRPC services, CLI) should live
// in their own packages and depend on this package.
package health
