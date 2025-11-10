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

// Package field declares the basic structured logging building block
// used by dxlog: a key-value pair.
//
// The goal of this package is to define a minimal, backend-agnostic
// representation of a log field. Runtime packages can provide richer
// helpers or optimized encoders, but they should accept and produce
// values compatible with this contract.
//
// Canonical field names that dxlog uses for timestamps, levels, service
// identity, tracing and error projections are defined in the subpackage
// "field/fields". Keeping those names in one place helps maintain a
// consistent log schema across services, transports and sinks.
package field
