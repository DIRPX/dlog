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

// Package level defines the logging severity type used across dxlog.
//
// The intent of this package is to provide a small, stable set of levels
// (trace, debug, info, warn, error) together with canonical string
// representations and simple parsing/validation routines.
//
// This package is deliberately kept free from concrete logging backends
// and frameworks. Runtime packages are expected to map these levels to
// specific backends (for example, to zap or slog) as needed.
//
// Canonical forms are lowercase strings ("trace", "debug", ...). Keeping
// level formatting and parsing in this package ensures that all components
// of the system interpret configuration and serialized records in the same
// way.
package level
