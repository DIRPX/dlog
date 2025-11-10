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

// Package context provides contracts for extracting well-known logging
// context from a Go context.Context and for holding that extracted data
// in a canonical shape.
//
// dxlog treats things like correlation IDs, trace/span IDs, service and
// node identity as first-class log fields. To avoid scattering extraction
// logic across the codebase, this package centralizes:
//
//  1. Pack — a simple value type that holds all well-known
//     context attributes the logger may want to inject into a record;
//  2. Extractor   — an interface for pulling those attributes out of
//     context.Context (for example, from OpenTelemetry, HTTP headers,
//     or your own middleware).
//
// Implementations of Extractor live in runtime or integration packages
// (e.g. an OTel-aware extractor). This package only defines the shape.
package context
