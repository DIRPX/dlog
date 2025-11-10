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

// Package fields contains canonical field names used across dxlog.
//
// These identifiers define the common "vocabulary" for structured logs.
// Using them consistently across services makes it easier to index,
// search and analyze logs in external systems (Loki, Elasticsearch, etc.).
//
// All names are lowercase and underscore-separated to keep them simple,
// predictable and friendly to JSON-based tooling.
//
// IMPORTANT:
//   - These constants describe the *schema-level* names only.
//   - How values are encoded (string, number, object) is decided by
//     the encoder/runtime, not by this package.
package fields
