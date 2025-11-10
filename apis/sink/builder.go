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

package sink

import "context"

// Builder constructs a Sink instance from a stable Config.
// This interface is a contract only. Implementations and registries live in runtime.
type Builder interface {
	// Kind returns the canonical sink kind identifier (e.g., "stdout", "file", "otlp").
	Kind() string

	// Build constructs a Sink for the given logical name and configuration.
	// The returned Sink.Name() MUST equal name.
	// Implementations should treat cfg as immutable input.
	Build(ctx context.Context, name string, cfg *Config) (Sink, error)
}
