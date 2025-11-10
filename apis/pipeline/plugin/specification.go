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

package plugin

// Specification describes a single plugin to be placed into the pipeline.
// It is intentionally generic: runtime is responsible for looking up
// the plugin factory by Kind/Name and for decoding Config into the
// concrete plugin's settings.
type Specification struct {
	// Kind is a stable identifier of the plugin type.
	// Examples: "redact", "sampling", "throttle", "rate_limit", "inject", "dedup".
	Kind string `json:"kind" yaml:"kind"`

	// Name is an optional human-readable name/alias for diagnostics.
	// If empty, runtime may use Kind or an auto-generated name.
	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	// Enabled explicitly turns the plugin on/off at the spec level.
	// If omitted, runtime may default to true.
	Enabled *bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`

	// Config is an opaque configuration payload for this plugin.
	// The runtime knows how to interpret it based on Kind.
	// We keep it as any to avoid leaking concrete types into apis.
	Config any `json:"config,omitempty" yaml:"config,omitempty"`
}
