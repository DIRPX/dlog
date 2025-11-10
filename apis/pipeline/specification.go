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

package pipeline

import (
	"dirpx.dev/dxlog/apis/pipeline/plugin"
)

// Specification is a declarative description of how the logging
// pipeline should be assembled.
//
// It does NOT execute anything, it is just data. Runtime code takes
// this spec and builds the actual Pipeline (plugins, encoder, sinks).
//
// Typical order is:
//  1. pre plugins  (redact, sampling, throttle, inject)
//  2. encoder      (implementation detail, not in spec)
//  3. sinks        (one or many)
//  4. post plugins (metrics, debug taps, etc.)
type Specification struct {
	// Pre is an ordered list of plugins that run before encoding/sinking.
	// Use this for things that may DROP the record or mutate sensitive data.
	Pre []plugin.Specification `json:"pre,omitempty" yaml:"pre,omitempty"`

	// Post is an ordered list of plugins that run after the record
	// has been (logically) delivered. Often used for metrics/taps.
	Post []plugin.Specification `json:"post,omitempty" yaml:"post,omitempty"`

	// Sinks is a list of sink IDs/names that the runtime must fan-out to.
	// Actual sink configs live elsewhere (in the top-level dxlog config),
	// here we just reference them by name.
	Sinks []string `json:"sinks,omitempty" yaml:"sinks,omitempty"`
}
