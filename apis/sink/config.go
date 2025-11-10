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

import "dirpx.dev/dxlog/apis/sink/policy"

// Config is an immutable snapshot of sink configuration.
//
// It is produced by config providers / runtime and consumed by sink
// builders to construct concrete sinks.
//
// This type intentionally stays generic: if a concrete sink needs more
// specific parameters (e.g. file path, URL), those should be carried
// in separate, sink-specific configs in the runtime layer.
type Config struct {
	// Name is the unique identifier of the sink.
	Name string

	// QueueCapacity defines how many entries the sink is willing to buffer
	// internally before applying the backpressure policy.
	QueueCapacity int

	// Backpressure defines how to behave when the queue is full.
	Backpressure policy.Backpressure

	// Retry describes how to retry failed writes.
	Retry policy.Retry

	// Batch describes batching behavior, if supported.
	Batch *policy.Batch

	// Rotation describes rotation behavior, if supported (e.g. file sink).
	Rotation *policy.Rotation

	// Labels is an optional set of key/value labels used for diagnostics
	// and metrics attribution (for example: {"kind":"stdout"}).
	Labels map[string]string
}
