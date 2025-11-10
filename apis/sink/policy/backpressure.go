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

package policy

// Backpressure defines what the sink should do when its internal
// queue/buffer is full.
type Backpressure uint8

const (
	// BackpressureBlock means the caller should be blocked until
	// there is free space in the sink buffer.
	BackpressureBlock Backpressure = iota

	// BackpressureDrop means the entry should be dropped immediately.
	// Implementations should record this in metrics.
	BackpressureDrop

	// BackpressureShed means the entry should be dropped and the sink
	// may try to shed additional load (for example, drop more aggressively).
	BackpressureShed
)
