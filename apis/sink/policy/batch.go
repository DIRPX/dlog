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

import "time"

// Batch describes batching behavior. Sinks that do not support batching
// may ignore this configuration.
type Batch struct {
	// MaxEntries is the maximal number of entries to accumulate
	// before a flush is triggered.
	MaxEntries int

	// Interval defines how often the batch should be flushed even if
	// MaxEntries was not reached. Zero means "no time-based flush".
	Interval time.Duration
}
