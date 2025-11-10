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

// Retry describes a simple retry/backoff policy for unreliable outputs.
type Retry struct {
	// Enable turns retries on or off.
	Enable bool

	// MaxRetries is the maximum number of attempts for one entry.
	MaxRetries int

	// Initial is the initial delay before the first retry.
	Initial time.Duration

	// Max is the upper bound for the backoff delay.
	Max time.Duration

	// Multiplier controls exponential backoff, e.g. 2.0 doubles each attempt.
	Multiplier float64
}
