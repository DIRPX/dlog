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

package health

import "time"

// Result is a single checker result.
// It is the smallest unit of health reporting.
type Result struct {
	// Name is a stable, human-readable name of the check.
	// Example: "db", "redis", "object-storage".
	Name string

	// Status is a normalized health state.
	Status Status

	// Error is optional. If present, it usually explains why the status
	// is degraded or unhealthy.
	Error error

	// ObservedAt is the time when the check was executed.
	ObservedAt time.Time

	// Details is an optional, unstructured map for extra data
	// (latency, endpoint, version, etc.).
	Details map[string]any
}

// OK returns true if the result indicates a healthy state.
func (r Result) OK() bool {
	return r.Status == StatusHealthy
}
