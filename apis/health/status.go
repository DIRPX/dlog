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

// Status is a normalized health state for a component or a service.
//
// Keep the set small to make HTTP/gRPC mapping trivial.
type Status string

const (
	// StatusUnknown means the checker could not determine the health state.
	StatusUnknown Status = "unknown"

	// StatusHealthy means the component works as expected.
	StatusHealthy Status = "healthy"

	// StatusDegraded means the component works, but with limited capacity
	// or elevated error rates. The system may still operate.
	StatusDegraded Status = "degraded"

	// StatusUnhealthy means the component is not operational and the system
	// should be considered unavailable.
	StatusUnhealthy Status = "unhealthy"
)
