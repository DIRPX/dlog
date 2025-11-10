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

import (
	"context"
)

// Checker is the minimal contract for any health check.
//
// Implementation SHOULD be quick and non-blocking, or accept context
// for timeouts/cancellation.
type Checker interface {
	Check(ctx context.Context) (Result, error)
}

// CheckFunc is an adapter to allow the use of ordinary functions as Checker.
type CheckFunc func(ctx context.Context) (Result, error)

// Check calls f(ctx).
func (f CheckFunc) Check(ctx context.Context) (Result, error) {
	return f(ctx)
}
