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
	"context"
)

// Builder is a high-level contract for constructing a Pipeline from
// declarative specs (e.g. from dxlog config).
// Putting it in apis allows you to write tests against the builder
// without pulling real runtime.
type Builder interface {
	// Build constructs a ready-to-use pipeline from the given spec.
	Build(ctx context.Context, spec Specification) (Pipeline, error)
}
