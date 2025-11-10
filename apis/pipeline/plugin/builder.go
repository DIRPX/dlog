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

import (
	"context"

	"dirpx.dev/dxlog/apis/pipeline/stage"
)

// Builder constructs a Stage from a plugin Specification.
// Implementations must be stateless and safe for concurrent use.
type Builder interface {
	// Kind returns the canonical plugin kind name (e.g., "level_filter", "redact").
	Kind() string

	// Build constructs a Stage instance for the given plugin Specification.
	Build(ctx context.Context, spec Specification) (stage.Stage, error)
}
