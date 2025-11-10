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

package provider

import "context"

// Provider supplies dxlog configuration with a fixed priority and optional
// change watching. Higher priority overrides lower on conflicts.
type Provider interface {
	// Name returns a stable identifier (e.g., "defaults", "file:/etc/dxlog.yaml").
	Name() string

	// Priority defines override order; higher value wins on conflicts.
	Priority() int

	// Snapshot returns the current Specification and an opaque Version (etag/revision).
	// Nil Specification means "no data". Version must change when meaningful data changes.
	Snapshot(ctx context.Context) (*Specification, string /*version*/, error)

	// Watch subscribes to changes. The first event SHOULD be ChangeInitial with
	// the current Version. Return (nil, nil) if watching is not supported; callers
	// may poll Snapshot instead.
	Watch(ctx context.Context) (Stream, error)
}
