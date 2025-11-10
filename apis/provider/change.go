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

import "time"

// ChangeReason indicates why a provider emitted a change event.
type ChangeReason uint8

const (
	// ChangeInitial is the first snapshot delivered by Watch.
	ChangeInitial ChangeReason = iota + 1
	// ChangeUpdate indicates a subsequent update.
	ChangeUpdate
	// ChangeDelete indicates that the provider removed its data (Specification becomes nil).
	ChangeDelete
	// ChangeError indicates a transient error; Err is set, Specification may be nil.
	ChangeError
)

// Change describes a single provider update event.
type Change struct {
	// Source is a stable provider name (e.g., "defaults", "file:/etc/dxlog.yaml").
	Source string

	// Version is an opaque etag/revision for de-duplication.
	Version string

	// At is when the change was observed.
	At time.Time

	// Reason describes the cause of the event.
	Reason ChangeReason

	// Spec holds the current configuration; may be nil for ChangeDelete/Error.
	Spec *Specification

	// Err is set for ChangeError.
	Err error
}
