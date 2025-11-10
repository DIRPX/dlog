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

package sink

// Group represents a fan-out sink that forwards entries to multiple sinks.
//
// This is useful when the same log stream should be written to stdout,
// a file, and a remote collector at the same time.
//
// This is an optional extension over Sink. Implementations may choose
// to embed a Group-like sink inside the runtime and not expose it directly.
type Group interface {
	Sink

	// Add registers a new sink in the group.
	// If a sink with the same name already exists, the behavior is implementation-defined
	// (typically: return an error).
	Add(s Sink) error

	// Remove unregisters a sink by its name.
	// If the sink is not found, implementations may return an error or ignore silently.
	Remove(name string) error

	// List returns the names of all sinks currently registered in the group.
	List() []string
}
