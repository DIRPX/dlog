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

// Stream is a read-only stream of provider changes.
// Implementations must be safe for concurrent use.
type Stream interface {
	// Updates returns a channel of Change events. The channel is closed when the
	// stream ends or Close is called.
	Updates() <-chan Change

	// Close stops the stream and releases resources.
	Close() error
}
