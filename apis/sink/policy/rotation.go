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

// Rotation describes file/log rotation policy for sinks that write to files.
type Rotation struct {
	// MaxSizeMB is the maximum size of a single file before rotation.
	MaxSizeMB int

	// MaxAgeDays is the maximum age of a file before rotation.
	MaxAgeDays int

	// MaxBackups is the number of old files to keep.
	MaxBackups int

	// Compress indicates whether rotated files should be compressed.
	Compress bool
}
