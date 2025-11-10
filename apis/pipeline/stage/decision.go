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

package stage

// Decision tells the pipeline what to do with the current record.
// The pipeline owns the control flow; plugins only return one of these.
type Decision uint8

const (
	// Continue means the record should be passed to the next stage.
	Continue Decision = iota

	// Drop means the record should be discarded and the pipeline must stop
	// processing this record. This is typically used by sampling, throttling,
	// rate-limit or security plugins (e.g. redact-and-drop).
	Drop
)
