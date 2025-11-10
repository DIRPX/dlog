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

// Package sink defines the contracts for log destinations (sinks) in dxlog.
//
// A sink is a final consumer of encoded log entries: stdout, file, network,
// OTLP exporter, local agent, etc. This package only describes the shape;
// concrete implementations live in runtime packages.
package sink
