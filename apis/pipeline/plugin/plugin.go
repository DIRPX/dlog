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
	"dirpx.dev/dxlog/apis/pipeline/stage"
)

// Plugin is a unit of log processing.
//
// Plugins are typically small, focused components that perform a
// single task, such as filtering, enriching, redacting, sampling,
// throttling, rate-limiting, or deduplication.
//
// Plugins implement the pipeline.Stage interface and can be composed
// into processing pipelines.
type Plugin interface {
	stage.Stage
}

// The following interfaces describe common plugin roles.
// They do not add new methods — they are only for documentation and
// for code that wants to group plugins by purpose.

type (
	// Filter decides whether the record should continue.
	Filter interface{ Plugin }

	// Enricher adds or normalizes fields/context.
	Enricher interface{ Plugin }

	// Redactor masks or removes sensitive data (PII, secrets).
	Redactor interface{ Plugin }

	// Sampler performs probabilistic or key-based sampling.
	Sampler interface{ Plugin }

	// Throttler suppresses records over time windows.
	Throttler interface{ Plugin }

	// RateLimiter applies per-key or global rate limits to records.
	RateLimiter interface{ Plugin }

	// Deduplicator suppresses duplicate records within a time window.
	Deduplicator interface{ Plugin }
)
