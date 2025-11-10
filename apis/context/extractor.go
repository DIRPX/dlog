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

package context

import "context"

// Extractor defines how to build a dxlog context Pack from a context.Context.
//
// Different implementations may pull data from different sources:
//   - HTTP / gRPC headers (correlation_id)
//   - OpenTelemetry span from context (trace_id, span_id)
//   - process-level/service-level defaults (service, env, node_id)
//   - custom application keys
//
// This interface is kept intentionally small so that it can be composed.
type Extractor interface {
	// Extract returns a Pack describing the current logging context.
	// Implementations should never return nil; use an empty Pack instead.
	Extract(ctx context.Context) Pack
}

// ExtractorFunc is an adapter to allow the use of ordinary functions
// as Extractor implementations.
type ExtractorFunc func(ctx context.Context) Pack

// Extract calls f(ctx).
func (f ExtractorFunc) Extract(ctx context.Context) Pack {
	return f(ctx)
}

// Chain returns an Extractor that merges results from all provided extractors
// using the same merge rule as in Merge: later extractors override
// fields of earlier ones when they return non-empty values.
//
// The typical usage is:
//
//	base := NewStaticExtractor(globalPack)
//	otel := NewOTelExtractor()
//	corr := NewHeaderExtractor()
//	e := Chain(base, otel, corr)
//
// Then e.Extract(ctx) will produce a Pack that contains global service data,
// then OTEL trace/span, then per-request correlation/operation.
func Chain(extractors ...Extractor) Extractor {
	return ExtractorFunc(func(ctx context.Context) Pack {
		var out Pack
		for i, ex := range extractors {
			if ex == nil {
				continue
			}
			p := ex.Extract(ctx)
			if i == 0 {
				// first non-nil extractor initializes the output
				out = p
				continue
			}
			out = Merge(out, p)
		}
		return out
	})
}

// Static returns an Extractor that always yields the provided pack.
// Useful for global/service-level attributes.
func Static(p Pack) Extractor {
	return ExtractorFunc(func(ctx context.Context) Pack {
		return p
	})
}
