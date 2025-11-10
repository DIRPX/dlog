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

// Package context defines dxlog-specific context structures that describe
// the runtime/environment identity of the emitter (service, node, trace ids, ...).
//
// This package is intentionally small and does not depend on any logging
// backend or tracing implementation. Runtime code (e.g. OTel integration)
// can fill these fields from concrete sources.
package context

// Pack is a normalized set of well-known attributes that can be attached
// to a log record. These fields mirror the canonical field names from
// dxlog (service, env, correlation_id, trace_id, span_id, ...).
//
// The struct is intended to be used as a plain value type: construct, fill,
// and pass further. Callers should treat it as immutable once created.
type Pack struct {
	// CorrelationID is an application-level correlation identifier.
	// It is often propagated via HTTP/gRPC headers and is meant to bind
	// multiple services into a single business transaction.
	CorrelationID string `json:"correlation_id"`

	// TraceID is the distributed tracing identifier (W3C / OTel compatible).
	// It ties this log entry to a trace.
	TraceID string `json:"trace_id"`

	// SpanID is the distributed tracing span identifier.
	// It ties this log entry to a specific span in the trace.
	SpanID string `json:"span_id"`

	// Service is the logical name of the application/service/component
	// emitting the log (e.g. "router", "auth", "edge-gw").
	Service string `json:"service"`

	// Version is the version of the running service or binary.
	// This can be a semantic version or a commit hash.
	Version string `json:"version"`

	// Env describes the runtime environment (e.g. "prod", "staging", "dev").
	Env string `json:"env"`

	// NodeID identifies the node/host/machine on which the process is running.
	NodeID string `json:"node_id"`

	// Instance identifies the concrete instance of the service
	// (pod name, container id, replica id, ...).
	Instance string `json:"instance"`

	// Region is the geographic or logical region/zone.
	Region string `json:"region"`

	// Component is a higher-level part of the service emitting the log.
	Component string `json:"component"`

	// Subsystem is a finer-grained part inside the component.
	Subsystem string `json:"subsystem"`

	// Operation is the current operation/action name.
	Operation string `json:"operation"`
}

// Empty returns a zero-initialized Pack.
// This is a convenience to make intent explicit in the call sites.
func Empty() Pack {
	return Pack{}
}

// Merge overlays fields from b onto a and returns the result.
//
// Rule:
//   - for each string field, if b.<field> is not empty, it replaces a.<field>.
//   - otherwise the original a.<field> value is kept.
//
// This is useful when you have a "global" pack (service/env/node) and want
// to enrich it with request-specific data (correlation, trace, operation).
func Merge(a, b Pack) Pack {
	// start with a copy
	out := a

	if b.CorrelationID != "" {
		out.CorrelationID = b.CorrelationID
	}
	if b.TraceID != "" {
		out.TraceID = b.TraceID
	}
	if b.SpanID != "" {
		out.SpanID = b.SpanID
	}
	if b.Service != "" {
		out.Service = b.Service
	}
	if b.Version != "" {
		out.Version = b.Version
	}
	if b.Env != "" {
		out.Env = b.Env
	}
	if b.NodeID != "" {
		out.NodeID = b.NodeID
	}
	if b.Instance != "" {
		out.Instance = b.Instance
	}
	if b.Region != "" {
		out.Region = b.Region
	}
	if b.Component != "" {
		out.Component = b.Component
	}
	if b.Subsystem != "" {
		out.Subsystem = b.Subsystem
	}
	if b.Operation != "" {
		out.Operation = b.Operation
	}

	return out
}

// IsZero reports whether all fields of the pack are empty.
// This can be used by encoders to skip emitting an empty context section.
func (p Pack) IsZero() bool {
	return p.CorrelationID == "" &&
		p.TraceID == "" &&
		p.SpanID == "" &&
		p.Service == "" &&
		p.Version == "" &&
		p.Env == "" &&
		p.NodeID == "" &&
		p.Instance == "" &&
		p.Region == "" &&
		p.Component == "" &&
		p.Subsystem == "" &&
		p.Operation == ""
}
