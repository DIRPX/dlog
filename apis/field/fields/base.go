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

package fields

const (
	// SchemaVersion describes the version of the log schema being emitted.
	// This is useful when the log format evolves and consumers need to
	// distinguish between old and new shapes.
	SchemaVersion = "log_schema"

	// Service is the logical name of the application/service/component
	// emitting the log (for example: "router", "auth", "billing-api").
	Service = "service"

	// Version is the version of the running service or binary.
	// This can be a semantic version, a git commit, or a build number.
	Version = "version"

	// Region indicates the geographic or logical region/zone.
	// Useful for multi-region deployments and latency investigations.
	Region = "region"

	// Env describes the runtime environment in which the service operates,
	// such as "prod", "staging", "qa", "dev".
	Env = "env"

	// NodeID identifies the node/host/machine on which the process is running.
	// This helps to correlate logs with infrastructure-level events.
	NodeID = "node_id"

	// InstanceID identifies a specific instance of the service,
	// for example a pod name, container id, or replica id.
	// This is useful in horizontally scaled deployments.
	InstanceID = "instance_id"

	// Timestamp is the moment when the log entry was created.
	// Encoders are expected to format it in UTC (e.g. RFC3339, RFC3339Nano
	// or epoch milliseconds), but the exact format is a runtime decision.
	Timestamp = "ts"

	// CorrelationID is the application-level identifier that ties
	// multiple logs across services into one business transaction.
	// Unlike trace_id, this may originate from the client.
	CorrelationID = "correlation_id"

	// TraceID is the distributed tracing identifier (W3C / OpenTelemetry)
	// that links this log entry to a trace.
	TraceID = "trace_id"

	// SpanID is the distributed tracing span identifier (W3C / OpenTelemetry)
	// that links this log entry to a specific span inside the trace.
	SpanID = "span_id"

	// Level is the severity/verbosity of the log entry.
	// Typical values: "trace", "debug", "info", "warn", "error".
	Level = "level"

	// Component is a higher-level part of the service that emits the log,
	// such as "ingress", "egress", "scheduler".
	Component = "component"

	// Subsystem is a more fine-grained part inside a component,
	// such as "jwt", "tls", "routing", "storage".
	Subsystem = "subsystem"

	// Operation describes the current operation or action,
	// typically aligned with a handler or business operation name,
	// for example "route", "issue_token", "list_users".
	Operation = "op"

	// Message is the human-readable main text of the log entry.
	// It should be short and descriptive, while additional context
	// should go into structured fields.
	Message = "msg"
)
