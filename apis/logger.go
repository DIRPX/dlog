package apis

import (
	"context"

	"dirpx.dev/dxlog/apis/field"
	"dirpx.dev/dxlog/apis/level"
)

// Logger is a structured logging interface.
// Implementations must be safe for concurrent use.
// Implementations are expected to respect dynamic/atomic configuration.
type Logger interface {
	// Enabled reports whether the given level should be logged right now.
	// This allows callers to skip expensive field construction.
	Enabled(lvl level.Level) bool

	// Debug logs a debug-level message.
	Debug(ctx context.Context, msg string, fields ...field.Field)

	// Info logs an info-level message.
	Info(ctx context.Context, msg string, fields ...field.Field)

	// Warn logs a warning-level message.
	Warn(ctx context.Context, msg string, fields ...field.Field)

	// Error logs an error-level message.
	Error(ctx context.Context, msg string, fields ...field.Field)

	// Fatal logs a fatal message and may terminate the process.
	// Exact termination behavior is implementation-defined.
	Fatal(ctx context.Context, msg string, fields ...field.Field)

	// Log emits a structured log record with the given level, message and fields.
	// The ctx is used to extract correlation/trace/service data if the logger supports it.
	Log(ctx context.Context, lvl level.Level, msg string, fields ...field.Field)
}

// FieldLogger is an optional extension for loggers that support pre-bound fields.
type FieldLogger interface {
	Logger

	// WithFields returns a derived logger that always logs the given fields.
	// The returned logger must be safe for concurrent use.
	WithFields(fields ...field.Field) Logger
}

// ContextLogger is an optional extension for loggers that support pre-bound context.
type ContextLogger interface {
	Logger

	// WithContext returns a derived logger that always uses the provided context
	// as a base for extracting logging metadata.
	WithContext(ctx context.Context) Logger
}
