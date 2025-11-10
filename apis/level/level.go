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

package level

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Level represents the logging severity used across dxlog.
//
// The order is intentional: lower values mean more verbosity.
// Implementations (runtime packages) may map these levels to concrete
// logging backends (e.g. zap, slog), but this package does not depend
// on any concrete logging library.
type Level int8

const (
	// Trace is the most verbose level.
	// Use it for development or deep diagnostics that are normally disabled.
	Trace Level = iota

	// Debug is verbose but typically enabled in non-production
	// or when diagnosing an issue.
	Debug

	// Info is the default informational level for normal operation.
	Info

	// Warn indicates unexpected situations that are not fatal
	// but may require attention.
	Warn

	// Error indicates errors after which the process can continue,
	// but the event should be surfaced to operators.
	Error

	// Fatal indicates unrecoverable errors after which the process must exit.
	Fatal
)

var (
	// ErrLevelInvalid is returned when a textual or numeric level cannot be recognized.
	ErrLevelInvalid = errors.New("dxlog: invalid level")
)

// Ensure Level can be marshaled/unmarshaled in a canonical way.
var (
	_ fmt.Stringer             = (*Level)(nil)
	_ encoding.TextMarshaler   = (*Level)(nil)
	_ encoding.TextUnmarshaler = (*Level)(nil)
)

// ParseLevel converts a textual representation into a Level.
//
// Accepted (case-insensitive):
//
//	"trace", "debug", "info", "warn", "warning", "error", "err", "fatal"
//
// "warning" is accepted as an alias for "warn" because it is common in configs.
// "err" is accepted as an alias for "error".
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return Trace, nil
	case "debug":
		return Debug, nil
	case "info":
		return Info, nil
	case "warn", "warning":
		return Warn, nil
	case "error", "err":
		return Error, nil
	case "fatal":
		return Fatal, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrLevelInvalid, s)
	}
}

// String returns the canonical lowercase name of the level.
// This representation is stable and should be used in logs and configs.
func (l Level) String() string {
	switch l {
	case Trace:
		return "trace"
	case Debug:
		return "debug"
	case Info:
		return "info"
	case Warn:
		return "warn"
	case Error:
		return "error"
	case Fatal:
		return "fatal"
	default:
		// Unknown levels should not normally appear, but we make the
		// string representation explicit to simplify diagnostics.
		return fmt.Sprintf("level(%d)", int(l))
	}
}

// Validate checks that the level is one of the known values.
func (l Level) Validate() error {
	switch l {
	case Trace, Debug, Info, Warn, Error, Fatal:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrLevelInvalid, int(l))
	}
}

// MarshalText encodes the level as its canonical lowercase name.
func (l Level) MarshalText() ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return []byte(l.String()), nil
}

// UnmarshalText decodes the level from a textual representation.
// It accepts the same values as ParseLevel.
func (l *Level) UnmarshalText(b []byte) error {
	v, err := ParseLevel(string(bytes.TrimSpace(b)))
	if err != nil {
		return err
	}
	*l = v
	return nil
}

// MarshalJSON encodes the level as a JSON string, e.g. "info".
func (l Level) MarshalJSON() ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(l.String())
}

// UnmarshalJSON decodes the level from a JSON string or number.
// Strings are preferred; numeric form is allowed for compact configs.
func (l *Level) UnmarshalJSON(b []byte) error {
	// Try string
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, perr := ParseLevel(s)
		if perr != nil {
			return perr
		}
		*l = v
		return nil
	}

	// Try numeric
	var n int8
	if err := json.Unmarshal(b, &n); err == nil {
		v := Level(n)
		if err := v.Validate(); err != nil {
			return err
		}
		*l = v
		return nil
	}

	return fmt.Errorf("%w: %s", ErrLevelInvalid, string(b))
}
