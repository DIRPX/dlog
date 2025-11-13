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

import (
	"context"
	"time"

	asink "dirpx.dev/dlog/apis/sink"
	spolicy "dirpx.dev/dlog/apis/sink/policy"
)

// RetryOptions configures the runtime retry behavior around a sink.
//
// The first Write attempt is always performed immediately; if it fails and
// Policy.Enable is true and Policy.MaxRetries > 0, subsequent retries are
// scheduled with exponential backoff.
type RetryOptions struct {
	// Policy is the declarative retry/backoff configuration from apis.
	Policy spolicy.Retry

	// Name overrides the sink name. If empty, the wrapper reports
	// its name as "retry(<inner.Name()>)".
	Name string
}

// retrySink wraps an asink.Sink and applies retry/backoff logic to Write.
//
// Semantics:
//   - Write:
//   - First attempt is executed immediately.
//   - On error and when enabled, up to MaxRetries additional attempts
//     are made with exponential backoff between them.
//   - If the context is cancelled before or during retries, Write returns
//     ctx.Err() and stops retrying.
//   - Flush/Close: delegated directly to the underlying sink; no retries.
type retrySink struct {
	next asink.Sink
	opt  RetryOptions
}

// Compile-time check: *retrySink implements asink.Sink.
var _ asink.Sink = (*retrySink)(nil)

// WithRetry wraps the given sink with retry/backoff behavior defined by opt.Policy.
//
// Notes:
//   - If opt.Policy.Enable is false or MaxRetries <= 0, Write still goes
//     through this wrapper but performs a single attempt without retries.
//   - Retry is applied only to Write. Flush and Close are passed through.
func WithRetry(next asink.Sink, opt RetryOptions) asink.Sink {
	opt.Policy = normalizeRetryPolicy(opt.Policy)
	return &retrySink{
		next: next,
		opt:  opt,
	}
}

// Name returns the human-friendly name of the sink.
func (s *retrySink) Name() string {
	if s.opt.Name != "" {
		return s.opt.Name
	}
	return "retry(" + s.next.Name() + ")"
}

// Write delivers a single encoded entry to the underlying sink with retry
// semantics defined by s.opt.Policy.
//
// Behavior:
//   - If ctx is already cancelled, Write returns ctx.Err() without calling inner.
//   - If Policy.Enable is false or MaxRetries <= 0, a single attempt is made.
//   - Otherwise, one initial attempt is made, followed by up to MaxRetries
//     additional attempts with backoff delays in between.
func (s *retrySink) Write(ctx context.Context, entry []byte) error {
	// Respect pre-cancelled contexts.
	if err := ctx.Err(); err != nil {
		return err
	}

	p := s.opt.Policy
	// Fast path: retries disabled or zero.
	if !p.Enable || p.MaxRetries <= 0 {
		return s.next.Write(ctx, entry)
	}

	// First attempt.
	err := s.next.Write(ctx, entry)
	if err == nil {
		return nil
	}

	// Prepare backoff parameters.
	delay := p.Initial

	for i := 0; i < p.MaxRetries; i++ {
		// Check for cancellation before waiting.
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}

		// Sleep before the next attempt, if delay is positive.
		if delay > 0 {
			time.Sleep(delay)
		}

		// Check again after sleeping, in case the context was cancelled.
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}

		// Retry attempt.
		err = s.next.Write(ctx, entry)
		if err == nil {
			return nil
		}

		// Compute delay for the next iteration.
		delay = nextDelay(delay, p.Multiplier, p.Max)
	}

	// All attempts failed; return the last error.
	return err
}

// Flush delegates to the underlying sink's Flush without retries.
func (s *retrySink) Flush(ctx context.Context) error {
	return s.next.Flush(ctx)
}

// Close delegates to the underlying sink's Close without retries.
func (s *retrySink) Close(ctx context.Context) error {
	return s.next.Close(ctx)
}

// normalizeRetryPolicy sanitizes the Retry fields to safe defaults.
//
// Rules:
//   - MaxRetries < 0 -> 0
//   - Initial < 0 -> 0
//   - Max < 0 -> 0
//   - Multiplier <= 0 -> default 2.0
func normalizeRetryPolicy(p spolicy.Retry) spolicy.Retry {
	if p.MaxRetries < 0 {
		p.MaxRetries = 0
	}
	if p.Initial < 0 {
		p.Initial = 0
	}
	if p.Max < 0 {
		p.Max = 0
	}
	if p.Multiplier <= 0 {
		p.Multiplier = 2.0
	}
	return p
}

// nextDelay computes the next backoff delay using exponential growth and max cap.
//
// Rules:
//   - If current delay <= 0, the next delay is 0 (no sleeping).
//   - Multiplier is assumed to be > 0 (enforced by normalizeRetryPolicy).
//   - If max > 0 and computed delay exceeds max, it is capped to max.
func nextDelay(current time.Duration, mult float64, max time.Duration) time.Duration {
	if current <= 0 {
		return 0
	}
	next := time.Duration(float64(current) * mult)
	if max > 0 && next > max {
		return max
	}
	return next
}
