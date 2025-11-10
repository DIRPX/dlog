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

package health

import (
	"context"
	"time"
)

// Aggregator executes multiple checkers and builds a Report.
// This is a convenience type; you can inline the logic if you prefer.
type Aggregator struct {
	checkers []namedChecker
}

// namedChecker pairs a name with a Checker.
type namedChecker struct {
	name    string
	checker Checker
}

// NewAggregator builds a new aggregator with no checkers.
func NewAggregator() *Aggregator {
	return &Aggregator{
		checkers: make([]namedChecker, 0),
	}
}

// Add registers a checker under a given name.
func (a *Aggregator) Add(name string, c Checker) {
	if c == nil {
		return
	}
	a.checkers = append(a.checkers, namedChecker{
		name:    name,
		checker: c,
	})
}

// Run executes all registered checkers and returns the aggregated report.
// If a checker returns (Result, error), the error is recorded into Result.Error
// and the status is downgraded to unhealthy if not set.
func (a *Aggregator) Run(ctx context.Context) Report {
	report := Report{
		Status:  StatusHealthy,
		Results: make([]Result, 0, len(a.checkers)),
	}

	for _, nc := range a.checkers {
		res, err := nc.checker.Check(ctx)
		// enforce name
		if res.Name == "" {
			res.Name = nc.name
		}
		// enforce timestamp
		if res.ObservedAt.IsZero() {
			res.ObservedAt = time.Now()
		}
		// enforce error
		if err != nil {
			res.Error = err
			// if checker forgot to set status, make it unhealthy
			if res.Status == "" || res.Status == StatusUnknown {
				res.Status = StatusUnhealthy
			}
		}
		report.Results = append(report.Results, res)

		// update overall status
		report.Status = mergeStatus(report.Status, res.Status)
	}

	return report
}

// mergeStatus collapses individual statuses into a single overall status.
// The order reflects severity: unhealthy > degraded > healthy > unknown.
func mergeStatus(current, item Status) Status {
	// if anything is unhealthy -> whole system is unhealthy
	if item == StatusUnhealthy {
		return StatusUnhealthy
	}
	// if current already unhealthy -> stay
	if current == StatusUnhealthy {
		return StatusUnhealthy
	}

	// degraded beats healthy/unknown
	if item == StatusDegraded {
		if current == StatusHealthy || current == StatusUnknown {
			return StatusDegraded
		}
	}
	if current == StatusDegraded {
		return StatusDegraded
	}

	// healthy beats unknown
	if item == StatusHealthy {
		if current == StatusUnknown {
			return StatusHealthy
		}
		return current
	}

	// item == unknown: keep current
	return current
}
