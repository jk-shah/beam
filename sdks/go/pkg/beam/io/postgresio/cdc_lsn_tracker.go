// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresio

import (
	"math"
	"sync"
)

// UnboundedStopLSN represents an open-ended streaming LSN upper bound.
const UnboundedStopLSN uint64 = math.MaxUint64

// LSNRange defines an LSN interval [FromLSN, ToLSN).
type LSNRange struct {
	FromLSN uint64 `beam:"from_lsn" json:"from_lsn"`
	ToLSN   uint64 `beam:"to_lsn" json:"to_lsn"`
}

// StartingFrom creates an unbounded LSNRange starting at the given position.
func StartingFrom(fromLSN uint64) LSNRange {
	return LSNRange{
		FromLSN: fromLSN,
		ToLSN:   UnboundedStopLSN,
	}
}

// LSNRangeTracker tracks monotonic LSN consumption and checkpoint splits.
type LSNRangeTracker struct {
	rangeVal LSNRange
	lastClaimed uint64
	hasClaimed  bool
	mu          sync.Mutex
}

// NewLSNRangeTracker creates a tracker for the specified range.
func NewLSNRangeTracker(r LSNRange) *LSNRangeTracker {
	return &LSNRangeTracker{
		rangeVal: r,
	}
}

// TryClaim validates and claims an LSN monotonically.
func (t *LSNRangeTracker) TryClaim(lsn uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if lsn < t.rangeVal.FromLSN || lsn >= t.rangeVal.ToLSN {
		return false
	}
	if t.hasClaimed && lsn <= t.lastClaimed {
		return false
	}
	t.lastClaimed = lsn
	t.hasClaimed = true
	return true
}

// TrySplit splits the remaining unbounded range for checkpointing.
func (t *LSNRangeTracker) TrySplit(fraction float64) (primary *LSNRange, residual *LSNRange, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.hasClaimed {
		return nil, nil, false
	}

	splitPoint := t.lastClaimed + 1
	p := LSNRange{
		FromLSN: t.rangeVal.FromLSN,
		ToLSN:   splitPoint,
	}
	r := LSNRange{
		FromLSN: splitPoint,
		ToLSN:   t.rangeVal.ToLSN,
	}
	t.rangeVal = p
	return &p, &r, true
}
