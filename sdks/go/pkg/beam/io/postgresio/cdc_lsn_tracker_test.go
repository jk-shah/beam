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
	"testing"
)

func TestClaimMonotonicPositions(t *testing.T) {
	r := StartingFrom(100)
	tracker := NewLSNRangeTracker(r)

	if !tracker.TryClaim(100) {
		t.Errorf("failed to claim initial LSN 100")
	}
	if !tracker.TryClaim(105) {
		t.Errorf("failed to claim progressive LSN 105")
	}
	if !tracker.TryClaim(110) {
		t.Errorf("failed to claim progressive LSN 110")
	}

	// Regressive or duplicate claim must be rejected
	if tracker.TryClaim(108) {
		t.Errorf("expected regressive claim 108 to fail, but it succeeded")
	}
	if tracker.TryClaim(110) {
		t.Errorf("expected duplicate claim 110 to fail, but it succeeded")
	}
}

func TestUnboundedSplitCheckpoint(t *testing.T) {
	r := StartingFrom(100)
	tracker := NewLSNRangeTracker(r)

	if !tracker.TryClaim(150) {
		t.Fatalf("failed to claim 150")
	}

	primary, residual, ok := tracker.TrySplit(0.5)
	if !ok || primary == nil || residual == nil {
		t.Fatalf("expected successful split, got ok=%v", ok)
	}

	if primary.FromLSN != 100 || primary.ToLSN != 151 {
		t.Errorf("unexpected primary range: %+v", primary)
	}
	if residual.FromLSN != 151 || residual.ToLSN != UnboundedStopLSN {
		t.Errorf("unexpected residual range: %+v", residual)
	}
}
