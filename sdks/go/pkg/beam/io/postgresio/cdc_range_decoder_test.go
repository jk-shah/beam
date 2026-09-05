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

func TestRangeDecoding(t *testing.T) {
	// 1. Inclusive lower, exclusive upper: [10, 50)
	r1, err := DecodeRange("[10, 50)")
	if err != nil {
		t.Fatalf("unexpected error parsing range: %v", err)
	}
	if !r1.LowerInclusive || r1.UpperInclusive || r1.Lower != 10 || r1.Upper != 50 {
		t.Errorf("unexpected range r1: %+v", r1)
	}

	// 2. Exclusive lower, inclusive upper: (100, 200]
	r2, err := DecodeRange("(100, 200]")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r2.LowerInclusive || !r2.UpperInclusive || r2.Lower != 100 || r2.Upper != 200 {
		t.Errorf("unexpected range r2: %+v", r2)
	}

	// 3. Empty range
	rEmpty, err := DecodeRange("empty")
	if err != nil || !rEmpty.IsEmpty {
		t.Errorf("expected empty range, got %+v (err: %v)", rEmpty, err)
	}

	// 4. Unbounded lower: [, 50]
	rUnboundedLower, err := DecodeRange("[, 50]")
	if err != nil || !rUnboundedLower.IsLowerUnbounded || rUnboundedLower.Upper != 50 {
		t.Errorf("expected unbounded lower range, got %+v (err: %v)", rUnboundedLower, err)
	}

	// 5. Unbounded upper: [25, )
	rUnboundedUpper, err := DecodeRange("[25, )")
	if err != nil || !rUnboundedUpper.IsUpperUnbounded || rUnboundedUpper.Lower != 25 {
		t.Errorf("expected unbounded upper range, got %+v (err: %v)", rUnboundedUpper, err)
	}
}
