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
	"testing"
)

func TestPointDecoding(t *testing.T) {
	pt, err := DecodePoint("(12.34, 56.78)")
	if err != nil {
		t.Fatalf("unexpected error parsing point: %v", err)
	}
	if math.Abs(pt.X-12.34) > 0.0001 || math.Abs(pt.Y-56.78) > 0.0001 {
		t.Errorf("unexpected point coordinates: %+v", pt)
	}

	// Negative coordinates
	ptNeg, err := DecodePoint("(-73.935242, 40.730610)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if math.Abs(ptNeg.X-(-73.935242)) > 0.000001 || math.Abs(ptNeg.Y-40.730610) > 0.000001 {
		t.Errorf("unexpected negative point: %+v", ptNeg)
	}
}
